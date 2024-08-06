import os
import asyncio
import logging
import websockets
import subprocess
import json

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "ws://config-server-0.config-server-svc.test-slurm.svc.cluster.local:3000/ws/notify")

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/worker_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def register_worker():
    async with websockets.connect(CONFIG_SERVER_URL) as websocket:
        node_details = get_worker_details()
        data = {
            "command": "register_worker",
            "data": node_details
        }
        logger.info(f"Registering worker: {data}")
        await websocket.send(json.dumps(data))
        await receive_configs(websocket)

def get_worker_details():
    node_name = os.popen("hostname").read().strip()
    ip_address = os.popen('hostname -i').read().strip()
    sockets = int(os.popen("lscpu | grep 'Socket(s):' | awk '{print $2}'").read().strip())
    cores_per_socket = int(os.popen("lscpu | grep 'Core(s) per socket:' | awk '{print $4}'").read().strip())
    threads_per_core = int(os.popen("lscpu | grep 'Thread(s) per core:' | awk '{print $4}'").read().strip())
    real_memory = int(os.popen("free -m | awk '/^Mem:/{print $2}'").read().strip())
    gpus = 0
    if os.getenv("USE_GPU") == "true" and os.system("command -v nvidia-smi") == 0:
        gpus = int(os.popen("nvidia-smi --query-gpu=index --format=csv,noheader | wc -l").read().strip())
    return {
        "node_name": node_name,
        "ip_address": ip_address,
        "sockets": sockets,
        "cores_per_socket": cores_per_socket,
        "threads_per_core": threads_per_core,
        "real_memory": real_memory,
        "gpus": gpus
    }

async def receive_configs(websocket):
    while True:
        try:
            message = await websocket.recv()
            message_data = json.loads(message)
            command = message_data.get("command")
            data = message_data.get("data")
            logger.info(f"Received command: {command}")
            logger.info(f"Received data: {data}")

            if command == "start_node":
                ## responsible for the following:
                ## 1. update munge.key
                ## 2. update slurm.conf
                ## 3. start munge
                ## 4. if munge running, then start slurmctld
                ## 5. if munge not running, then send status_code to config-server
                await handle_start_node(data, websocket)
            elif command == "update_node":
                ## responsible for the following:
                ## 1. update slurm.conf
                ## 2. restart slurmctld
                await update_node(data, websocket)
            elif command == "stop_node":
                pass
        except websockets.ConnectionClosed:
            logger.error("Connection closed, reconnecting...")
            await asyncio.sleep(5)
            await register_worker()
            
async def handle_start_node(data, websocket):
    logger.debug(f"handle_start_node: {data}")
    await update_slurm_config(data["slurm_conf"])

    slurmctld_running = await start_slurmd()
    if not slurmctld_running:
        await websocket.send(json.dumps({"command": "node_status", "data": {"code": "slurmctld_not_running", "message": "Cannot start Slurmctld, unknown error"}}))    

async def update_slurm_config(data):
    logger.info("Updating Slurm configuration")
    slurm_conf_path = "/etc/slurm-llnl/slurm.conf"
    with open(slurm_conf_path, "w") as f:
        f.write(data)
    logger.info(f"Updated slurm.conf with: {data}")
    
    
async def start_slurmd():
    await start_service("slurmd")
    return status_service("slurmd")

async def restart_slurmd():
    await restart_service("slurmd")
    return status_service("slurmd")

async def update_node(data, websocket):
    node_name = os.popen("hostname").read().strip()
    await update_slurm_config(data["slurm_conf"])
    await verify_and_restart_node(node_name, websocket)

async def verify_and_restart_node(node_name, websocket):
    undesirable_states = [
        "DOWN", "FAIL", "NO_RESPOND", "POWER_DOWN", "UNKNOWN"
    ]
    retries = 5
    for i in range(retries):
        result = subprocess.run(["scontrol", "show", "node", node_name], capture_output=True)
        output = result.stdout.decode()
        errors = result.stderr.decode()

        logger.debug(f"Retry number: {i}\n\n output:\n {output} \n\n errors:\n {errors}")

        if "slurm_load_node error" in errors:
            logger.error(f"scontrol show node error: {errors}")
            handle_scontrol_errors(errors)
            await asyncio.sleep(5)  # Wait before retrying
            continue

        state = None
        for line in output.split('\n'):
            if 'State=' in line:
                state = line.split('State=')[1].split()[0]
                break

        if state == "IDLE":
            logger.info(f"Node {node_name} is idle")
            await websocket.send(json.dumps({"command": "node_status", "data": {"code": "idle", "message": f"Node {node_name} is idle"}}))
            return  # Exit the function immediately
        elif state and state in undesirable_states:
            logger.warning(f"Node {node_name} in undesirable state {state}, restarting slurmd")
            status = await restart_slurmd()
            if status:
                await asyncio.sleep(5)  # Wait for the service to restart
                # Check the node status again after the restart
                result = subprocess.run(["scontrol", "show", "node", node_name], capture_output=True)
                output = result.stdout.decode()
                for line in output.split('\n'):
                    if 'State=' in line:
                        state = line.split('State=')[1].split()[0]
                        break
                if state == "IDLE":
                    logger.info(f"Node {node_name} is idle after restart")
                    await websocket.send(json.dumps({"command": "node_status", "data": {"code": "idle", "message": f"Node {node_name} is idle"}}))
                    return  # Exit the function immediately
            else:
                logger.error(f"Failed to restart slurmd for node {node_name}")
        else:
            logger.info(f"Node {node_name} is in state {state}, no restart needed")
            await websocket.send(json.dumps({"command": "node_status", "data": {"code": state.lower(), "message": f"Node {node_name} is in state {state}"}}))
            return  # Exit the function immediately
    else:
        logger.error(f"Node {node_name} status is still unknown after retries")
        await websocket.send(json.dumps({"command": "node_status", "data": {"code": "unknown", "message": f"Node {node_name} status is unknown after retries"}}))


def handle_scontrol_errors(errors):
    if "munge" in errors:
        if not status_service('munge'):
            logger.error("Munge error, Munge is not running. ensure munge is running and then restart slurm.")
    if "Invalid authentication credential" in errors:
        logger.error("Authentication error, verify Munge credentials and configuration")
    # Add any other specific error handling as needed

async def start_service(service_name, *args):
    log_dir = "/var/log/slurm-llnl"
    stdout_log = os.path.join(log_dir, f"{service_name}_stdout.log")
    stderr_log = os.path.join(log_dir, f"{service_name}_stderr.log")
    logger.info(f"Starting {service_name} service...")
    try:
        with open(stdout_log, 'wb') as out, open(stderr_log, 'wb') as err:
            process = await asyncio.create_subprocess_exec(
                "service", service_name, "start", *args,
                stdout=out,
                stderr=err
            )
            await process.communicate()
        logger.info(f"{service_name} service started, logs: {stdout_log}, {stderr_log}")
    except Exception as e:
        logger.error(f"Failed to start {service_name}: {e}")

async def restart_service(service_name):
    log_dir = "/var/log/slurm-llnl"
    stdout_log = os.path.join(log_dir, f"{service_name}_stdout.log")
    stderr_log = os.path.join(log_dir, f"{service_name}_stderr.log")
    logger.info(f"Restarting {service_name} service...")
    try:
        with open(stdout_log, 'wb') as out, open(stderr_log, 'wb') as err:
            process = await asyncio.create_subprocess_exec(
                "service", service_name, "restart",
                stdout=out,
                stderr=err
            )
            await process.communicate()
        logger.info(f"{service_name} service restarted, logs: {stdout_log}, {stderr_log}")
    except Exception as e:
        logger.error(f"Failed to restart {service_name}: {e}")
        
def status_service(service_name):
    try:
        result = subprocess.run(["service", service_name, "status"], capture_output=True)
        if "is running" in result.stdout.decode():
            logger.info(f"{service_name} is running")
            return True
        else:
            logger.error(f"{service_name} is not running")
            return False
    except Exception as e:
        logger.error(f"Failed to check status of {service_name}: {e}")
        return False
    
if __name__ == "__main__":
    logger.info("Starting worker client...")
    asyncio.run(register_worker())
    logger.info("Worker client finished.")
