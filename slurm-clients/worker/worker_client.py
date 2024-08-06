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
    await update_slurm_config(data["slurm_conf"])
    status = await restart_slurmd()

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
        if "active (running)" in result.stdout.decode():
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
