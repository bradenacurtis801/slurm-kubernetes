import os
import asyncio
import logging
import websockets
import subprocess
import json

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "ws://config-server-0.config-server-svc.test-slurm.svc.cluster.local:3000/ws/notify")
PHYSICAL_NODE_HOSTNAME = os.getenv("NODE_NAME", None)
PHYSICAL_NODE_IP = os.getenv("NODE_IP", None)
SLURM_CONF_DIR = os.getenv("SLURM_CONF_DIR", "/etc/slurm-llnl")
LOG_DIR = os.getenv("LOG_DIR", "/var/log/slurm-llnl")
USE_GPU = os.getenv("USE_GPU", 'false')

POD_NODE_NAME = os.popen("hostname").read().strip()
POD_IP_ADDRESS = os.popen('hostname -i').read().strip()
POD_SOCKETS = int(os.popen("lscpu | grep 'Socket(s):' | awk '{print $2}'").read().strip())
POD_CORES_PER_SOCKET = int(os.popen("lscpu | grep 'Core(s) per socket:' | awk '{print $4}'").read().strip())
POD_THREADS_PER_CORE = int(os.popen("lscpu | grep 'Thread(s) per core:' | awk '{print $4}'").read().strip())
POD_REAL_MEMORY = int(os.popen("free -m | awk '/^Mem:/{print $2}'").read().strip())
POD_GPUS = 0 # IF USE_GPU ENV VAR IS TRUE, THEN THIS VALUE WILL BE OVERWRITTEN 

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

# Global variable to store the running verify_and_restart_node task
running_task = None

async def register_worker():
    async with websockets.connect(CONFIG_SERVER_URL) as websocket:
        node_details = get_worker_details()
        data = {
            "command": "register_worker",
            "data": {
                **node_details,  # Unpack node_details dictionary
                "machine_hostname": PHYSICAL_NODE_HOSTNAME,
                "machine_ip": PHYSICAL_NODE_IP
                }
        }
        logger.info(f"Registering worker: {data}")
        await websocket.send(json.dumps(data))
        await receive_configs(websocket)

def get_worker_details():
    if USE_GPU == "true" and os.system("command -v nvidia-smi") == 0:
        logger.info("USE_GPU set to true, configuring GRES info...")
        GPUS = int(os.popen("nvidia-smi --query-gpu=index --format=csv,noheader | wc -l").read().strip())
        generate_gres_conf()
    return {
        "pod_hostname": POD_NODE_NAME,
        "pod_ip_address": POD_IP_ADDRESS,
        "pod_sockets": POD_SOCKETS,
        "pod_cores_per_socket": POD_CORES_PER_SOCKET,
        "pod_threads_per_core": POD_THREADS_PER_CORE,
        "pod_real_memory": POD_REAL_MEMORY,
        "pod_gpus": POD_GPUS
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

            if command == "update_node":
                await update_node(data, websocket)
            elif command == "stop_node":
                pass
            else:
                logger.warning(f"Unexpected command: {message}")

        except websockets.ConnectionClosed:
            logger.error("Connection closed, reconnecting...")
            await asyncio.sleep(5)
            await register_worker()
            
async def update_slurm_config(data):
    logger.info("Updating Slurm configuration")
    slurm_conf_path = f"{SLURM_CONF_DIR}/slurm.conf"
    with open(slurm_conf_path, "w") as f:
        f.write(data)
    logger.info(f"Updated slurm.conf with: {data}")

def generate_gres_conf():
    gres_conf_path = os.path.join(SLURM_CONF_DIR, "gres.conf")
    
    # Initialize gres file
    with open(gres_conf_path, 'w') as gres_file:
        pass

    logger.info(f"Generating {gres_conf_path}")

    gpu_index = 0  # Start with the first GPU index
    gpu_devices = sorted([dev for dev in os.listdir('/dev') if dev.startswith('nvidia') and dev[6:].isdigit()])
    
    with open(gres_conf_path, 'a') as gres_file:
        for gpu_device in gpu_devices:
            gpu_name = subprocess.getoutput(f"nvidia-smi --query-gpu=name --format=csv,noheader --id={gpu_index}")
            gpu_name = gpu_name.strip().replace(" ", "_")
            if gpu_name != "No_devices_were_found":
                gres_file.write(f"Name=gpu Type={gpu_name} File=/dev/{gpu_device}\n")
            gpu_index += 1  # Increment the GPU index for the next device

    logger.info(f"{gres_conf_path} generated:")
    with open(gres_conf_path, 'r') as gres_file:
        logger.info(gres_file.read())

async def start_slurmd():
    await start_service("slurmd")
    return status_service("slurmd")

async def restart_slurmd():
    await restart_service("slurmd")
    return status_service("slurmd")

async def update_node(data, websocket):
    global running_task
    log_current_tasks()

    logger.info(f"Updating Slurm configuration for node: {POD_NODE_NAME}")

    await update_slurm_config(data["slurm_conf"])
    await websocket.send(json.dumps({"command": "node_status", "data": {"code": "updating", "message": f"Node {POD_NODE_NAME} is updating"}}))
    logger.info(f"Slurm configuration updated with data: {data['slurm_conf']}")

    if running_task is not None:
        logger.info("Cancelling the previous running task.")
        running_task.cancel()
        try:
            await running_task
        except asyncio.CancelledError:
            logger.info("Previous verify_and_restart_node task cancelled.")

    logger.info("Starting a new verify_and_restart_node task.")
    running_task = asyncio.create_task(verify_and_restart_node(websocket))

async def verify_and_restart_node(websocket):

    undesirable_states = [
        "DOWN", "FAIL", "NO_RESPOND", "POWER_DOWN", "UNKNOWN"
    ]

    retries = 5

    for i in range(retries):
        result = subprocess.run(["scontrol", "show", "node", POD_NODE_NAME], capture_output=True)
        output = result.stdout.decode()
        errors = result.stderr.decode()

        logger.debug(f"Retry number: {i}\n\n output:\n {output} \n\n errors:\n {errors}")

        if "slurm_load_node error" in errors:
            logger.error(f"scontrol show node error: {errors}")
            handle_scontrol_errors(errors)
            await asyncio.sleep(5)  # Wait before retrying
            continue

        if f"Node {POD_NODE_NAME} not found" in output:
            logger.error(f"Node {POD_NODE_NAME} not found, registering worker")
            await register_worker()
            return

        state = None
        for line in output.split('\n'):
            if 'State=' in line:
                state = line.split('State=')[1].split()[0].strip('*')
                break

        if state == "IDLE":
            logger.info(f"Node {POD_NODE_NAME} is idle")
            await websocket.send(json.dumps({"command": "node_status", "data": {"code": "idle", "message": f"{POD_NODE_NAME}:> slurm worker is idle"}}))
            return  # Exit the function immediately
        elif state and state in undesirable_states:
            logger.warning(f"Node {POD_NODE_NAME} in undesirable state {state}, restarting slurmd")
            status = await restart_slurmd()
            if status:
                await asyncio.sleep(5)  # Wait for the service to restart
                # Check the node status again after the restart
                result = subprocess.run(["scontrol", "show", "node", POD_NODE_NAME], capture_output=True)
                output = result.stdout.decode()
                for line in output.split('\n'):
                    if 'State=' in line:
                        state = line.split('State=')[1].split()[0].strip('*')
                        break
                if state == "IDLE":
                    logger.info(f"Node {POD_NODE_NAME} is idle after restart")
                    await websocket.send(json.dumps({"command": "node_status", "data": {"code": "idle", "message": f"{POD_NODE_NAME}:> slurm worker is idle"}}))
                    return  # Exit the function immediately
            else:
                logger.error(f"Failed to restart slurmd for node {POD_NODE_NAME}")
        else:
            logger.info(f"Node {POD_NODE_NAME} is in state {state}, no restart needed")
            await websocket.send(json.dumps({"command": "node_status", "data": {"code": state.lower(), "message": f"{POD_NODE_NAME}:> slurm worker is in state {state}"}}))
            return  # Exit the function immediately

    else:
        logger.error(f"Node {POD_NODE_NAME} status is still unknown after retries")
        await websocket.send(json.dumps({"command": "node_status", "data": {"code": "unknown", "message": f"{POD_NODE_NAME}:> slurm worker status is unknown after retries"}}))

def handle_scontrol_errors(errors):
    if "munge" in errors:
        if not status_service('munge'):
            logger.error("Munge error, Munge is not running. ensure munge is running and then restart slurm.")
    if "Invalid authentication credential" in errors:
        logger.error("Authentication error, verify Munge credentials and configuration")
    # Add any other specific error handling as needed

async def start_service(service_name, *args):
    stdout_log = os.path.join(LOG_DIR, f"{service_name}_stdout.log")
    stderr_log = os.path.join(LOG_DIR, f"{service_name}_stderr.log")
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
    stdout_log = os.path.join(LOG_DIR, f"{service_name}_stdout.log")
    stderr_log = os.path.join(LOG_DIR, f"{service_name}_stderr.log")
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

def log_current_tasks():
    tasks = asyncio.all_tasks()
    logger.info(f"Currently running tasks ({len(tasks)}):")
    for task in tasks:
        logger.info(f"Task: {task.get_name()}, Status: {task._state}, Coroutine: {task.get_coro()}")
    
if __name__ == "__main__":
    logger.info("Starting worker client...")
    asyncio.run(register_worker())
    logger.info("Worker client finished.")
