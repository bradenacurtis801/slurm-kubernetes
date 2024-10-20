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

POD_NODE_NAME = os.popen("hostname").read().strip()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/master_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global variable to store the running verify_and_restart_node task
running_task = None

async def register_master():
    async with websockets.connect(CONFIG_SERVER_URL) as websocket:
        data = {
            "command": "register_master",
            "data": {
                "pod_hostname": POD_NODE_NAME,
                "machine_hostname": PHYSICAL_NODE_HOSTNAME,
                "machine_ip": PHYSICAL_NODE_IP
                }
        }
        logger.info(f"Registering master: {data}")
        await websocket.send(json.dumps(data))
        await receive_configs(websocket)

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
            await register_master()
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            continue

def log_current_tasks():
    tasks = asyncio.all_tasks()
    logger.info(f"Currently running tasks ({len(tasks)}):")
    for task in tasks:
        logger.info(f"Task: {task.get_name()}, Status: {task._state}, Coroutine: {task.get_coro()}")

async def update_node(data, websocket):
    global running_task
    log_current_tasks()

    logger.info(f"Updating Slurm configuration for node: {POD_NODE_NAME}")

    await update_slurm_config(data["slurm_conf"])
    await restart_service("slurmctld")
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
        
async def update_slurm_config(data):
    logger.info("Updating Slurm configuration")
    slurm_conf_path = f"{SLURM_CONF_DIR}/slurm.conf"
    with open(slurm_conf_path, "w") as f:
        f.write(data)
    logger.info(f"Updated slurm.conf with: {data}")

async def restart_slurmctld():
    await restart_service("slurmctld")
    return status_service("slurmctld")

async def verify_and_restart_node(websocket):
    retries = 5

    for i in range(retries):
        logger.debug(f"Retry number: {i}\n\n")
        if not status_service("slurmctld"):
            logger.warning("Slurm master is still updating...")
            await websocket.send(json.dumps({"command": "node_status", "data": {"code": "updating", "message": f"{POD_NODE_NAME}:> slurm master is updating"}}))
            await asyncio.sleep(5)  # Wait before retrying
            continue
        else:
            logger.info("Slurm master is now running")
            await websocket.send(json.dumps({"command": "node_status", "data": {"code": "running", "message": f"{POD_NODE_NAME}:> slurm master is running"}}))
            return
    # If forloop doesnt break, then slurm master is not running and there is an error.
    await websocket.send(json.dumps({"command": "node_status", "data": {"code": "not_running", "message": f"{POD_NODE_NAME}:> slurm master failed to update!"}}))

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
    
if __name__ == "__main__":
    logger.info("Starting master client...")
    asyncio.run(register_master())
    logger.info("Master client finished.")
