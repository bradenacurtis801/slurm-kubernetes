# sockets/config_server.py

import os
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect
from models.client_socket import ClientSockets
from config.slurm_config import SlurmConfig

logger = logging.getLogger(__name__)

class ConfigServer:
    def __init__(self, config_dir="./data"):
        self.config_dir = config_dir
        self.ensure_config_dir()
        self.client_sockets = ClientSockets()
        self.slurm_config = SlurmConfig()

    def ensure_config_dir(self):
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)
            logging.info(f"Created config directory: {self.config_dir}")

    async def on_connect(self, websocket: WebSocket):
        await websocket.accept()
        self.client_sockets.add_client(websocket)
        logging.info(f"Client connected: {websocket.client.host}")

    async def on_disconnect(self, websocket: WebSocket):
        await websocket.close()  # Ensure the WebSocket connection is fully closed
        logging.info(f"Client disconnected: {websocket.client.host}")
        client_socket = self.client_sockets.get_client(websocket)
        pod_name = client_socket.get_pod_name()
        if client_socket and pod_name:
            logger.info(f"Removing node from slurm config: {pod_name}")
            self.slurm_config.remove_node(pod_name)
            await self.broadcast_update_nodes()
        logger.info(f"Removing client socket: {websocket}")
        self.client_sockets.remove_client(websocket)
        logging.info(f"Client list: {self.client_sockets.get_clients()}")

    async def handle_message(self, websocket: WebSocket, message: str):
        try:
            message_data = json.loads(message)
            command = message_data.get("command")
            data = message_data.get("data")
            logger.info(f"Received command: {command}")
            if command == "register_master":
                await self.register_master(websocket, data)
            elif command == "register_worker":
                await self.register_worker(websocket, data)
            elif command == "node_status":
                await self.handle_client_status_codes(websocket, data)

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message")

    async def handle_client_status_codes(self, websocket: WebSocket, code: dict):
        self.client_sockets.update_client_meta(websocket, "status", code)
        logger.info(f"Client status code: {code}")
        if code == "slurm_ctld_not_running":
            pass

    async def register_master(self, websocket: WebSocket, data: dict):
        hostname = data["pod_hostname"]
        pod_node_info = {
            "pod_hostname": data["pod_hostname"],
            "role": "master"
        }
        machine_info = {
            "machine_hostname": data["machine_hostname"],
            "machine_ip": data["machine_ip"]
        }
        self.slurm_config.slurmctld_host = hostname
        logger.info(f"Updating slurm.conf with hostname: {hostname}")

        # Update the websocket metadata
        self.client_sockets.update_client_meta(
            websocket, "node_compute_info", pod_node_info
        )
        self.client_sockets.update_client_meta(
            websocket, "machine_info", machine_info
        )

        self.client_sockets.get_client(websocket).set_name(data["machine_hostname"])
        self.client_sockets.get_client(websocket).set_pod_name(data["pod_hostname"])

        await self.broadcast_update_nodes()

    async def register_worker(self, websocket: WebSocket, data: dict):
        pod_node_info = {
            "node_name": data["pod_hostname"],
            "node_addr": data["pod_ip_address"],
            "sockets": data["pod_sockets"],
            "cores_per_socket": data["pod_cores_per_socket"],
            "threads_per_core": data["pod_threads_per_core"],
            "real_memory": data["pod_real_memory"],
            "gres": f"gpu:{data['pod_gpus']}",
        }
        machine_info = {
            "machine_hostname": data["machine_hostname"],
            "machine_ip": data["machine_ip"]
        }

        self.slurm_config.add_node(**pod_node_info)

        # Update the websocket metadata
        self.client_sockets.update_client_meta(
            websocket, "pod_compute_info", pod_node_info
        )
        self.client_sockets.update_client_meta(
            websocket, "machine_info", machine_info
        )
        self.client_sockets.get_client(websocket).set_name(data["machine_hostname"])
        self.client_sockets.get_client(websocket).set_pod_name(data["pod_hostname"])

        await self.broadcast_update_nodes()

    async def start_node(self, websocket: WebSocket):
        slurm_conf = self.slurm_config.generate_conf()
        message = {
            "command": "start_node",
            "data": {"slurm_conf": slurm_conf},
        }
        logger.info(f"Sending message: {message}")
        await websocket.send_text(json.dumps(message))

    async def broadcast_update_nodes(self):
        slurm_conf = self.slurm_config.generate_conf()
        logger.info("Broadcasting update nodes: \n\n{}".format(slurm_conf))

        # Create a list of WebSocket keys to avoid modifying the dictionary while iterating
        for websocket in list(self.client_sockets.get_clients().keys()):
            try:
                data = {
                    "command": "update_node",
                    "data": {"slurm_conf": slurm_conf},
                }
                await websocket.send_text(json.dumps(data))
                logger.info(f"Updated node: {websocket.client.host}")
            except Exception as e:
                logger.error(f"Failed to notify client: {websocket.client.host} due to error: {e}")
                # Remove the client that caused the error, as it's no longer connected
                self.client_sockets.remove_client(websocket)
                logger.info(f"Removed WebSocket {websocket.client.host} from client list.")

config_server = ConfigServer()

