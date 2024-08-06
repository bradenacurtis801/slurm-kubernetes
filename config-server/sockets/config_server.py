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
        hostname = websocket.client.host
        self.client_sockets.add_client(websocket)
        logging.info(f"Client connected: {hostname}")

    async def on_disconnect(self, websocket: WebSocket):
        logging.info(f"Client disconnected: {websocket.client.host}")
        client_socket = self.client_sockets.get_client(websocket)
        if client_socket and client_socket.get_name():
            logger.info(f"Removing node from slurm config: {client_socket.get_name()}")
            self.slurm_config.remove_node(client_socket.get_name())
            await self.broadcast_update_nodes()
        logger.info(f"Removing client socket: {websocket}")
        self.client_sockets.remove_client(websocket)

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
        hostname = data.get("hostname")
        self.slurm_config.slurmctld_host = hostname
        logger.info(f"Updating slurm.conf with hostname: {hostname}")
        self.client_sockets.get_client(websocket).set_name(hostname)
        await self.start_node(websocket)
        await self.broadcast_update_nodes()

    async def register_worker(self, websocket: WebSocket, data: dict):
        node_info = {
            "node_name": data["node_name"],
            "node_addr": data["ip_address"],
            "sockets": data["sockets"],
            "cores_per_socket": data["cores_per_socket"],
            "threads_per_core": data["threads_per_core"],
            "real_memory": data["real_memory"],
            "gres": f"gpu:{data['gpus']}",
        }

        self.slurm_config.add_node(**node_info)

        # Update the websocket metadata
        self.client_sockets.update_client_meta(
            websocket, "node_compute_info", node_info
        )
        self.client_sockets.get_client(websocket).set_name(data["node_name"])

        await self.start_node(websocket)
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
        logger.info("Broadcasting update nodes")
        slurm_conf = self.slurm_config.generate_conf()
        for websocket in self.client_sockets.get_clients().keys():
            try:
                data = {
                    "command": "update_node",
                    "data": {
                        "slurm_conf": slurm_conf,
                    },
                }
                await websocket.send_text(json.dumps(data))
                logger.info(f"updated node: {websocket.client.host}")
            except Exception as e:
                logging.error(f"Failed to notify client: {e}")


config_server = ConfigServer()

