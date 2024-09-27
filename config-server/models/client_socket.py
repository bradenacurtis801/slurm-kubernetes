# models/client_socket.py

from fastapi import WebSocket
import logging

logger = logging.getLogger(__name__)

class ClientSocket:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.meta = {}
        self.node_name = None
        self.pod_name = None

    def get_name(self):
        return self.node_name

    def get_pod_name(self):
        return self.pod_name

    def set_name(self, name: str):
        self.node_name = name

    def set_pod_name(self, name: str):
        self.pod_name = name

    def update_meta(self, key: str, value):
        self.meta[key] = value

    def get_meta(self):
        return self.meta

    def to_dict(self):
        return {
            "node_name": self.node_name,
            "pod_name": self.pod_name,
            "meta": self.meta
        }

    def __str__(self):
        return str(self.websocket)

class ClientSockets:
    def __init__(self):
        self.clients = {}

    def add_client(self, websocket: WebSocket):
        self.clients[websocket] = ClientSocket(websocket)

    def remove_client(self, websocket: WebSocket):
        if websocket in self.clients:
            del self.clients[websocket]
            logger.info(f"Client {websocket.client.host} removed successfully.")
        else:
            logger.error(f"Client {websocket.client.host} not found in the client list.")

    def get_clients(self):
        return self.clients

    def get_client(self, websocket: WebSocket):
        return self.clients.get(websocket)

    def get_client_metadata(self, client_id: str):
        for client_socket in self.clients.values():
            if str(client_socket) == client_id:
                return client_socket.to_dict()
        return None

    def get_all_clients_metadata(self):
        return {str(client): client.to_dict() for client in self.clients.values()}

    def update_client_meta(self, websocket: WebSocket, key: str, value):
        if websocket in self.clients:
            self.clients[websocket].update_meta(key, value)
