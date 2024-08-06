# main.py

import asyncio
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from routes import api
from sockets.config_server import ConfigServer
from utils.logger import setup_logger

logger = setup_logger()

config_server = ConfigServer()
app = FastAPI()
app.include_router(api.router)

@app.websocket("/ws/notify")
async def websocket_endpoint(websocket: WebSocket):
    await config_server.on_connect(websocket)
    try:
        while True:
            message = await websocket.receive_text()
            await config_server.handle_message(websocket, message)
    except WebSocketDisconnect:
        await config_server.on_disconnect(websocket)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=3000, log_level="info")
