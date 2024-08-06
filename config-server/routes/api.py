from fastapi import APIRouter
from fastapi.responses import JSONResponse
from main import config_server

router = APIRouter(tags=["Config API"])

@router.get("/clients")
def get_all_clients():
    return JSONResponse(
        content=config_server.client_sockets.get_all_clients_metadata()
    )

@router.get("/clients/{client_id}")
def get_client(client_id: str):
    metadata = config_server.client_sockets.get_client_metadata(client_id)
    if metadata:
        return JSONResponse(content=metadata)
    else:
        return JSONResponse(status_code=404, content={"message": "Client not found"})