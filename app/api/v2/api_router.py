from app.api.v2.client import auth, device
from fastapi import APIRouter

v2_client_router = APIRouter(prefix="/client")

v2_client_router.include_router(auth.router)
v2_client_router.include_router(device.router)
