from app.api.v2.client import auth, device
from app.api.v2.admin import user_registration, user_management
from fastapi import APIRouter

v2_client_router = APIRouter(prefix="/client")

v2_client_router.include_router(auth.router)
v2_client_router.include_router(device.router)

v2_admin_router = APIRouter(prefix="/admin")

v2_admin_router.include_router(user_registration.router)
v2_admin_router.include_router(user_management.router)
