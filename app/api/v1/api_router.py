from app.api.v1.client import device, chat, notices
from app.api.v1.admin import auth, settings, monitor, apikeys, logs
from fastapi import APIRouter

client_router = APIRouter(prefix="/client")

client_router.include_router(device.router)
client_router.include_router(chat.router)
client_router.include_router(notices.router)

admin_router = APIRouter(prefix="/admin")

admin_router.include_router(auth.router)
admin_router.include_router(settings.router)
admin_router.include_router(monitor.router)
admin_router.include_router(apikeys.router)
admin_router.include_router(logs.router)
