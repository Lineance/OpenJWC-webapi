from pydantic import BaseModel
from typing import List, Optional
from app.models.v2.auth_requests import (
    RegisterRequest,
    LoginRequest,
)
from app.models.v2.device_request import (
    UnbindRequest,
)
from app.models.v2.login_data import (
    LoginData,
)
from app.models.v2.device_data import (
    DeviceItem,
    DeviceListData,
)
from app.models.v2.responses import (
    V2Response,
    V2DetailResponse,
)
