from dataclasses import dataclass
from enum import StrEnum


class UserRegistrationStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass(slots=True)
class UserRegistrationRecord:
    id: int
    username: str
    email: str
    status: UserRegistrationStatus = UserRegistrationStatus.PENDING
    created_at: str = ""
