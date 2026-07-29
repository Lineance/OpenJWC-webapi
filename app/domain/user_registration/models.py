from dataclasses import dataclass
from enum import StrEnum
from typing import Self

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

    @classmethod
    def pending(cls, *, id: int, username: str, email: str, created_at: str = "") -> Self:
        return cls(id=id, username=username, email=email, created_at=created_at)

    @classmethod
    def from_storage(
        cls,
        *,
        id: int,
        username: str,
        email: str,
        status: UserRegistrationStatus,
        created_at: str,
    ) -> Self:
        return cls(id=id, username=username, email=email, status=status, created_at=created_at)
