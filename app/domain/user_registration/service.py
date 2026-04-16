from app.domain.user_registration.models import UserRegistrationStatus


def parse_status(status: str) -> UserRegistrationStatus | None:
    try:
        return UserRegistrationStatus(status)
    except ValueError:
        return None
