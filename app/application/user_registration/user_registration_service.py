from app.domain.user_registration import parse_status, UserRegistrationStatus
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.infrastructure.storage.sqlite.user_registration_repository import UserRegistrationRepository
from app.utils.logging_manager import setup_logger

logger = setup_logger("user_registration_service")
user_registration_repository = UserRegistrationRepository(db)


def get_pending_registrations_for_admin(
    page: int,
    size: int,
    status: str | None,
) -> tuple[int, list[dict]]:
    offset = size * (page - 1)
    return user_registration_repository.list_for_admin(
        status=status, offset=offset, limit=size
    )


def get_registration_detail(user_id: str) -> dict | None:
    record = user_registration_repository.get_by_id(user_id)
    if record is None:
        return None
    return {
        "id": str(record.id),
        "username": record.username,
        "email": record.email,
        "status": record.status,
        "created_at": record.created_at,
    }


def audit_user_registration(user_id: str, status: str, review: str) -> bool:
    parsed_status = parse_status(status)
    if parsed_status is None:
        logger.warning(f"不支持的审核状态: {status}")
        return False

    if parsed_status == UserRegistrationStatus.APPROVED:
        password_hash = user_registration_repository.get_password_hash(user_id)
        if not password_hash:
            logger.warning(f"未找到用户ID {user_id} 的密码哈希")
            return False

        record = user_registration_repository.get_by_id(user_id)
        if not record:
            logger.warning(f"未找到用户ID {user_id} 的注册记录")
            return False

        try:
            db.create_user_from_registration(record.username, record.email, password_hash)
            user_registration_repository.delete(user_id)
            logger.info(f"用户注册审核通过并迁移到正式用户表: {record.username}")
        except Exception as e:
            logger.error(f"迁移用户到正式表失败: {e}")
            return False
    else:
        user_registration_repository.update_status(user_id, status, review)

    logger.info(f"用户注册审核完成，用户ID: {user_id}, 状态: {status}")
    return True
