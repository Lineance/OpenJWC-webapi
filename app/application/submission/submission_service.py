from app.domain.submission import (
    SubmissionDraft,
    SubmissionStatus,
    create_submission_record,
    parse_status,
    validate_submission_size,
)
from app.infrastructure.ingestion.adapters.submission import SubmissionAdapter
from app.infrastructure.ingestion.pipeline import IngestionPipeline
from app.infrastructure.ingestion.validators import DocumentValidator, URLValidator
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.infrastructure.storage.sqlite.submission_repository import SubmissionRepository
from app.utils.logging_manager import setup_logger

logger = setup_logger("audit_service")
submission_repository = SubmissionRepository(db)
submission_adapter = SubmissionAdapter()
submission_pipeline = IngestionPipeline(
    validator=DocumentValidator(
        url_validator=URLValidator(require_domain_whitelist=False)
    )
)


def _get_submission_max_length() -> int:
    setting_value = db.get_system_setting("submission_max_length")
    return int(setting_value or "10000")


def submit_submission(draft: SubmissionDraft, submitter_id: str) -> tuple[bool, str]:
    max_length = _get_submission_max_length()
    if not validate_submission_size(draft, max_length):
        return False, f"正文文字量超过上限:{max_length}"

    created = submission_repository.create(
        create_submission_record(draft, submitter_id)
    )
    if not created:
        return False, "投稿已存在或提交失败"

    return True, "提交成功"


def get_submissions_for_admin(
    page: int,
    size: int,
    status: str | None,
) -> tuple[int, list[dict]]:
    offset = size * (page - 1)
    return submission_repository.list_for_admin(
        status=status, offset=offset, limit=size
    )


def get_submission_detail(submission_id: str) -> dict | None:
    record = submission_repository.get_by_id(submission_id)
    if record is None:
        return None
    return {
        "id": record.submission_id,
        "label": record.label,
        "title": record.title,
        "date": record.date,
        "detail_url": record.detail_url,
        "is_page": record.is_page,
        "content_text": record.content_text,
        "attachments": record.attachment_urls,
        "status": record.status,
        "review": record.review,
    }


def get_my_submissions(submitter_id: str) -> list[dict]:
    return submission_repository.list_by_submitter(submitter_id)


def audit_and_import_submission(submission_id: str, status: str, review: str) -> bool:
    parsed_status = parse_status(status)
    if parsed_status is None:
        logger.warning(f"不支持的审核状态: {status}")
        return False

    if parsed_status == SubmissionStatus.APPROVED:
        record = submission_repository.get_by_id(submission_id)
        if not record:
            logger.warning(f"submissions表中不存在ID为 {submission_id} 的记录")
            return False

        if len(str(record.content_text)) > _get_submission_max_length():
            logger.warning("该投稿文字量过大，已拦截入库。")
            return False

        doc = submission_adapter.convert_one(record, review)
        process_result = submission_pipeline.process_one(doc)
        if process_result.status not in ("success", "duplicate"):
            logger.error(
                f"投稿入库到 ingestion pipeline 失败: {process_result.status} {process_result.message}"
            )
            return False

    updated = submission_repository.update_status(submission_id, status, review)
    logger.info(f"整个审核提交流程完成，ID: {submission_id}")
    return updated
