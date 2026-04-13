import hashlib
import json

from app.domain.submission.models import (
    SubmissionDraft,
    SubmissionRecord,
    SubmissionStatus,
)


def build_submission_id(draft: SubmissionDraft) -> str:
    raw = f"{draft.title}{draft.date}{draft.detail_url or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def calculate_submission_size(draft: SubmissionDraft) -> int:
    return max(
        len(draft.label),
        len(draft.title),
        len(draft.detail_url or ""),
        len(draft.content.text),
        len(json.dumps(draft.content.attachment_urls, ensure_ascii=False)),
    )


def validate_submission_size(draft: SubmissionDraft, max_length: int) -> bool:
    return calculate_submission_size(draft) <= max_length


def create_submission_record(
    draft: SubmissionDraft, submitter_id: str
) -> SubmissionRecord:
    return SubmissionRecord(
        submission_id=build_submission_id(draft),
        submitter_id=submitter_id,
        label=draft.label,
        title=draft.title,
        date=draft.date,
        detail_url=draft.detail_url,
        is_page=draft.is_page,
        content_text=draft.content.text,
        attachment_urls=draft.content.attachment_urls,
    )


def to_notice_data(record: SubmissionRecord) -> dict[str, object]:
    return {
        "id": record.submission_id,
        "label": record.label or "用户投稿",
        "title": record.title,
        "date": record.date,
        "detail_url": record.detail_url or "",
        "is_page": record.is_page,
        "content_text": record.content_text,
        "attachments": record.attachment_urls,
    }


def parse_status(status: str) -> SubmissionStatus | None:
    try:
        return SubmissionStatus(status)
    except ValueError:
        return None
