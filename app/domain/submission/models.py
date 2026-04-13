from dataclasses import dataclass, field
from enum import StrEnum


class SubmissionStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass(slots=True)
class SubmissionContent:
    attachment_urls: list[str] = field(default_factory=list)
    text: str = ""


@dataclass(slots=True)
class SubmissionDraft:
    label: str
    title: str
    date: str
    detail_url: str | None
    is_page: bool
    content: SubmissionContent


@dataclass(slots=True)
class SubmissionRecord:
    submission_id: str
    submitter_id: str
    label: str
    title: str
    date: str
    detail_url: str | None
    is_page: bool
    content_text: str
    attachment_urls: list[str] = field(default_factory=list)
    status: SubmissionStatus = SubmissionStatus.PENDING
    review: str = ""
