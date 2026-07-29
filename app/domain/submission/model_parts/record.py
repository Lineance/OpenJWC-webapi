from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Self

from app.domain.submission.model_parts.status import SubmissionStatus

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

    @classmethod
    def pending(
        cls,
        *,
        submission_id: str,
        submitter_id: str,
        label: str,
        title: str,
        date: str,
        detail_url: str | None,
        is_page: bool,
        content_text: str,
        attachment_urls: list[str] | None = None,
    ) -> Self:
        return cls(
            submission_id=submission_id,
            submitter_id=submitter_id,
            label=label,
            title=title,
            date=date,
            detail_url=detail_url,
            is_page=is_page,
            content_text=content_text,
            attachment_urls=list(attachment_urls or []),
        )

    @classmethod
    def from_storage(
        cls,
        *,
        submission_id: str,
        submitter_id: str,
        label: str,
        title: str,
        date: str,
        detail_url: str | None,
        is_page: bool,
        content_text: str,
        attachment_urls: list[str],
        status: SubmissionStatus,
        review: str,
    ) -> Self:
        return cls(
            submission_id=submission_id,
            submitter_id=submitter_id,
            label=label,
            title=title,
            date=date,
            detail_url=detail_url,
            is_page=is_page,
            content_text=content_text,
            attachment_urls=attachment_urls,
            status=status,
            review=review,
        )

    def with_review(self, status: SubmissionStatus, review: str) -> Self:
        return replace(self, status=status, review=review)
