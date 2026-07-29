from __future__ import annotations

from dataclasses import dataclass, field
from typing import Self

@dataclass(slots=True)
class SubmissionContent:
    attachment_urls: list[str] = field(default_factory=list)
    text: str = ""

    @classmethod
    def from_text(cls, text: str, attachment_urls: list[str] | None = None) -> Self:
        return cls(attachment_urls=list(attachment_urls or []), text=text)

@dataclass(slots=True)
class SubmissionDraft:
    label: str
    title: str
    date: str
    detail_url: str | None
    is_page: bool
    content: SubmissionContent

    @classmethod
    def for_notice(
        cls,
        *,
        label: str,
        title: str,
        date: str,
        detail_url: str | None,
        is_page: bool,
        content: SubmissionContent,
    ) -> Self:
        return cls(label, title, date, detail_url, is_page, content)
