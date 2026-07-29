from dataclasses import dataclass, field
from enum import StrEnum
from app.domain.submission.model_parts.status import (
    SubmissionStatus,
)
from app.domain.submission.model_parts.draft import (
    SubmissionContent,
    SubmissionDraft,
)
from app.domain.submission.model_parts.record import (
    SubmissionRecord,
)
