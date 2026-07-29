from __future__ import annotations

from app.infrastructure.ingestion.dedup import (
    Any,
    DEFAULT_SIMHASH_ENABLED,
    SIMHASH_BITS,
    SIMHASH_DISTANCE_THRESHOLD,
    SimHash,
    compute_simhash,
    compute_url_hash,
    dataclass,
    field,
    hashlib,
    is_similar,
    logger,
    logging,
    normalize_url,
    re,
    url_hash,
)

@dataclass
class DedupResult:
    """三岔去重结果"""

    new_docs: list[dict[str, Any]] = field(default_factory=list)
    upsert_docs: list[dict[str, Any]] = field(default_factory=list)
    duplicate_docs: list[dict[str, Any]] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.new_docs or self.upsert_docs or self.duplicate_docs)
