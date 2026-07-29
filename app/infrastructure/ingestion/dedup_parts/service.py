from __future__ import annotations

from app.infrastructure.ingestion.dedup import (
    Any,
    DEFAULT_SIMHASH_ENABLED,
    DedupResult,
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

class DeduplicationService:
    """统一去重服务"""

    def __init__(
        self,
        repository: Any,
        simhash_enabled: bool = DEFAULT_SIMHASH_ENABLED,
        simhash_bits: int = SIMHASH_BITS,
        simhash_threshold: int = SIMHASH_DISTANCE_THRESHOLD,
    ) -> None:
        self._repository = repository
        self._simhash_enabled = simhash_enabled
        self._simhash = SimHash(bits=simhash_bits) if simhash_enabled else None
        self._simhash_threshold = simhash_threshold

        self._news_id_key = "news_id"
        self._url_key = "url"
        self._publish_date_key = "publish_date"
        self._content_key = "content_text"

    def dedup(
        self,
        documents: list[dict[str, Any]],
        news_id_key: str | None = None,
        url_key: str | None = None,
        publish_date_key: str | None = None,
        content_key: str | None = None,
    ) -> DedupResult:
        """三岔去重"""
        if news_id_key is not None:
            self._news_id_key = news_id_key
        if url_key is not None:
            self._url_key = url_key
        if publish_date_key is not None:
            self._publish_date_key = publish_date_key
        if content_key is not None:
            self._content_key = content_key

        if not documents:
            return DedupResult()

        batch_unique, batch_duplicates = self._in_batch_dedup(documents)

        if not batch_unique:
            return DedupResult(
                new_docs=[], upsert_docs=[], duplicate_docs=batch_duplicates
            )

        new_docs, upsert_docs, db_duplicates = self._db_dedup_three_way(batch_unique)
        all_duplicates = batch_duplicates + db_duplicates

        logger.info(
            f"Dedup result: new={len(new_docs)}, upsert={len(upsert_docs)}, "
            f"duplicate={len(all_duplicates)}"
        )

        return DedupResult(
            new_docs=new_docs,
            upsert_docs=upsert_docs,
            duplicate_docs=all_duplicates,
        )

    def _in_batch_dedup(
        self,
        documents: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """批次内去重：URL hash + 可选 SimHash"""
        unique = []
        duplicates = []
        seen_url_hashes: set[str] = set()
        seen_simhashes: list[int] = []

        for doc in documents:
            url = doc.get(self._url_key, "")
            content = doc.get(self._content_key, "")

            url_h = url_hash(url)
            if url_h in seen_url_hashes:
                duplicates.append(doc)
                continue

            if self._simhash_enabled and content:
                content_h = self._simhash.compute(content)
                for existing_h in seen_simhashes:
                    if self._simhash.is_similar(
                        content_h, existing_h, self._simhash_threshold
                    ):
                        duplicates.append(doc)
                        break
                else:
                    seen_simhashes.append(content_h)

            seen_url_hashes.add(url_h)
            unique.append(doc)

        return unique, duplicates

    def _db_dedup_three_way(
        self,
        documents: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """批量 DB 查询 + 三岔分类"""
        news_ids = [
            doc.get(self._news_id_key)
            for doc in documents
            if doc.get(self._news_id_key)
        ]

        if not news_ids:
            return documents, [], []

        existing_records = self._repository.find_by_news_ids(news_ids)
        existing_by_id: dict[str, dict[str, Any]] = {
            rec[self._news_id_key]: rec for rec in existing_records
        }

        new_docs = []
        upsert_docs = []
        duplicate_docs = []

        for doc in documents:
            news_id = doc.get(self._news_id_key)
            if not news_id:
                new_docs.append(doc)
                continue

            existing = existing_by_id.get(news_id)
            if existing is None:
                new_docs.append(doc)
                continue

            incoming_url_norm = normalize_url(doc.get(self._url_key) or "")
            existing_url_norm = normalize_url(existing.get(self._url_key) or "")

            if incoming_url_norm != existing_url_norm:
                new_docs.append(doc)
                continue

            dates_match = self._dates_match(
                doc.get(self._publish_date_key),
                existing.get(self._publish_date_key),
            )

            if dates_match:
                duplicate_docs.append(doc)
            else:
                upsert_docs.append(doc)

        return new_docs, upsert_docs, duplicate_docs

    @staticmethod
    def _dates_match(date1: Any, date2: Any) -> bool:
        """比对两个日期是否相同（处理 None 和 datetime 对象）"""
        if date1 is None and date2 is None:
            return True
        if date1 is None or date2 is None:
            return False
        dt1 = date1.isoformat() if hasattr(date1, "isoformat") else str(date1)
        dt2 = date2.isoformat() if hasattr(date2, "isoformat") else str(date2)
        return dt1 == dt2
