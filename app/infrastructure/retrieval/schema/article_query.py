from __future__ import annotations

from app.infrastructure.retrieval.schema.article import (
    Any,
    Article,
    LanceModel,
    Vector,
    cast,
    contextlib,
    datetime,
    pa,
)

class ArticleQuery(LanceModel):
    """文章查询模型"""

    keyword: str | None = None
    search_fields: list[str] = ["title", "content_text"]

    vector_query: list[float] | None = None
    vector_field: str = "content_embedding"
    similarity_threshold: float = 0.7

    source_site: str | None = None
    author: str | None = None
    tags: list[str] | None = None
    start_date: datetime.datetime | None = None
    end_date: datetime.datetime | None = None
    min_crawl_version: int | None = None

    limit: int = 10
    offset: int = 0
    order_by: str = "publish_date"
    order_desc: bool = True

    keyword_weight: float = 0.3
    vector_weight: float = 0.7

    def build_where_clause(self) -> str:
        """构建 WHERE 子句"""
        conditions = []

        if self.source_site:
            conditions.append(f"source_site = '{self.source_site}'")

        if self.author:
            conditions.append(f"author = '{self.author}'")

        if self.tags:
            tags_str = ", ".join(f"'{tag}'" for tag in self.tags)
            conditions.append(f"tags IN ({tags_str})")

        if self.start_date:

            start_str = self.start_date.isoformat()
            conditions.append(f"publish_date >= CAST('{start_str}' AS timestamp)")

        if self.end_date:
            end_str = self.end_date.isoformat()
            conditions.append(f"publish_date <= CAST('{end_str}' AS timestamp)")

        if self.min_crawl_version:
            conditions.append(f"crawl_version >= {self.min_crawl_version}")

        return " AND ".join(conditions) if conditions else "1=1"

    def validate_data(self) -> tuple[bool, list[str]]:
        """验证查询参数"""
        errors = []

        if self.limit <= 0 or self.limit > 100:
            errors.append("limit must be between 1 and 100")

        if self.offset < 0:
            errors.append("offset must be >= 0")

        if self.keyword_weight + self.vector_weight != 1.0:
            errors.append("keyword_weight + vector_weight must equal 1.0")

        if self.vector_query and self.vector_field not in ["title_embedding", "content_embedding"]:
            errors.append(
                f"vector_field must be 'title_embedding' or 'content_embedding', got {self.vector_field}"
            )

        if self.vector_query:
            if self.vector_field == "title_embedding":
                expected_dim = 384
            elif self.vector_field == "content_embedding":
                expected_dim = 1024
            elif self.vector_field == "both_embedding":
                expected_dim = 1024
            else:
                expected_dim = 1024
            if len(self.vector_query) != expected_dim:
                errors.append(
                    f"vector_query dimension mismatch: expected {expected_dim}, got {len(self.vector_query)}"
                )

        return len(errors) == 0, errors
