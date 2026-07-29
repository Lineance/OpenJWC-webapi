from __future__ import annotations

from app.infrastructure.ingestion.pipeline import (
    Any,
    ArticleFields,
    ArticleRepository,
    DeduplicationService,
    DocumentValidator,
    EmbeddingClient,
    TagMatcher,
    ValidationResult,
    dataclass,
    datetime,
    field,
    get_article_repository,
    get_embedder,
    get_notice_repository,
    get_tag_matcher,
    init_database,
    logger,
    logging,
    normalize_content,
    normalize_datetime,
    normalize_markdown,
)

@dataclass
class ProcessResult:
    """单条记录处理结果"""

    news_id: str | None = None
    url: str | None = None
    status: str = "unknown"
    message: str = ""

@dataclass
class PipelineResult:
    """管道批处理结果"""

    total: int = 0
    success: int = 0
    upsert: int = 0
    invalid: int = 0
    duplicate: int = 0
    error: int = 0
    results: list[ProcessResult] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    def add_result(self, result: ProcessResult) -> None:
        """添加处理结果"""
        self.results.append(result)
        self.total += 1

        if result.status == "success":
            self.success += 1
        elif result.status == "upsert":
            self.upsert += 1
        elif result.status == "invalid":
            self.invalid += 1
        elif result.status == "duplicate":
            self.duplicate += 1
        elif result.status == "error":
            self.error += 1

    def summary(self) -> str:
        """生成摘要"""
        return (
            f"Pipeline result: total={self.total}, success={self.success}, "
            f"upsert={self.upsert}, invalid={self.invalid}, "
            f"duplicate={self.duplicate}, error={self.error}, "
            f"elapsed={self.elapsed_seconds:.2f}s"
        )
