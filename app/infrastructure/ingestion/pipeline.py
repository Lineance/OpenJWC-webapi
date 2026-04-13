"""
IngestionPipeline - ETL 数据摄取管道

完整的数据处理流程: 验证 → 标准化 → 去重 → 向量化 → 写入

Responsibilities:
    - 数据验证
    - 内容标准化 (Markdown → 纯文本)
    - 重复检测
    - 向量嵌入生成
    - 原子写入 LanceDB
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.infrastructure.storage.lancedb import (
    ArticleFields,
    ArticleRepository,
    get_article_repository,
    init_database,
)

from .dedup import DuplicateDetector, RepositoryDedup
from .embedder import Embedder, get_embedder
from .normalizers import normalize_content, normalize_datetime, normalize_markdown
from .tag_matcher import TagMatcher, get_tag_matcher
from .validators import DocumentValidator, ValidationResult

logger = logging.getLogger(__name__)


# =============================================================================
# 处理结果
# =============================================================================


@dataclass
class ProcessResult:
    """
    单条记录处理结果

    Attributes:
        news_id: 新闻 ID
        url: URL
        status: 状态 (success, invalid, duplicate, error)
        message: 附加信息
    """

    news_id: str | None = None
    url: str | None = None
    status: str = "unknown"
    message: str = ""


@dataclass
class PipelineResult:
    """
    管道批处理结果

    Attributes:
        total: 总记录数
        success: 成功数
        invalid: 验证失败数
        duplicate: 重复数
        error: 错误数
        results: 每条记录的处理结果
        elapsed_seconds: 耗时
    """

    total: int = 0
    success: int = 0
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
            f"invalid={self.invalid}, duplicate={self.duplicate}, error={self.error}, "
            f"elapsed={self.elapsed_seconds:.2f}s"
        )


# =============================================================================
# 数据摄取管道
# =============================================================================


class IngestionPipeline:
    """
    数据摄取管道

    完整的 ETL 处理流程:
    1. 验证: 检查必填字段、URL 格式、内容长度
    2. 标准化: Markdown → 纯文本、日期格式化、Unicode 规范化
    3. 去重: URL 哈希检查、数据库查重
    4. 向量化: 生成标题和正文嵌入
    5. 写入: 原子写入 LanceDB

    Usage:
        >>> pipeline = IngestionPipeline()
        >>> result = pipeline.process_batch(documents)
        >>> print(result.summary())
    """

    def __init__(
        self,
        repository: ArticleRepository | None = None,
        embedder: Embedder | None = None,
        validator: DocumentValidator | None = None,
        tag_matcher: TagMatcher | None = None,
        skip_validation: bool = False,
        skip_dedup: bool = False,
        skip_embedding: bool = False,
        skip_tag_matching: bool = False,
        db_path: str | None = None,
    ):
        """
        初始化管道

        Args:
            repository: 数据仓库
            embedder: 向量化客户端
            validator: 文档验证器
            tag_matcher: 标签匹配器
            skip_validation: 跳过验证
            skip_dedup: 跳过去重
            skip_embedding: 跳过向量化
            skip_tag_matching: 跳过标签匹配
            db_path: 数据库路径
        """
        # 初始化数据库
        if db_path:
            init_database(db_path)

        self._repository = repository or get_article_repository()
        self._embedder = embedder or get_embedder()
        self._validator = validator or DocumentValidator()
        self._tag_matcher = tag_matcher or get_tag_matcher()
        self._dedup = RepositoryDedup(self._repository)
        self._detector = DuplicateDetector()

        self._skip_validation = skip_validation
        self._skip_dedup = skip_dedup
        self._skip_embedding = skip_embedding
        self._skip_tag_matching = skip_tag_matching

        logger.info("IngestionPipeline initialized")

    # =========================================================================
    # 核心处理方法
    # =========================================================================

    def process_one(self, raw_data: dict[str, Any]) -> ProcessResult:
        """
        处理单条记录

        Args:
            raw_data: 原始数据

        Returns:
            处理结果
        """
        news_id = raw_data.get("news_id")
        url = raw_data.get("url")

        try:
            # 1. 验证
            if not self._skip_validation:
                validation = self._validate(raw_data)
                if not validation.is_valid:
                    return ProcessResult(
                        news_id=news_id,
                        url=url,
                        status="invalid",
                        message="; ".join(validation.errors),
                    )

            # 2. 标准化
            normalized = self._normalize(raw_data)

            # 3. 去重检查
            if not self._skip_dedup and self._is_duplicate(normalized):
                return ProcessResult(
                    news_id=news_id,
                    url=url,
                    status="duplicate",
                    message="Document already exists",
                )

            # 4. 向量化
            if not self._skip_embedding:
                normalized = self._embed(normalized)

            # 5. 写入
            success = self._write(normalized)
            if success:
                return ProcessResult(
                    news_id=news_id,
                    url=url,
                    status="success",
                    message="",
                )
            else:
                return ProcessResult(
                    news_id=news_id,
                    url=url,
                    status="error",
                    message="Failed to write to database",
                )

        except Exception as e:
            logger.exception(f"Error processing document: {e}")
            return ProcessResult(
                news_id=news_id,
                url=url,
                status="error",
                message=str(e),
            )

    def process_batch(
        self,
        raw_data_list: list[dict[str, Any]],
        batch_size: int = 32,
    ) -> PipelineResult:
        """
        批量处理记录

        Args:
            raw_data_list: 原始数据列表
            batch_size: 向量化批处理大小

        Returns:
            批处理结果
        """
        import time

        start_time = time.time()
        result = PipelineResult()

        if not raw_data_list:
            return result

        logger.info(f"Processing batch of {len(raw_data_list)} documents")

        # 1. 验证
        if not self._skip_validation:
            valid_docs = []
            for doc in raw_data_list:
                validation = self._validate(doc)
                if validation.is_valid:
                    valid_docs.append(doc)
                else:
                    result.add_result(
                        ProcessResult(
                            news_id=doc.get("news_id"),
                            url=doc.get("url"),
                            status="invalid",
                            message="; ".join(validation.errors),
                        )
                    )
        else:
            valid_docs = raw_data_list

        # 2. 标准化
        normalized_docs = [self._normalize(doc) for doc in valid_docs]

        # 3. 去重
        if not self._skip_dedup:
            # 批内去重
            unique_docs, dup_docs = self._detector.find_duplicates(normalized_docs)

            # 数据库去重
            new_docs, existing_docs = self._dedup.filter_new_documents(unique_docs)

            # 记录重复
            for doc in dup_docs + existing_docs:
                result.add_result(
                    ProcessResult(
                        news_id=doc.get("news_id"),
                        url=doc.get("url"),
                        status="duplicate",
                        message="Document already exists",
                    )
                )
            docs_to_process = new_docs
        else:
            docs_to_process = normalized_docs

        # 4. 批量向量化
        if not self._skip_embedding and docs_to_process:
            logger.info(f"Starting embedding for {len(docs_to_process)} documents...")
            try:
                docs_to_process = self._embed_batch(docs_to_process, batch_size)
                logger.info(f"Embedding completed for {len(docs_to_process)} documents")
            except Exception as e:
                logger.error(f"Embedding failed: {e}")
                raise

        # 5. 批量写入
        if docs_to_process:
            logger.info(f"Starting write for {len(docs_to_process)} documents...")
            try:
                self._repository.add(docs_to_process)
                logger.info(f"Write completed for {len(docs_to_process)} documents")
                for doc in docs_to_process:
                    result.add_result(
                        ProcessResult(
                            news_id=doc.get(ArticleFields.NEWS_ID),
                            url=doc.get(ArticleFields.URL),
                            status="success",
                            message="",
                        )
                    )
            except Exception as e:
                logger.error(f"Batch write failed: {e}")
                for doc in docs_to_process:
                    result.add_result(
                        ProcessResult(
                            news_id=doc.get("news_id"),
                            url=doc.get("url"),
                            status="error",
                            message=str(e),
                        )
                    )

        result.elapsed_seconds = time.time() - start_time
        logger.info(result.summary())
        return result

    # =========================================================================
    # 内部方法
    # =========================================================================

    def _validate(self, data: dict[str, Any]) -> ValidationResult:
        """验证数据"""
        return self._validator.validate(data)

    def _normalize(self, data: dict[str, Any]) -> dict[str, Any]:
        """标准化数据"""
        result = {}

        # 必填字段
        result[ArticleFields.NEWS_ID] = data.get("news_id", "")

        # 标题处理：如果标题为空，尝试从内容中提取第一句作为标题
        raw_title = data.get("title", "")
        if not raw_title:
            # 尝试从内容中提取标题
            content_markdown = data.get("content_markdown", "")
            if content_markdown:
                from .normalizers import extract_first_sentence

                extracted_title = extract_first_sentence(
                    content_markdown, is_markdown=True, max_title_length=100
                )
                if extracted_title:
                    raw_title = extracted_title
                    logger.debug(f"从内容中提取标题: {extracted_title[:50]}...")

        result[ArticleFields.TITLE] = raw_title
        result[ArticleFields.URL] = data.get("url", "")

        # 日期标准化
        result[ArticleFields.PUBLISH_DATE] = normalize_datetime(data.get("publish_date"))

        # 可选字段
        result[ArticleFields.SOURCE_SITE] = data.get("source_site", "")
        result[ArticleFields.AUTHOR] = data.get("author", "")
        result[ArticleFields.TAGS] = data.get("tags", [])

        # 内容处理
        content_markdown = data.get("content_markdown", "")
        content_markdown = normalize_markdown(content_markdown)
        result[ArticleFields.CONTENT_MARKDOWN] = content_markdown

        # Markdown → 纯文本
        if "content_text" in data and data["content_text"]:
            result[ArticleFields.CONTENT_TEXT] = data["content_text"]
        else:
            result[ArticleFields.CONTENT_TEXT] = normalize_content(
                content_markdown, is_markdown=True
            )

        # 版本控制
        result[ArticleFields.CRAWL_VERSION] = data.get("crawl_version", 1)
        result[ArticleFields.LAST_UPDATED] = datetime.now()

        # 元数据
        metadata = data.get("metadata")
        if metadata:
            import json

            result[ArticleFields.METADATA] = (
                json.dumps(metadata, ensure_ascii=False) if isinstance(metadata, dict) else metadata
            )
        else:
            result[ArticleFields.METADATA] = None

        return result

    def _is_duplicate(self, data: dict[str, Any]) -> bool:
        """检查是否重复"""
        news_id = data.get(ArticleFields.NEWS_ID)
        if news_id and self._dedup.exists_by_id(news_id):
            return True

        url = data.get(ArticleFields.URL)
        return bool(url and self._dedup.exists_by_url(url))

    def _embed(self, data: dict[str, Any]) -> dict[str, Any]:
        """生成向量嵌入"""
        title = data.get(ArticleFields.TITLE, "")
        content = data.get(ArticleFields.CONTENT_TEXT, "")

        # 使用 embed_batch 方法
        title_vecs, content_vecs = self._embedder.embed_batch([title], [content])

        data[ArticleFields.TITLE_EMBEDDING] = title_vecs[0] if title_vecs else []
        data[ArticleFields.CONTENT_EMBEDDING] = content_vecs[0] if content_vecs else []

        # 标签匹配（如果不跳过）
        if not self._skip_tag_matching:
            data = self._match_tags(data)

        return data

    def _match_tags(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        匹配内容标签

        Args:
            data: 包含内容向量的数据

        Returns:
            更新后的数据（包含匹配的标签）
        """
        content_embedding = data.get(ArticleFields.CONTENT_EMBEDDING)
        if not content_embedding:
            logger.warning("Cannot match tags: content embedding is missing")
            return data

        try:
            # 使用标签匹配器查找相似标签
            matched_tags = self._tag_matcher.match_tags(content_embedding)

            if matched_tags:
                # 合并现有标签和新匹配的标签（去重）
                existing_tags = data.get(ArticleFields.TAGS, [])
                all_tags = list(set(existing_tags + matched_tags))
                data[ArticleFields.TAGS] = all_tags
                logger.debug(
                    f"Matched {len(matched_tags)} tags for article: {data.get(ArticleFields.NEWS_ID)}"
                )
            else:
                logger.debug(f"No tags matched for article: {data.get(ArticleFields.NEWS_ID)}")

            return data
        except Exception as e:
            logger.error(f"Failed to match tags: {e}")
            return data

    def _embed_batch(
        self,
        docs: list[dict[str, Any]],
        batch_size: int = 32,
    ) -> list[dict[str, Any]]:
        """批量生成向量嵌入"""
        # 提取标题和内容
        titles = [doc.get(ArticleFields.TITLE, "") for doc in docs]
        contents = [doc.get(ArticleFields.CONTENT_TEXT, "") for doc in docs]

        # 批量向量化
        title_vecs, content_vecs = self._embedder.embed_batch(titles, contents, batch_size)

        # 将向量添加回文档
        for i, (title_vec, content_vec) in enumerate(zip(title_vecs, content_vecs, strict=False)):
            docs[i][ArticleFields.TITLE_EMBEDDING] = title_vec
            docs[i][ArticleFields.CONTENT_EMBEDDING] = content_vec

        return docs

    def _write(self, data: dict[str, Any]) -> bool:
        """写入数据库"""
        return bool(self._repository.add_one(data))


# =============================================================================
# 便捷函数
# =============================================================================


def create_pipeline(
    db_path: str | None = None,
    skip_validation: bool = False,
    skip_dedup: bool = False,
) -> IngestionPipeline:
    """
    创建数据摄取管道

    Args:
        db_path: 数据库路径
        skip_validation: 跳过验证
        skip_dedup: 跳过去重

    Returns:
        IngestionPipeline 实例
    """
    return IngestionPipeline(
        db_path=db_path,
        skip_validation=skip_validation,
        skip_dedup=skip_dedup,
    )


def ingest_documents(
    documents: list[dict[str, Any]],
    db_path: str | None = None,
) -> PipelineResult:
    """
    快速导入文档

    Args:
        documents: 文档列表
        db_path: 数据库路径

    Returns:
        处理结果
    """
    pipeline = create_pipeline(db_path=db_path)
    return pipeline.process_batch(documents)
