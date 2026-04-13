"""
Retrieval Layer - 检索层

提供基于 LanceDB 的混合检索功能，支持向量搜索和全文搜索。

主要模块:
- schema/article: LanceDB 数据模型定义
- utils/embedding: 检索专用向量化工具
- store: LanceDB 表操作封装
- engine: 混合检索引擎

Usage:
    >>> from retrieval import create_engine
    >>> engine = create_engine()
    >>> results = engine.search("人工智能")
    >>> print(f"找到 {len(results['results'])} 个结果")
"""

from .engine import RetrievalEngine, create_engine, get_engine
from .schema.article import Article, ArticleQuery
from .store import LanceStore, create_store, get_store
from .utils.embedding import (
    RetrievalEmbedder,
    cosine_similarity,
    embed_query,
    get_retrieval_embedder,
)

__all__ = [
    # Engine
    "RetrievalEngine",
    "create_engine",
    "get_engine",
    # Store
    "LanceStore",
    "create_store",
    "get_store",
    # Schema
    "Article",
    "ArticleQuery",
    # Embedding
    "RetrievalEmbedder",
    "get_retrieval_embedder",
    "embed_query",
    "cosine_similarity",
]
