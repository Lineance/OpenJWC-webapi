"""Embedding provider factory.

Selects cloud or local embedder implementations with lazy imports so the
default cloud path does not require local model dependencies.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Protocol, cast

DEFAULT_EMBEDDING_PROVIDER: Literal["cloud", "local"] = "cloud"

logger = logging.getLogger(__name__)


class EmbeddingClient(Protocol):
    """Common embedder interface used by ingestion and retrieval."""

    def embed_titles(
        self, texts: list[str], batch_size: int = 32
    ) -> list[list[float]]: ...

    def embed_contents(
        self, texts: list[str], batch_size: int = 32
    ) -> list[list[float]]: ...

    def embed_query(self, query: str) -> list[float]: ...

    def embed_batch(
        self,
        titles: list[str],
        contents: list[str],
        batch_size: int = 32,
    ) -> tuple[list[list[float]], list[list[float]]]: ...

    def get_dimensions(self) -> dict[str, Any]: ...


EmbeddingProvider = Literal["cloud", "local"]


def _normalize_provider(provider: str | None) -> EmbeddingProvider:
    selected = (provider or DEFAULT_EMBEDDING_PROVIDER).strip().lower()
    if selected not in {"cloud", "local"}:
        raise ValueError(f"Unsupported embedding provider: {provider}")
    return cast("EmbeddingProvider", selected)


def get_embedder(provider: str | None = None) -> EmbeddingClient:
    """Get embedder by provider.

    Defaults to cloud provider.
    """
    selected = _normalize_provider(provider)

    if selected == "cloud":
        from .embedder.clouds_embedder import get_embedder as get_cloud_embedder

        return cast("EmbeddingClient", get_cloud_embedder())

    try:
        from .embedder.local_embedder import get_embedder as get_local_embedder
    except ImportError as e:
        raise RuntimeError(
            "Local embedding provider requires optional dependencies. "
            "Install with: uv pip install -e .[local]"
        ) from e

    return cast("EmbeddingClient", get_local_embedder())


def embed_title(text: str, provider: str | None = None) -> list[float]:
    embedder = get_embedder(provider)
    result = embedder.embed_titles([text])
    dims = embedder.get_dimensions().get("title", 384)
    return result[0] if result else [0.0] * int(dims)


def embed_content(text: str, provider: str | None = None) -> list[float]:
    embedder = get_embedder(provider)
    result = embedder.embed_contents([text])
    dims = embedder.get_dimensions().get("content", 1024)
    return result[0] if result else [0.0] * int(dims)


def embed_query(text: str, provider: str | None = None) -> list[float]:
    embedder = get_embedder(provider)
    return embedder.embed_query(text)


def reset_embedder(provider: str | None = None) -> None:
    """Reset provider singleton. Intended for tests."""
    selected = _normalize_provider(provider)

    if selected == "cloud":
        from .embedder.clouds_embedder import CloudEmbedder

        CloudEmbedder.reset()
        return

    try:
        from .embedder.local_embedder import Embedder
    except ImportError:
        logger.warning("Local provider is not installed; nothing to reset")
        return

    Embedder.reset()
