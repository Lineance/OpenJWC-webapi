from __future__ import annotations

from app.infrastructure.ingestion.embedder.local_embedder import (
    Any,
    BGE_PASSAGE_PREFIX,
    BGE_QUERY_PREFIX,
    CONTENT_EMBEDDING_DIM,
    CONTENT_MODEL_NAME,
    LOCAL_MODEL_CACHE,
    Literal,
    Path,
    Self,
    SentenceTransformer,
    TITLE_EMBEDDING_DIM,
    TITLE_MODEL_FALLBACKS,
    TITLE_MODEL_NAME,
    _find_model_snapshot_path,
    _is_model_cached_locally,
    _iter_model_cache_dirs,
    cast,
    logger,
    logging,
    os,
    snapshot_download,
    threading,
    try_to_load_from_cache,
)

def _ensure_model_available(model_name: str) -> str:
    """确保模型可用：如果本地没有则从网络下载"""
    snapshot_path = _find_model_snapshot_path(model_name)
    if snapshot_path is not None:
        logger.info(f"Model '{model_name}' found in local snapshot: {snapshot_path}")
        return snapshot_path

    if _is_model_cached_locally(model_name):
        logger.info(f"Model '{model_name}' found in local cache")
        return model_name

    logger.info(
        f"Model '{model_name}' not found locally, downloading from HuggingFace Hub..."
    )

    try:

        import queue
        from threading import Thread

        result_queue: queue.Queue[str] = queue.Queue()
        error_queue: queue.Queue[Exception] = queue.Queue()

        def download_task() -> None:
            try:
                local_path = snapshot_download(repo_id=model_name)
                result_queue.put(local_path)
            except Exception as e:
                error_queue.put(e)

        thread = Thread(target=download_task, daemon=True)
        thread.start()
        thread.join(timeout=300)

        if thread.is_alive():
            raise TimeoutError(
                f"Download of model '{model_name}' timed out after 300 seconds"
            )

        if not result_queue.empty():
            local_path = result_queue.get()
            logger.info(f"Model '{model_name}' downloaded successfully to {local_path}")
            return model_name

        if not error_queue.empty():
            e = error_queue.get()
            raise RuntimeError(f"Cannot download model '{model_name}': {e}") from e

        raise RuntimeError(f"Cannot download model '{model_name}': unknown error")

    except TimeoutError as e:
        logger.error(f"Download timed out: {e}")
        raise RuntimeError(
            f"Cannot download model '{model_name}': download timed out"
        ) from e
    except Exception as e:
        logger.error(f"Failed to download model '{model_name}': {e}")
        raise RuntimeError(f"Cannot download model '{model_name}': {e}") from e

def _load_model_with_auto_download(model_name: str) -> SentenceTransformer:
    """加载模型，如果本地没有则自动从网络下载"""

    model_ref = _ensure_model_available(model_name)
    logger.info(f"Loading model '{model_name}' from: {model_ref}")
    return SentenceTransformer(model_ref)

def _load_first_available_model(
    model_names: list[str],
) -> tuple[SentenceTransformer, str]:
    """按顺序尝试加载模型，返回首个成功加载的模型及其名称。"""
    errors: list[str] = []

    for model_name in model_names:
        try:
            model = _load_model_with_auto_download(model_name)
            return model, model_name
        except Exception as e:
            errors.append(f"{model_name}: {e}")

    error_message = "; ".join(errors)
    raise RuntimeError(f"Failed to load any model from candidates: {error_message}")
