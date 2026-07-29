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
    cast,
    logger,
    logging,
    os,
    snapshot_download,
    threading,
    try_to_load_from_cache,
)

def _iter_model_cache_dirs() -> list[Path]:
    """返回可能的 Hugging Face Hub 缓存目录（按优先级）。"""
    cache_dirs: list[Path] = []

    hub_cache = os.getenv("HUGGINGFACE_HUB_CACHE")
    if hub_cache:
        cache_dirs.append(Path(hub_cache).expanduser())

    hf_home = os.getenv("HF_HOME")
    if hf_home:
        cache_dirs.append(Path(hf_home).expanduser() / "hub")

    cache_dirs.append(Path(LOCAL_MODEL_CACHE).expanduser())

    project_tmp_hub = (
        Path(__file__).resolve().parents[3] / "tmp" / "huggingface" / "hub"
    )
    cache_dirs.append(project_tmp_hub)

    unique_dirs: list[Path] = []
    seen: set[str] = set()
    for cache_dir in cache_dirs:
        key = str(cache_dir)
        if key in seen:
            continue
        seen.add(key)
        unique_dirs.append(cache_dir)

    return unique_dirs

def _find_model_snapshot_path(model_name: str) -> str | None:
    """从缓存目录中解析模型快照路径。"""
    model_cache_key = f"models--{model_name.replace('/', '--')}"

    for cache_root in _iter_model_cache_dirs():
        model_dir = cache_root / model_cache_key
        if not model_dir.exists():
            continue

        refs_main = model_dir / "refs" / "main"
        if refs_main.exists():
            try:
                commit_hash = refs_main.read_text(encoding="utf-8").strip()
                if commit_hash:
                    snapshot_dir = model_dir / "snapshots" / commit_hash
                    if snapshot_dir.exists():
                        return str(snapshot_dir)
            except Exception as e:
                logger.debug(
                    f"Failed to parse refs/main for {model_name} in {model_dir}: {e}"
                )

        snapshots_dir = model_dir / "snapshots"
        if snapshots_dir.exists():
            try:
                for snapshot_dir in sorted(
                    [p for p in snapshots_dir.iterdir() if p.is_dir()],
                    key=lambda p: p.name,
                    reverse=True,
                ):
                    if snapshot_dir.exists():
                        return str(snapshot_dir)
            except Exception as e:
                logger.debug(
                    f"Failed to inspect snapshots for {model_name} in {model_dir}: {e}"
                )

    return None
