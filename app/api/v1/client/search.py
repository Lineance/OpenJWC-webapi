import json
from asyncio import to_thread

from fastapi import APIRouter, Depends

from app.api.dependencies import verify_api_key
from app.api.logging_route import LoggingRoute
from app.core.config import DATA_DIR
from app.infrastructure.retrieval.engine import RetrievalEngine
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.models.schemas import ResponseModel, SemanticSearchRequest
from app.utils.logging_manager import setup_logger

logger = setup_logger("search_logs")
retrieval_engine: RetrievalEngine | None = None


def _get_retrieval_engine() -> RetrievalEngine:
    global retrieval_engine
    if retrieval_engine is None:
        retrieval_engine = RetrievalEngine(
            db_path=str(DATA_DIR / "lancedb"), table_name="articles"
        )
    return retrieval_engine


router = APIRouter(prefix="/notices/search", route_class=LoggingRoute)


@router.post("", response_model=ResponseModel)
async def hybrid_search(
    request: SemanticSearchRequest,
    valid_token: str = Depends(verify_api_key),
):
    """
    混合搜索资讯接口（向量+全文），强制鉴权，消耗嵌入模型额度

    Args:
        request: 搜索请求，包含查询文本、返回数量上限、最低相似度阈值

    Returns:
        带相似度分数和元信息的搜索结果
    """
    logger.info(
        f"接受到混合搜索请求: {request.query[:50]}... Token: {valid_token[:8]}..."
    ).

    # 获取系统默认相似度阈值
    min_similarity = request.min_similarity
    if min_similarity is None:
        min_similarity = float(db.get_system_setting("search_min_similarity"))

    # 限制 top_k 范围
    top_k = max(1, min(request.top_k, 20))

    # 执行混合搜索
    search_payload = await to_thread(
        _get_retrieval_engine().search,
        request.query,
        "hybrid",
        top_k,
    )

    final_results = []
    for item in search_payload.get("results", []):
        similarity = float(item.get("_similarity") or item.get("_score") or 0.0)
        if similarity < min_similarity:
            continue

        metadata = item.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}

        tags = item.get("tags") or []
        label = tags[0] if tags else metadata.get("label")
        final_results.append(
            {
                "id": str(item.get("news_id", "")),
                "label": label,
                "title": str(item.get("title", "")),
                "date": str(item.get("publish_date", "")),
                "detail_url": str(metadata.get("detail_url") or item.get("url") or ""),
                "is_page": bool(metadata.get("is_page", True)),
                "similarity_score": similarity,
                "distance": max(0.0, 1.0 - similarity),
            }
        )

    logger.info(f"混合搜索完成，返回 {len(final_results)} 条结果")

    return ResponseModel(
        msg="搜索成功",
        data={
            "total_found": len(final_results),
            "results": final_results,
        },
    )
