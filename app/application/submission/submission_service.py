from app.services.sql_db_service import db
from app.services.vector_db_service import vector_db
from app.utils.logging_manager import setup_logger

logger = setup_logger("audit_service")


def audit_and_import_submission(submission_id: str, status: str, review: str) -> bool:
    """
    通过一个submissions中的投稿并将其入库。
    """
    if status == "approved":
        submission_dict = db.get_submission_by_id(submission_id)
        if not submission_dict:
            logger.warning(f"submissions表中不存在ID为 {submission_id} 的记录")
            return False
        notice_data = {
            "id": str(submission_dict["id"]),
            "label": submission_dict.get("label", "用户投稿"),
            "title": submission_dict.get("title", ""),
            "date": submission_dict.get("date", ""),
            "detail_url": submission_dict.get("detail_url", ""),
            "is_page": submission_dict.get("is_page", 0),
            "content_text": submission_dict.get("content_text", ""),
            "attachments": submission_dict.get("attachments") or "[]",
        }

        if len(notice_data["content_text"]) > int(
            db.get_system_setting("submission_max_length")
        ):
            logger.warning("该投稿文字量过大，已拦截入库。")
            return False

        is_inserted = db.insert_notice_from_dict(notice_data)
        if not is_inserted:
            logger.warning(
                f"notices表中已存在ID为 {notice_data['id']} 的记录，无法导入"
            )
            return False
        try:
            vector_db.process_and_index_notice(
                {
                    "id": notice_data["id"],
                    "title": notice_data["title"],
                    "content_text": notice_data["content_text"],
                    "date": notice_data["date"],
                }
            )
            logger.info(f"成功将记录 {notice_data['id']} 向量化并存入 Vector DB")
        except Exception as e:
            logger.error(f"向量化失败: {e}")
            db.delete_notice_by_id(notice_data["id"])
            return False
    db.update_submission_status(submission_id, status, review)
    logger.info(f"整个审核提交流程完成，ID: {submission_id}")
    return True
