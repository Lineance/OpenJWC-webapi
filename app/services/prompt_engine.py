from app.utils.logging_manager import setup_logger
from datetime import date
from app.services.sql_db_service import db

logger = setup_logger("prompt_engine_logs")


class PromptEngine:
    @staticmethod
    def build_chat_prompt(history, user_query, context=None):
        """在这里统一管理 Prompt 模板"""
        messages = [msg.model_dump() for msg in history]

        if context:
            logger.info("Prompt Engine RAG模式")
            # RAG 模式：注入背景知识
            system_prompt = f"""
以下是和用户提问相关的资讯或背景知识：{context}
今天的日期为{date.today()}
{db.get_system_setting("system_prompt")}
"""
        else:
            # 普通模式
            logger.info("Prompt Engine 普通模式")
            system_prompt = "你是一个教务处智能助手，请友好地回答学生问题。"

        messages.insert(0, {"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_query})
        preview_length = int(db.get_system_setting("prompt_preview_length"))
        preview_prompt = (
            system_prompt
            if len(system_prompt) <= preview_length
            else system_prompt[:preview_length] + "..."
        )
        logger.info(f"已注入系统提示词：{preview_prompt}")
        logger.debug(messages)
        return messages
