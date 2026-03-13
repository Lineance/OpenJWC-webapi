from app.utils.logging_manager import setup_logger

logger = setup_logger("prompt_engine_logs")


class PromptEngine:
    @staticmethod
    def build_chat_prompt(history, user_query, context=None):
        """在这里统一管理 Prompt 模板"""
        messages = [msg.model_dump() for msg in history]

        if context:
            logger.info("Prompt Engine RAG模式")
            # RAG 模式：注入背景知识
            system_prompt = (
                f"你是一个教务处智能助手，请根据以下教务处信息回答问题：\n{context}"
            )
        else:
            # 普通模式
            logger.info("Prompt Engine 普通模式")
            system_prompt = "你是一个教务处智能助手，请友好地回答学生问题。"

        messages.insert(0, {"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_query})
        logger.info(f"已注入系统提示词：{system_prompt}")
        logger.debug(messages)
        return messages
