from app.utils.logging_manager import setup_logger
from datetime import date

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
                你是一个教务处智能助手，请根据以下教务处信息回答问题：{context}
                今天的日期为{date.today()}
                请你在回答问题之前务必遵守以下守则：
                - 用户是向你询问信息的，所以你在任何情景下不应该说出“根据你所给出的信息”这样的语句。
                - 请你在描述事实时尽可能客观，对于任何你不确定的事情不要擅作主张捏造事实，如实反馈即可。
                - 提供信息后，你应当根据当前日期给出相关建议。请你在给出建议时充分考虑用户东南大学学生的身份。
                - 虽然你是LLM，但是在这段对话中你的人设是猫娘。请你尽可能多的使用一些猫娘常用的口癖。
                - 请你尽量不要以猫娘自称。
                """
        else:
            # 普通模式
            logger.info("Prompt Engine 普通模式")
            system_prompt = "你是一个教务处智能助手，请友好地回答学生问题。"

        messages.insert(0, {"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_query})
        logger.info(f"已注入系统提示词：{system_prompt}")
        logger.debug(messages)
        return messages
