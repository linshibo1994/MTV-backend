"""LLM 供应商抽象 — Protocol 定义。

支持两类能力：
- complete_json(system, user) -> dict：结构化单轮补全，用于 pipeline（split/extract/
  merge/enrich/parents）。返回可被 json.loads 解析的 dict，由上层 schema 校验器兜底。
- chat(messages, system) -> str：多轮对话，用于新闻播报等对话式生成。

具体实现见 llm_openai_compat.py（覆盖 OpenAI/DeepSeek/百炼/Gemini）与
llm_anthropic.py（Claude）。缺 key 时抛 RuntimeError，由调用方捕获后释放积分。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.providers.registry import ProviderError


class LLMResponseContractError(ProviderError):
    """LLM 响应不满足 Provider 对外契约。"""

    def __init__(self) -> None:
        super().__init__("LLM 供应商返回了无效的 assistant 正文")


def require_non_empty_chat_content(content: object) -> str:
    """校验并返回非空 assistant 正文，不在异常中暴露响应内容。"""
    if not isinstance(content, str) or not content.strip():
        raise LLMResponseContractError
    return content


@runtime_checkable
class LLMProvider(Protocol):
    """LLM 供应商协议。

    实现方需保证 complete_json 返回可被 json.loads 解析的 dict；chat 返回非空纯文本。
    缺少 API key 时应在初始化或调用时抛出 RuntimeError，由调用方
    捕获后标记任务失败并释放积分，避免 worker 卡死。
    """

    async def complete_json(self, system: str, user: str) -> dict:
        """以 JSON 模式补全，返回解析后的 dict。

        参数:
            system: 系统提示词（角色与全局约束，必须含 "json" 关键词以兼容百炼/DeepSeek）
            user: 用户提示词（任务上下文与输入数据）
        """
        ...

    async def chat(
        self,
        messages: list[dict],
        *,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """多轮对话补全，返回非空 assistant 文本。

        参数:
            messages: OpenAI 风格消息列表 [{role, content}, ...]，role ∈ user/assistant。
            system: 可选系统提示词；为 None 时不附加 system 消息。
            temperature: 采样温度，None 表示用厂商默认。
            max_tokens: 最大生成 token 数，None 表示用厂商默认。
        """
        ...
