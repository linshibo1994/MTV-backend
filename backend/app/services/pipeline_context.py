"""请求级 LLM 供应商+模型选择的跨层透传。

API 层在请求作用域内调用 set_llm_selection()；worker 在任务开始前从
任务 detail 恢复选择。split/extract/storyboard/scene_auditions 等深层
服务通过 get_llm_from_context() 读取当前选择并动态构造 LLM 实例，
避免在数千行服务函数签名中逐层追加参数。

ContextVar 在 asyncio task 派生时自动拷贝，天然隔离并发请求与 worker
任务；未设置时回退 get_llm_provider() 单例（服务端默认链）。
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass

from app.providers import LLMProvider, build_llm_with_fallback, get_llm_provider

__all__ = [
    "LLMSelection",
    "current_llm_selection",
    "set_llm_selection",
    "reset_llm_selection",
    "llm_selection_scope",
    "get_llm_from_context",
]


@dataclass(frozen=True)
class LLMSelection:
    """一次请求显式选择的 LLM 供应商与模型；均为空表示走服务端默认。"""

    provider: str | None = None
    model: str | None = None

    def is_default(self) -> bool:
        """未显式选择任何厂商/模型时视为服务端默认。"""
        return not (self.provider or "").strip() and not (self.model or "").strip()


_llm_selection: ContextVar[LLMSelection | None] = ContextVar(
    "llm_selection", default=None
)


def current_llm_selection() -> LLMSelection | None:
    """读取当前上下文的显式 LLM 选择；未设置返回 None。"""
    return _llm_selection.get()


def set_llm_selection(
    selection: LLMSelection | None,
) -> Token[LLMSelection | None]:
    """在当前上下文设置显式 LLM 选择，返回用于恢复的 Token。"""
    return _llm_selection.set(selection)


def reset_llm_selection(token: Token[LLMSelection | None]) -> None:
    """按 Token 恢复上下文，避免泄漏到同请求的其他阶段。"""
    _llm_selection.reset(token)


@contextmanager
def llm_selection_scope(selection: LLMSelection | None):
    """请求/任务作用域的 LLM 选择上下文；离开时自动恢复。"""
    token = set_llm_selection(selection)
    try:
        yield
    finally:
        reset_llm_selection(token)


def get_llm_from_context() -> LLMProvider:
    """按当前上下文选择构造 LLM；无选择时回退服务端默认单例。

    显式选择存在时经 build_llm_with_fallback 包装，模型不可用类 4xx
    自动用同厂商保底模型重试一次。
    """
    selection = _llm_selection.get()
    if selection is None or selection.is_default():
        return get_llm_provider()
    provider = (selection.provider or "").strip()
    if not provider:
        # 只传 model 未传 provider：模型无法脱离厂商路由，保持服务端默认，
        # 避免把未知模型误发给默认厂商产生错误计费。
        return get_llm_provider()
    return build_llm_with_fallback(provider, selection.model)
