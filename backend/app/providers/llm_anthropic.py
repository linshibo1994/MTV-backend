"""Anthropic Claude LLM 供应商实现 — httpx 直连 Messages API。

官方无 OpenAI 兼容端点，必须原生适配：
- 端点 POST {base_url}/v1/messages
- 鉴权头 x-api-key + anthropic-version: 2023-06-01
- system 为请求体顶层字段（非 messages 内消息）
- messages 必须 user/assistant 交替

JSON 结构化输出：用 tool_use 强制。定义通用工具 emit_result，默认
input_schema={"type":"object","additionalProperties":true}；调用方提供 Schema 时将其
作为 input_schema。设 tool_choice={"type":"tool","name":"emit_result"}，模型返回的
ToolUseBlock.input 即为 dict，比提示词约束更可靠。chat 取 text 块拼接。

缺 ANTHROPIC_API_KEY 时抛 RuntimeError，由调用方捕获后释放积分。
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from app.core.config import settings
from app.providers.llm import (
    LLMJSONResponseError,
    LLMResponseContractError,
    require_non_empty_chat_content,
)
from app.providers.registry import ProviderConfigurationError

_API_VERSION = "2023-06-01"
_MAX_TOKENS_DEFAULT = 4096
_SCHEMA_COMPATIBILITY_STATUS_CODES = frozenset({400, 422})

# 强制结构化输出的通用工具：input_schema 放宽为任意 object，
# 由上层 pipeline_contracts 的 Pydantic 严格校验兜底。
_EMIT_RESULT_TOOL: dict[str, Any] = {
    "name": "emit_result",
    "description": "以 JSON 对象形式返回最终结果",
    "input_schema": {
        "type": "object",
        "additionalProperties": True,
    },
}


class _AnthropicHTTPStatusError(RuntimeError):
    """Anthropic 非瞬态 HTTP 错误，只保留安全的状态码。"""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"Claude 调用失败 status={status_code}")


def _build_emit_result_tool(response_schema: dict | None) -> dict[str, Any]:
    """构造本次调用的输出工具，避免修改默认宽松 Schema。"""
    tool = dict(_EMIT_RESULT_TOOL)
    if response_schema is not None:
        tool["input_schema"] = response_schema
    return tool


class AnthropicLLMProvider:
    """Claude Messages API 原生实现（httpx 直连，零额外依赖）。"""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str = "",
    ) -> None:
        if not api_key:
            raise ProviderConfigurationError(
                "CHAT_LLM_API_KEY/TEXT_LLM_API_KEY（或 ANTHROPIC_API_KEY）未配置，"
                "无法初始化 Claude LLM 供应商"
            )
        self._api_key = api_key
        self._model = model
        self._base_url = (base_url or settings.anthropic_base_url).rstrip("/")
        if not self._base_url:
            raise ProviderConfigurationError("Anthropic base_url 未配置")

    def _headers(self) -> dict[str, str]:
        return {
            "x-api-key": self._api_key,
            "anthropic-version": _API_VERSION,
            "content-type": "application/json",
        }

    async def _create(
        self,
        *,
        system: str | None,
        messages: list[dict],
        tools: list[dict] | None = None,
        tool_choice: dict | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": self._model,
            "max_tokens": max_tokens or _MAX_TOKENS_DEFAULT,
            "messages": messages,
        }
        if system:
            body["system"] = system
        if tools:
            body["tools"] = tools
        if tool_choice:
            body["tool_choice"] = tool_choice
        if temperature is not None:
            body["temperature"] = temperature
        try:
            async with httpx.AsyncClient(timeout=120.0) as http:
                resp = await http.post(
                    f"{self._base_url}/v1/messages",
                    headers=self._headers(),
                    json=body,
                )
        except httpx.HTTPError:
            raise ConnectionError("Claude 供应商暂不可用") from None
        if resp.status_code >= 500:
            raise ConnectionError("Claude 供应商暂不可用")
        if resp.status_code == 429:
            raise ConnectionError("Claude 供应商暂不可用")
        if resp.status_code >= 400:
            raise _AnthropicHTTPStatusError(resp.status_code)
        return resp.json()

    async def complete_json(
        self,
        system: str,
        user: str,
        *,
        response_schema: dict | None = None,
    ) -> dict:
        """用 tool_use 强制返回结构化 JSON 对象。

        带 Schema 的工具仅在 400/422 时降级一次到默认宽松工具，避免认证等其他
        4xx 被误判为 Schema 兼容问题。
        """
        messages = [{"role": "user", "content": user}]
        try:
            data = await self._create(
                system=system,
                messages=messages,
                tools=[_build_emit_result_tool(response_schema)],
                tool_choice={"type": "tool", "name": "emit_result"},
            )
        except _AnthropicHTTPStatusError as exc:
            if (
                response_schema is None
                or exc.status_code not in _SCHEMA_COMPATIBILITY_STATUS_CODES
            ):
                raise
            data = await self._create(
                system=system,
                messages=messages,
                tools=[_build_emit_result_tool(None)],
                tool_choice={"type": "tool", "name": "emit_result"},
            )
        for block in data.get("content", []):
            if block.get("type") == "tool_use" and block.get("name") == "emit_result":
                inp = block.get("input")
                if isinstance(inp, dict):
                    return inp
                if isinstance(inp, str):
                    try:
                        parsed = json.loads(inp)
                        if isinstance(parsed, dict):
                            return parsed
                        raise LLMJSONResponseError(
                            "non_object_json", kind=type(parsed).__name__, content_length=len(inp)
                        )
                    except json.JSONDecodeError:
                        raise LLMJSONResponseError(
                            "invalid_json", kind="tool_input", content_length=len(inp)
                        ) from None
        # 退化：模型未调用工具时尝试从 text 块解析 JSON
        for block in data.get("content", []):
            if block.get("type") == "text":
                try:
                    parsed = json.loads(block.get("text", ""))
                    if isinstance(parsed, dict):
                        return parsed
                    raise LLMJSONResponseError(
                        "non_object_json", kind=type(parsed).__name__, content_length=len(block.get("text", ""))
                    )
                except json.JSONDecodeError:
                    continue
        raise LLMJSONResponseError("invalid_json", kind="empty")

    async def chat(
        self,
        messages: list[dict],
        *,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """多轮对话，拼接所有 text 块返回。

        messages 中若夹带 role==system 消息（OpenAI 风格），合并进顶层 system
        参数而非降级为 user（避免破坏 user/assistant 严格交替且保留系统指令语义）。
        """
        normalized, merged_system = _normalize_messages(messages, system)
        data = await self._create(
            system=merged_system,
            messages=normalized,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        texts: list[str] = []
        blocks = data.get("content", [])
        if isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict) or block.get("type") != "text":
                    continue
                text = block.get("text")
                if not isinstance(text, str):
                    raise LLMResponseContractError
                texts.append(text)
        return require_non_empty_chat_content("".join(texts))


def _normalize_messages(
    messages: list[dict], system: str | None
) -> tuple[list[dict], str | None]:
    """规整为 Anthropic 可接受的 messages + 顶层 system。

    - role==system 的消息 content 合并进 system 参数（不进 messages）
    - 仅保留 user/assistant，content 统一为字符串
    - 返回 (messages, system|None)
    """
    sys_parts: list[str] = [system] if system else []
    out: list[dict] = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role == "system":
            if content:
                sys_parts.append(str(content))
            continue
        if role not in ("user", "assistant"):
            role = "user"
        out.append({"role": role, "content": content})
    merged = "\n\n".join(part for part in sys_parts if part) if sys_parts else None
    return out, merged
