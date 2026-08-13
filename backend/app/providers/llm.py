"""LLM 供应商抽象 — Protocol 定义。

支持两类能力：
- complete_json(system, user, *, response_schema=None) -> dict：结构化单轮补全，
  可选地将 JSON Schema 下推给支持该能力的供应商；用于 pipeline（split/extract/
  merge/enrich/parents）。返回可被 json.loads 解析的 dict，由上层 schema 校验器兜底。
- chat(messages, system) -> str：多轮对话，用于新闻播报等对话式生成。

具体实现见 llm_openai_compat.py（覆盖 OpenAI/DeepSeek/百炼/Gemini）与
llm_anthropic.py（Claude）。缺 key 时抛 RuntimeError，由调用方捕获后释放积分。
"""

from __future__ import annotations

import ast
import json
import math
import re
from typing import Protocol, runtime_checkable

from app.providers.registry import ProviderError


class LLMResponseContractError(ProviderError):
    """LLM 响应不满足 Provider 对外契约。"""

    def __init__(self) -> None:
        super().__init__("LLM 供应商返回了无效的 assistant 正文")


class LLMJSONResponseError(ProviderError, ValueError):
    """结构化响应不是可用 JSON 对象，只携带脱敏形态信息。"""

    def __init__(
        self,
        category: str,
        *,
        kind: str,
        content_length: int = 0,
        repair_attempted: bool = False,
    ) -> None:
        super().__init__("LLM 供应商返回了无效的 JSON 正文")
        # 仅供服务端指标使用，不进入对外 diagnostic，避免改变既有错误契约。
        self.repair_attempted = repair_attempted
        if content_length <= 0:
            length_bucket = "0"
        elif content_length <= 128:
            length_bucket = "1_128"
        elif content_length <= 2048:
            length_bucket = "129_2048"
        else:
            length_bucket = "over_2048"
        self.diagnostic = {
            "version": 1,
            "category": category,
            "response": {
                "kind": kind,
                "length_bucket": length_bucket,
                "top_level_kind": "invalid_json" if category == "invalid_json" else kind,
            },
        }


_MARKDOWN_FENCE_RE = re.compile(
    r"^\s*```(?:json|javascript|python)?[ \t]*\r?\n?(.*?)\r?\n?```\s*$",
    re.IGNORECASE | re.DOTALL,
)
_OUTSIDE_STRING_TRANSLATION = {
    "：": ":",
    "，": ",",
    "｛": "{",
    "｝": "}",
    "［": "[",
    "］": "]",
}
_CURLY_QUOTE_PAIRS = {"“": ("\"", "”"), "‘": ("'", "’")}


def _strip_markdown_fence(text: str) -> str:
    """仅剥离完整包裹正文的 Markdown 代码围栏。"""
    candidate = text.strip()
    match = _MARKDOWN_FENCE_RE.fullmatch(candidate)
    return match.group(1).strip() if match is not None else candidate


def _normalize_json_like_punctuation(text: str) -> str:
    """只在字符串外归一化全角标点，并把弯引号识别为字符串边界。"""
    output: list[str] = []
    quote: str | None = None
    curved_closer: str | None = None
    escaped = False
    for char in text:
        if quote is not None:
            if curved_closer is not None and char == curved_closer:
                output.append(quote)
                quote = None
                curved_closer = None
                escaped = False
                continue
            output.append(char)
            if curved_closer is not None:
                continue
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        curved = _CURLY_QUOTE_PAIRS.get(char)
        if curved is not None:
            quote, curved_closer = curved
            output.append(quote)
        elif char in {"\"", "'"}:
            quote = char
            output.append(char)
        else:
            output.append(_OUTSIDE_STRING_TRANSLATION.get(char, char))
    return "".join(output)


def _quote_bare_object_keys(text: str) -> str:
    """为对象中的 ASCII 裸键补双引号，不修改任何字符串内容。"""
    output: list[str] = []
    index = 0
    quote: str | None = None
    escaped = False
    length = len(text)
    while index < length:
        char = text[index]
        output.append(char)
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in {"\"", "'"}:
            quote = char
            index += 1
            continue
        if char not in {"{", ","}:
            index += 1
            continue

        cursor = index + 1
        while cursor < length and text[cursor].isspace():
            output.append(text[cursor])
            cursor += 1
        key_start = cursor
        if cursor < length and (text[cursor].isalpha() or text[cursor] == "_"):
            cursor += 1
            while cursor < length and (text[cursor].isalnum() or text[cursor] in {"_", "-"}):
                cursor += 1
            key = text[key_start:cursor]
            colon = cursor
            while colon < length and text[colon].isspace():
                colon += 1
            if colon < length and text[colon] == ":":
                output.append(json.dumps(key, ensure_ascii=False))
                output.append(text[cursor:colon + 1])
                index = colon + 1
                continue
        output.append(text[key_start:cursor])
        index = cursor
    return "".join(output)


def _remove_trailing_commas(text: str) -> str:
    """移除对象或数组结束符前的尾逗号，不触碰字符串内容。"""
    output: list[str] = []
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(text):
        char = text[index]
        if quote is not None:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in {"\"", "'"}:
            quote = char
            output.append(char)
            index += 1
            continue
        if char == ",":
            cursor = index + 1
            while cursor < len(text) and text[cursor].isspace():
                cursor += 1
            if cursor < len(text) and text[cursor] in {"}", "]"}:
                index += 1
                continue
        output.append(char)
        index += 1
    return "".join(output)


def _repair_json_like(text: str) -> str:
    """对常见 LLM JSON 标点偏差执行确定性、无代码执行的保守修复。"""
    normalized = _normalize_json_like_punctuation(_strip_markdown_fence(text))
    return _remove_trailing_commas(_quote_bare_object_keys(normalized))


def repair_json_like(text: str) -> str:
    """公开的 JSON-like 修复入口；保留私有同名能力供 Provider 内部复用。"""
    return _repair_json_like(text)


def _parse_python_literal(text: str) -> object:
    """仅通过 ``ast.literal_eval`` 解析 Python 字面量，不执行表达式。"""
    return ast.literal_eval(text)


def _parse_python_call_kwargs(text: str) -> dict[str, object]:
    """解析 ``Result(a=1)`` 形态；仅接受关键字字面量，绝不执行调用。"""
    expression = ast.parse(text, mode="eval").body
    if (
        not isinstance(expression, ast.Call)
        or not isinstance(expression.func, ast.Name)
        or expression.args
        or expression.func.id.startswith("_")
    ):
        raise ValueError("不是安全的关键字调用形态")
    result: dict[str, object] = {}
    for keyword in expression.keywords:
        if keyword.arg is None or keyword.arg in result:
            raise ValueError("关键字参数非法或重复")
        result[keyword.arg] = ast.literal_eval(keyword.value)
    return result


def _is_json_value(value: object) -> bool:
    # 迭代遍历避免供应商构造的深层结构触发 Python 递归上限。
    pending = [value]
    while pending:
        current = pending.pop()
        if current is None or isinstance(current, (str, bool, int)):
            continue
        if isinstance(current, float):
            if math.isfinite(current):
                continue
            return False
        if isinstance(current, list):
            pending.extend(current)
            continue
        if isinstance(current, dict):
            if any(not isinstance(key, str) for key in current):
                return False
            pending.extend(current.values())
            continue
        return False
    return True


def _require_json_object(
    value: object,
    *,
    kind: str,
    content_length: int,
    repair_attempted: bool,
) -> dict:
    if not isinstance(value, dict):
        raise LLMJSONResponseError(
            "non_object_json",
            kind=type(value).__name__,
            content_length=content_length,
            repair_attempted=repair_attempted,
        )
    if not _is_json_value(value):
        raise LLMJSONResponseError(
            "invalid_json",
            kind=kind,
            content_length=content_length,
            repair_attempted=repair_attempted,
        )
    return value


def parse_json_object(text: str, *, kind: str = "text") -> dict:
    """把供应商正文解析为 JSON 对象，按安全兜底链逐级收敛。

    顺序为标准 JSON、保守标点修复、Python 字面量、仅关键字调用形态。
    所有 Python 兜底均通过 AST 字面量解析，禁止执行任意表达式。
    """
    candidate = _strip_markdown_fence(text)
    try:
        return _require_json_object(
            json.loads(candidate),
            kind=kind,
            content_length=len(candidate),
            repair_attempted=False,
        )
    except json.JSONDecodeError:
        pass

    repaired = repair_json_like(candidate)
    try:
        return _require_json_object(
            json.loads(repaired),
            kind=kind,
            content_length=len(candidate),
            repair_attempted=True,
        )
    except json.JSONDecodeError:
        pass

    for parser in (_parse_python_literal, _parse_python_call_kwargs):
        try:
            parsed = parser(repaired)
        except (SyntaxError, ValueError, TypeError, MemoryError, RecursionError):
            continue
        return _require_json_object(
            parsed,
            kind=kind,
            content_length=len(candidate),
            repair_attempted=True,
        )
    raise LLMJSONResponseError(
        "invalid_json",
        kind=kind,
        content_length=len(candidate),
        repair_attempted=True,
    ) from None


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

    async def complete_json(
        self,
        system: str,
        user: str,
        *,
        response_schema: dict | None = None,
    ) -> dict:
        """以 JSON 模式补全，返回解析后的 dict。

        参数:
            system: 系统提示词（角色与全局约束，必须含 "json" 关键词以兼容百炼/DeepSeek）
            user: 用户提示词（任务上下文与输入数据）
            response_schema: 可选 JSON Schema。供应商不支持时保持 JSON object 兼容模式，
                由上层继续执行语义校验。
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
