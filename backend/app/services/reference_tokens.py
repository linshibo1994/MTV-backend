"""稳定参考图 token 映射块的确定性渲染。"""

from __future__ import annotations

from collections.abc import Mapping

REFERENCE_TOKEN_BLOCK_START = "## 图片内容说明"
REFERENCE_TOKEN_BLOCK_END = "## 图片内容说明结束"


def _safe_mapping_text(value: object, *, fallback: str) -> str:
    if not isinstance(value, str):
        return fallback
    normalized = " ".join(value.replace("\r", " ").replace("\n", " ").split())
    normalized = normalized.replace(REFERENCE_TOKEN_BLOCK_START, "图片内容说明")
    normalized = normalized.replace(REFERENCE_TOKEN_BLOCK_END, "图片内容说明结束")
    return normalized[:255] or fallback


def render_reference_token_block(
    token_map: Mapping[str, Mapping[str, object]],
) -> str:
    """渲染不含内部 ID 的 token/短引用/名称/类型映射块。"""
    lines: list[str] = []
    for token, metadata in token_map.items():
        safe_token = _safe_mapping_text(token, fallback="图")
        name = _safe_mapping_text(metadata.get("name"), fallback="参考图")
        short_ref = _safe_mapping_text(metadata.get("ref"), fallback="无")
        subject_type = _safe_mapping_text(
            metadata.get("type") or metadata.get("category"),
            fallback="other",
        )
        lines.append(
            f"{safe_token}: {name}（短引用 {short_ref}；类型 {subject_type}）"
        )
    if not lines:
        return ""
    return "\n".join(
        [REFERENCE_TOKEN_BLOCK_START, *lines, REFERENCE_TOKEN_BLOCK_END]
    )


def remove_reference_token_block(prompt: str) -> str:
    """移除既有映射块，供最终图片顺序变化时重新绑定。"""
    start = prompt.find(REFERENCE_TOKEN_BLOCK_START)
    if start < 0:
        return prompt
    end = prompt.find(REFERENCE_TOKEN_BLOCK_END, start)
    if end < 0:
        return prompt
    end += len(REFERENCE_TOKEN_BLOCK_END)
    prefix = prompt[:start].rstrip()
    suffix = prompt[end:].lstrip()
    return "\n".join(part for part in (prefix, suffix) if part)


def bind_reference_tokens_to_prompt(
    prompt: str,
    token_map: Mapping[str, Mapping[str, object]],
) -> str:
    """把最终映射块置于正文之前；无图片时仅清理陈旧映射块。"""
    cleaned = remove_reference_token_block(prompt).strip()
    block = render_reference_token_block(token_map)
    if not block:
        return cleaned
    return f"{block}\n{cleaned}" if cleaned else block


__all__ = [
    "REFERENCE_TOKEN_BLOCK_END",
    "REFERENCE_TOKEN_BLOCK_START",
    "bind_reference_tokens_to_prompt",
    "remove_reference_token_block",
    "render_reference_token_block",
]
