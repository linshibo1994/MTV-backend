"""视频提示词适配器的纯函数基础。

本层只做确定性的文本变换，不发起网络请求，也不决定供应商请求体结构。调用方可把
``adapter_version`` 与 ``text`` 的哈希写入任务快照，从而避免能力矩阵更新导致重试漂移。
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass

from app.providers.registry import PromptSyntax, ProviderName, get_prompt_capability

_CONSTRAINT_MARKER = "【约束】"
_BOUNDARY_FACTS_MARKER = "【冻结边界事实 boundary-facts】"
_VARIATION_SELECTORS = range(0xFE00, 0xFE10)
_EMOJI_MODIFIERS = range(0x1F3FB, 0x1F400)
_REGIONAL_INDICATORS = range(0x1F1E6, 0x1F200)


class PromptAdaptationError(ValueError):
    """提示词无法在不破坏受保护约束的情况下完成适配。"""


@dataclass(frozen=True)
class AdaptedPrompt:
    """提示词适配结果及可持久化审计元数据。"""

    text: str
    provider: ProviderName
    model: str | None
    adapter_version: str
    prompt_syntax: PromptSyntax
    max_prompt_characters: int | None
    original_characters: int
    adapted_characters: int
    protected_suffix_characters: int
    truncated: bool


def _is_cluster_extension(character: str) -> bool:
    codepoint = ord(character)
    return (
        bool(unicodedata.combining(character))
        or codepoint in _VARIATION_SELECTORS
        or codepoint in _EMOJI_MODIFIERS
    )


def _unicode_clusters(text: str):
    """生成适合提示词截断的近似 Unicode 字素簇。

    标准库没有完整 UAX #29 分词器；这里覆盖生成提示词常见的组合音标、变体选择符、
    肤色修饰符、ZWJ emoji 和国旗区域码，避免按 UTF-8 字节或半个 emoji 截断。
    """
    cluster = ""
    regional_count = 0
    for character in text:
        codepoint = ord(character)
        is_regional = codepoint in _REGIONAL_INDICATORS
        if not cluster:
            cluster = character
            regional_count = 1 if is_regional else 0
            continue
        if (
            _is_cluster_extension(character)
            or character == "\u200d"
            or cluster.endswith("\u200d")
            or is_regional
            and regional_count == 1
        ):
            cluster += character
            regional_count = regional_count + 1 if is_regional else 0
            continue
        yield cluster
        cluster = character
        regional_count = 1 if is_regional else 0
    if cluster:
        yield cluster


def unicode_safe_prefix(text: str, max_characters: int) -> str:
    """按 Unicode 码点预算返回不拆分常见字素簇的前缀。"""
    if max_characters < 0:
        raise ValueError("max_characters 不能为负数")
    if len(text) <= max_characters:
        return text
    used = 0
    chunks: list[str] = []
    for cluster in _unicode_clusters(text):
        cluster_length = len(cluster)
        if used + cluster_length > max_characters:
            break
        chunks.append(cluster)
        used += cluster_length
    return "".join(chunks)


def split_protected_constraint_tail(prompt: str) -> tuple[str, str]:
    """把冻结边界事实及其后的约束段视为不可截断尾段。

    视频服务会把边界投影紧邻约束锁放置。若仅保护最后的 ``【约束】``，百炼
    等有限长供应商会优先截掉刚写入的起止事实，造成持久化事实与实际提交提示
    脱节。这里选择受控尾段标记中最早出现的位置，同时继续兼容只有约束锁的旧
    提示词。
    """

    indexes = [
        index
        for marker in (_BOUNDARY_FACTS_MARKER, _CONSTRAINT_MARKER)
        if (index := prompt.rfind(marker)) >= 0
    ]
    if not indexes:
        return prompt, ""
    marker_index = min(indexes)
    return prompt[:marker_index].rstrip(), prompt[marker_index:]


def _truncate_preserving_constraint_tail(prompt: str, limit: int) -> tuple[str, int]:
    prefix, protected_suffix = split_protected_constraint_tail(prompt)
    if not protected_suffix:
        return unicode_safe_prefix(prompt, limit).rstrip(), 0
    if len(protected_suffix) > limit:
        raise PromptAdaptationError("受保护的【约束】尾段超过供应商提示词上限")
    if not prefix:
        return protected_suffix, len(protected_suffix)
    separator = "\n"
    prefix_budget = limit - len(protected_suffix) - len(separator)
    if prefix_budget <= 0:
        return protected_suffix, len(protected_suffix)
    truncated_prefix = unicode_safe_prefix(prefix, prefix_budget).rstrip()
    if not truncated_prefix:
        return protected_suffix, len(protected_suffix)
    return f"{truncated_prefix}{separator}{protected_suffix}", len(protected_suffix)


def adapt_prompt(
    prompt: str,
    *,
    provider: str,
    model: str | None = None,
) -> AdaptedPrompt:
    """按 JSON 能力矩阵适配视频提示词，并完整保留末尾约束段。"""
    if not isinstance(prompt, str) or not prompt.strip():
        raise PromptAdaptationError("视频提示词不能为空")
    source = prompt.strip()
    capability = get_prompt_capability(provider, model=model)
    protected_suffix_length = len(split_protected_constraint_tail(source)[1])
    adapted = source
    limit = capability.max_prompt_characters
    if limit is not None and len(source) > limit:
        adapted, protected_suffix_length = _truncate_preserving_constraint_tail(source, limit)
    return AdaptedPrompt(
        text=adapted,
        provider=capability.provider,
        model=model,
        adapter_version=capability.adapter_version,
        prompt_syntax=capability.prompt_syntax,
        max_prompt_characters=limit,
        original_characters=len(source),
        adapted_characters=len(adapted),
        protected_suffix_characters=protected_suffix_length,
        truncated=adapted != source,
    )
