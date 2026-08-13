"""主体候选来源证据的持久化边界规范化。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

MAX_EVIDENCE_ITEMS = 100
MAX_EVIDENCE_QUOTE_LENGTH = 2_000


def normalize_candidate_evidence(value: object) -> list[dict[str, object]]:
    """将历史证据收敛为可公开的最小结构。

    旧版本曾允许字符串和任意 JSON 对象直接入库。这里仅保留逐字片段与合法集号，
    未知字段不会进入 API 响应，避免历史内部元数据被意外透出。
    """

    if isinstance(value, str) or isinstance(value, Mapping):
        items: Sequence[object] = [value]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items = value
    else:
        return []

    normalized: list[dict[str, object]] = []
    for item in items[:MAX_EVIDENCE_ITEMS]:
        episode_no: int | None = None
        if isinstance(item, str):
            quote = item.strip()
        elif isinstance(item, Mapping):
            raw_quote = item.get("quote")
            if not isinstance(raw_quote, str):
                continue
            quote = raw_quote.strip()
            raw_episode_no = item.get("episode_no")
            if (
                isinstance(raw_episode_no, int)
                and not isinstance(raw_episode_no, bool)
                and raw_episode_no >= 1
            ):
                episode_no = raw_episode_no
        else:
            continue

        if not quote:
            continue
        record: dict[str, object] = {
            "quote": quote[:MAX_EVIDENCE_QUOTE_LENGTH],
        }
        if episode_no is not None:
            record["episode_no"] = episode_no
        normalized.append(record)
    return normalized
