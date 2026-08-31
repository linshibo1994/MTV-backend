"""主体列表的稳定排序规则。

场景主体的名称通常来自剧本原文，名称前缀可能包含场景编号（例如
``场景 7：雨夜码头``）。数据库无法把这类自由文本安全地按数字排序，
因此在返回 API 前仅对场景主体做一次确定性的数字排序。
"""

from __future__ import annotations

import re
from collections.abc import Sequence

_SCENE_NAME_RE = re.compile(
    r"^\s*(?:场景|scene)\s*(?:(?:编号)\s*|[#＃:：\-－]\s*)?(\d+)(?=\D|$)",
    re.IGNORECASE,
)

def scene_number_from_name(name: object) -> int | None:
    """从场景主体名称开头提取阿拉伯数字编号。"""

    if not isinstance(name, str):
        return None
    match = _SCENE_NAME_RE.match(name)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError, OverflowError):
        return None


def sort_scene_subjects[T](subjects: Sequence[T]) -> list[T]:
    """保持其他主体原有位置，只按数字重排场景主体。

    未编号场景排在已编号场景之后；排序使用 Python 的稳定排序，编号相同
    或都未编号时保留数据库原始顺序。这样不会改变角色、道具和声音的
    既有分组顺序。
    """

    ordered = list(subjects)
    scene_positions = [
        index
        for index, subject in enumerate(ordered)
        if str(getattr(subject, "type", "")).strip().lower()
        in {"scene", "background"}
    ]
    if len(scene_positions) < 2:
        return ordered

    scene_items = [ordered[index] for index in scene_positions]
    scene_items.sort(
        key=lambda subject: (
            (number := scene_number_from_name(getattr(subject, "name", None))) is None,
            number if number is not None else 0,
        )
    )
    for index, subject in zip(scene_positions, scene_items, strict=True):
        ordered[index] = subject
    return ordered
