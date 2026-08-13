"""分镜边界到关键帧提示的纯函数投影。

边界只描述对应时刻的可见事实；调用方必须显式指定 ``start`` 或 ``end``，
避免把尾边界混进首帧提示，制造第二套相互冲突的画面权威。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from app.schemas.storyboard_contracts import ShotBoundary

BoundaryRole = Literal["start", "end"]


@dataclass(frozen=True)
class BoundaryProjection:
    role: BoundaryRole
    prompt_fragment: str
    snapshot: dict[str, object]


def boundary_snapshot(boundary: ShotBoundary, *, role: BoundaryRole) -> dict[str, object]:
    """生成可持久化的稳定边界快照。"""

    return {
        "boundary_role": role,
        "facts": boundary.model_dump(mode="json", exclude_none=True),
    }


def project_boundary(
    boundary: ShotBoundary | None,
    *,
    role: BoundaryRole,
    subject_names: Mapping[str, str] | None = None,
) -> BoundaryProjection | None:
    """把单一边界投影为关键帧提示片段；空边界返回 ``None``。"""

    if boundary is None:
        return None
    names = subject_names or {}
    parts: list[str] = []
    role_label = "入画状态" if role == "start" else "出画状态"
    if boundary.positions:
        position_parts = []
        for item in boundary.positions:
            name = names.get(item.ref, item.ref)
            facts = [fact for fact in (item.position, item.facing) if fact]
            position_parts.append(f"{name}:{'、'.join(facts) if facts else '位置待定'}")
        parts.append(f"人物位置与朝向={';'.join(position_parts)}")
    if boundary.pose_balance:
        parts.append(f"姿态重心={boundary.pose_balance}")
    if boundary.gaze:
        parts.append(f"视线={boundary.gaze}")
    if boundary.hands is not None:
        hand_parts = []
        if boundary.hands.left:
            hand_parts.append(f"左手:{boundary.hands.left}")
        if boundary.hands.right:
            hand_parts.append(f"右手:{boundary.hands.right}")
        if hand_parts:
            parts.append(f"双手={'；'.join(hand_parts)}")
    if boundary.held_props:
        parts.append(
            "手持道具=" + "、".join(names.get(ref, ref) for ref in boundary.held_props)
        )
    if boundary.visible_state:
        parts.append("可见状态=" + "、".join(boundary.visible_state))
    prompt_fragment = f"【{role_label}】" + ("；".join(parts) if parts else "未提供可见事实")
    return BoundaryProjection(
        role=role,
        prompt_fragment=prompt_fragment,
        snapshot=boundary_snapshot(boundary, role=role),
    )


__all__ = [
    "BoundaryProjection",
    "BoundaryRole",
    "boundary_snapshot",
    "project_boundary",
]
