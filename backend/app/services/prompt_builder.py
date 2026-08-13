"""分镜导演指令的纯规则构造器。

本模块不访问数据库或供应商，只把 storyboard.v2 的结构事实压缩为短小、
可追溯的导演指令。分组被人工重新组合后应重新调用，frame_role 不由 LLM
输出，也不作为跨分组稳定事实持久化。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.schemas.storyboard_contracts import (
    StoryboardV2ShotContract,
    StoryboardV2ShotGroupContract,
)

FrameRole = Literal["single", "first", "key", "last"]
ActionBeat = Literal["trigger", "peak", "aftermath"]

_TRIGGER_KEYWORDS = (
    "开始",
    "突然",
    "推门",
    "起身",
    "抬手",
    "冲入",
    "发现",
    "听见",
    "看见",
    "触发",
)
_PEAK_KEYWORDS = (
    "爆发",
    "击中",
    "抓住",
    "撞",
    "摔",
    "冲突",
    "对峙",
    "揭露",
    "喊",
    "高峰",
)
_AFTERMATH_KEYWORDS = (
    "停下",
    "落地",
    "沉默",
    "喘息",
    "回望",
    "余波",
    "恢复",
    "离开",
    "结束",
    "定格",
)

_CATEGORY_ORDER = (
    "continuity",
    "spatial_continuity",
    "subject_consistency",
    "dialogue_fidelity",
    "coverage",
    "audience_visibility",
)


@dataclass(frozen=True)
class DirectorCommandSummary:
    """一镜的压缩导演指令及可追溯原因。"""

    frame_role: FrameRole
    action_beat: ActionBeat
    must_categories: tuple[str, ...]
    preferred_categories: tuple[str, ...]
    reason_tags: tuple[str, ...]

    def render(self) -> str:
        must = "、".join(self.must_categories) or "none"
        preferred = "、".join(self.preferred_categories) or "none"
        reasons = "、".join(self.reason_tags) or "base"
        return (
            f"【导演指令】镜头职责={self.frame_role}；动作拍点={self.action_beat}；"
            f"必须={must}；优先={preferred}；原因={reasons}"
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "frame_role": self.frame_role,
            "action_beat": self.action_beat,
            "must_categories": list(self.must_categories),
            "preferred_categories": list(self.preferred_categories),
            "reason_tags": list(self.reason_tags),
            "command": self.render(),
        }


def derive_frame_role(display_no: int, total_shots: int) -> FrameRole:
    """按最终组内顺序派生职责；单镜头明确为 ``single``。"""

    if total_shots < 1:
        raise ValueError("total_shots 必须大于 0")
    if display_no < 1 or display_no > total_shots:
        raise ValueError("display_no 必须位于镜头组范围内")
    if total_shots == 1:
        return "single"
    if display_no == 1:
        return "first"
    if display_no == total_shots:
        return "last"
    return "key"


def infer_action_beat(action_text: str, frame_role: FrameRole) -> ActionBeat:
    """由动作关键词推断拍点；无命中时按镜头职责给出稳定兜底。"""

    scores: dict[ActionBeat, int] = {
        "trigger": sum(keyword in action_text for keyword in _TRIGGER_KEYWORDS),
        "peak": sum(keyword in action_text for keyword in _PEAK_KEYWORDS),
        "aftermath": sum(keyword in action_text for keyword in _AFTERMATH_KEYWORDS),
    }
    defaults: dict[FrameRole, ActionBeat] = {
        "single": "peak",
        "first": "trigger",
        "key": "peak",
        "last": "aftermath",
    }
    fallback = defaults[frame_role]
    best_score = max(scores.values())
    if best_score == 0:
        return fallback
    winners = [beat for beat, score in scores.items() if score == best_score]
    return fallback if fallback in winners else winners[0]


def score_frame_guidance(
    shot: StoryboardV2ShotContract,
    *,
    frame_role: FrameRole,
) -> tuple[dict[str, int], tuple[str, ...]]:
    """给当前镜头的质量约束打分，并返回命中原因标签。"""

    scores = {category: 0 for category in _CATEGORY_ORDER}
    reasons: list[str] = []

    if frame_role in {"single", "first"}:
        scores["continuity"] += 3
        reasons.append("group_entry")
    elif frame_role == "key":
        scores["continuity"] += 1
    if frame_role in {"single", "last"}:
        scores["coverage"] += 3
        reasons.append("group_exit")
    if shot.dialogue is not None:
        scores["dialogue_fidelity"] += 4
        reasons.append("dialogue")
    if len(shot.character_refs) >= 2:
        scores["spatial_continuity"] += 4
        scores["subject_consistency"] += 3
        reasons.append("multi_character")
    elif shot.character_refs:
        scores["subject_consistency"] += 1
    if shot.character_positions:
        scores["spatial_continuity"] += 2
        reasons.append("position_facts")
    if shot.start_boundary is not None or shot.end_boundary is not None:
        scores["continuity"] += 2
        reasons.append("boundary_facts")
    if shot.source_block_refs or shot.coverage:
        scores["coverage"] += 2
        reasons.append("source_blocks")
    if shot.audience_visibility:
        scores["audience_visibility"] += 4
        reasons.append("visibility_facts")

    return scores, tuple(dict.fromkeys(reasons))


def build_director_command_summary(
    shot: StoryboardV2ShotContract,
    *,
    total_shots: int,
) -> DirectorCommandSummary:
    """把全量质量要求压缩为一条按风险排序的导演命令。"""

    frame_role = derive_frame_role(shot.display_no, total_shots)
    action_beat = shot.visual.action_beat or infer_action_beat(shot.visual.action, frame_role)
    scores, reasons = score_frame_guidance(shot, frame_role=frame_role)
    if shot.visual.action_beat is not None:
        reasons = (*reasons, "explicit_action_beat")
    must = tuple(category for category in _CATEGORY_ORDER if scores[category] >= 3)
    preferred = tuple(category for category in _CATEGORY_ORDER if 0 < scores[category] < 3)
    return DirectorCommandSummary(
        frame_role=frame_role,
        action_beat=action_beat,
        must_categories=must,
        preferred_categories=preferred,
        reason_tags=reasons,
    )


def build_group_director_summaries(
    group: StoryboardV2ShotGroupContract,
) -> list[DirectorCommandSummary]:
    """按最终组内顺序构造每镜摘要，供生成与 merge 共用。"""

    total_shots = len(group.shots)
    return [
        build_director_command_summary(shot, total_shots=total_shots)
        for shot in group.shots
    ]


__all__ = [
    "ActionBeat",
    "DirectorCommandSummary",
    "FrameRole",
    "build_director_command_summary",
    "build_group_director_summaries",
    "derive_frame_role",
    "infer_action_beat",
    "score_frame_guidance",
]
