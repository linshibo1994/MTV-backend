"""可复用、无 I/O 的视频提示词结构增强函数。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from app.providers.prompt_adapter import split_protected_constraint_tail

CAMERA_MOVEMENT_VIDEO_PHRASES: dict[str, str] = {
    "固定": "Camera holds steady throughout the shot.",
    "推镜": "Camera dollies forward smoothly at a steady pace.",
    "拉镜": "Camera dollies backward smoothly at a steady pace.",
    "摇镜": "Camera pans smoothly to reveal the scene.",
    "移镜": "Camera tracks laterally with smooth, stable motion.",
    "跟拍": "Camera follows the main subject continuously.",
    "升降": "Camera cranes vertically with smooth acceleration and deceleration.",
    "环绕": "Camera orbits smoothly around the main subject.",
    "手持": "Camera uses controlled handheld movement with natural micro-shake.",
}

_SUBJECT_OVER_DURATION_MARKER = "【主体时长动作 subject-over-duration】"
_BOUNDARY_FACTS_MARKER = "【冻结边界事实 boundary-facts】"


def camera_motion_phrase(camera_movement: str | None) -> str:
    """把受控运镜枚举映射为供应商易理解的前置英文短语。"""
    normalized = (camera_movement or "").strip()
    if not normalized:
        return ""
    try:
        return CAMERA_MOVEMENT_VIDEO_PHRASES[normalized]
    except KeyError as exc:
        raise ValueError(f"不支持的运镜类型: {normalized}") from exc


def prepend_camera_motion(prompt: str, camera_movement: str | None) -> str:
    """把运镜短语固定放到提示词首行；重复调用保持幂等。"""
    source = prompt.strip()
    phrase = camera_motion_phrase(camera_movement)
    if not phrase or source == phrase or source.startswith(f"{phrase}\n"):
        return source
    return f"{phrase}\n{source}" if source else phrase


def _shot_action(shot: Mapping[str, object]) -> str:
    raw_visual = shot.get("visual")
    visual = raw_visual if isinstance(raw_visual, Mapping) else {}
    for value in (shot.get("action"), visual.get("action"), shot.get("content")):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _shot_subject(shot: Mapping[str, object]) -> str:
    raw_characters = shot.get("characters")
    if isinstance(raw_characters, Sequence) and not isinstance(raw_characters, (str, bytes)):
        characters = [item.strip() for item in raw_characters if isinstance(item, str) and item.strip()]
        if characters:
            return "、".join(characters)
    for value in (shot.get("subject"), shot.get("character")):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "画面主体"


def _shot_time(shot: Mapping[str, object], field_name: str) -> int | None:
    value = shot.get(field_name)
    return value if type(value) is int and value >= 0 else None


def build_subject_over_duration(
    shots: Sequence[Mapping[str, object]],
    *,
    duration_seconds: int | None = None,
) -> str:
    """从结构化镜头数据生成主体随时间变化描述，不解析自由文本模板。"""
    if duration_seconds is not None and (type(duration_seconds) is not int or duration_seconds <= 0):
        raise ValueError("duration_seconds 必须是正整数")
    segments: list[tuple[int, int, int, str, str]] = []
    for source_index, shot in enumerate(shots):
        start = _shot_time(shot, "start_seconds")
        end = _shot_time(shot, "end_seconds")
        action = _shot_action(shot)
        if start is None or end is None or end <= start or not action:
            continue
        if duration_seconds is not None and end > duration_seconds:
            raise ValueError("镜头结束时间不能超过镜头组时长")
        display_no = _shot_time(shot, "display_no") or source_index + 1
        segments.append((start, display_no, end, _shot_subject(shot), action))
    if not segments:
        return ""
    segments.sort(key=lambda item: (item[0], item[1], item[2]))
    lines = [
        f"{start}s～{end}s：{subject}：{action}"
        for start, _display_no, end, subject, action in segments
    ]
    return f"{_SUBJECT_OVER_DURATION_MARKER}\n" + "\n".join(lines)


def _insert_before_constraint_tail(prompt: str, block: str) -> str:
    if not block:
        return prompt.strip()
    if block in prompt:
        return prompt.strip()
    prefix, constraint_tail = split_protected_constraint_tail(prompt.strip())
    parts = [part for part in (prefix, block, constraint_tail) if part]
    return "\n".join(parts)


def append_boundary_facts(prompt: str, boundary_fragments: Sequence[str]) -> str:
    """在约束尾段前追加首/尾各自投影，绝不混写另一端冻结事实。"""

    fragments = [item.strip() for item in boundary_fragments if item.strip()]
    if not fragments:
        return prompt.strip()
    block = f"{_BOUNDARY_FACTS_MARKER}\n" + "\n".join(fragments)
    return _insert_before_constraint_tail(prompt, block)


def compose_video_prompt(
    prompt: str,
    *,
    camera_movement: str | None = None,
    shots: Sequence[Mapping[str, object]] = (),
    duration_seconds: int | None = None,
) -> str:
    """组合运镜前置与 subject-over-duration，并让约束段始终留在末尾。"""
    source = prompt.strip()
    subject_block = build_subject_over_duration(shots, duration_seconds=duration_seconds)
    with_subject_duration = _insert_before_constraint_tail(source, subject_block)
    return prepend_camera_motion(with_subject_duration, camera_movement)


__all__ = [
    "CAMERA_MOVEMENT_VIDEO_PHRASES",
    "append_boundary_facts",
    "build_subject_over_duration",
    "camera_motion_phrase",
    "compose_video_prompt",
    "prepend_camera_motion",
]
