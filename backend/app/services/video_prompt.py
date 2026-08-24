"""可复用、无 I/O 的视频执行提示词渲染与结构增强函数。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from app.providers.prompt_adapter import split_protected_constraint_tail
from app.schemas.storyboard_contracts import StoryboardV2ShotContract

_EMPTY_SOUND_DESIGN_VALUES = frozenset(
    {"", "none", "null", "nil", "n/a", "无", "暂无", "无声", "无声音", "无音效"}
)
_COMPOSITION_DESCRIPTIONS: dict[str, str] = {
    "单人": "镜头聚焦单一人物",
    "双人": "两人同处画面，互动关系清晰",
    "多人": "多人在画面中保持清晰层次",
    "过肩": "以前景人物肩部作为取景前景",
    "对称": "画面保持稳定对称",
    "三分法": "主体位于画面一侧，另一侧保留环境空间",
    "中央构图": "主体居于画面中央",
    "框景": "利用门窗或前景形成取景框",
    "前景遮挡": "前景有虚化物体局部遮挡画面边缘",
    "留白": "画面保留安静留白",
    "纵深构图": "前景、中景和背景形成明显纵深",
}
_CAMERA_MOVEMENT_DESCRIPTIONS: dict[str, str] = {
    "固定": "镜头保持固定",
    "推镜": "镜头缓慢向主体推进",
    "拉镜": "镜头平稳向后拉开",
    "摇镜": "镜头平稳摇移展开画面",
    "移镜": "镜头沿主体方向平稳横移",
    "跟拍": "镜头持续跟随主体移动",
    "升降": "镜头平稳升降改变视角",
    "环绕": "镜头围绕主体平稳环绕",
    "手持": "镜头以克制的手持感跟随现场",
}
_EXECUTION_CONSTRAINT_LOCK = (
    "【约束】人物外观、服装、道具和空间关系在镜头间保持连续，面部和肢体自然稳定，"
    "镜头切换平顺，画面清晰稳定。情绪表达与角色状态一致；画内台词的口型、动作和情绪一致，"
    "画外音与内心独白中人物不张口。画面不出现任何可读文字、字幕、Logo 或水印。"
)
_BOUNDARY_FACTS_MARKER = "【冻结边界事实 boundary-facts】"
_TERMINAL_PUNCTUATION = ("。", "！", "？", "!", "?", "…")
_STRUCTURED_SNAPSHOT_EXTRA_FIELDS = frozenset({"schema_version", "subject_ids_by_ref", "background_ref"})
_DISALLOWED_EXECUTION_METADATA_MARKERS = (
    "构图是【",
    "声音设计是【",
    "台词类型是【",
    "【角色】",
    "【叙事目的】",
    "【导演指令】",
)


def _clean_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _string_list(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _sentence(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if text.endswith(_TERMINAL_PUNCTUATION):
        return text
    return f"{text}。"


def _shot_time(shot: Mapping[str, object], field_name: str) -> int | None:
    value = shot.get(field_name)
    return value if type(value) is int and value >= 0 else None


def _structured_snapshot(shot: Mapping[str, object]) -> Mapping[str, object]:
    raw = shot.get("structured_data")
    if not isinstance(raw, Mapping) or raw.get("schema_version") != "storyboard.shot-structure.v1":
        return {}
    payload = {key: value for key, value in raw.items() if key not in _STRUCTURED_SNAPSHOT_EXTRA_FIELDS}
    try:
        StoryboardV2ShotContract.model_validate(payload)
    except ValueError:
        return {}
    return raw


def _composition_description_from_values(values: Sequence[str]) -> str:
    descriptions = [
        _COMPOSITION_DESCRIPTIONS.get(value, f"画面保持{value}的空间组织")
        for value in values
    ]
    return "；".join(dict.fromkeys(descriptions))


def _composition_description(visual: Mapping[str, object]) -> str:
    return _composition_description_from_values(_string_list(visual.get("composition")))


def _props_description(shot: Mapping[str, object]) -> str:
    props = _string_list(shot.get("props"))
    return f"画面中保留{'、'.join(props)}等道具" if props else ""


def _speaker_name(
    shot: Mapping[str, object],
    dialogue: Mapping[str, object],
    subject_names_by_ref: Mapping[str, str],
) -> str:
    speaker_ref = _clean_text(dialogue.get("speaker_ref"))
    if speaker_ref and _clean_text(subject_names_by_ref.get(speaker_ref)):
        return _clean_text(subject_names_by_ref.get(speaker_ref))
    dialogue_character = _clean_text(shot.get("dialogue_character"))
    if dialogue_character:
        return dialogue_character
    characters = _string_list(shot.get("characters"))
    return characters[0] if characters else ""


def _quoted_dialogue(text: str) -> str:
    if text.endswith(_TERMINAL_PUNCTUATION):
        return f"“{text}”"
    return f"“{text}”。"


def _dialogue_description(
    shot: Mapping[str, object],
    snapshot: Mapping[str, object],
    subject_names_by_ref: Mapping[str, str],
) -> str:
    dialogue = _as_mapping(snapshot.get("dialogue"))
    text = _clean_text(dialogue.get("text"))
    if not text:
        return ""
    speaker = _speaker_name(shot, dialogue, subject_names_by_ref)
    kind = _clean_text(dialogue.get("kind"))
    quoted = _quoted_dialogue(text)
    if kind == "voice_over":
        prefix = f"{speaker}的" if speaker else ""
        return f"{prefix}画外音响起，内容为{quoted}；画面中的人物不张口。"
    if kind == "inner_monologue":
        prefix = f"{speaker}的" if speaker else ""
        return f"{prefix}内心独白以画外音呈现，内容为{quoted}；画面中的人物不张口。"
    if speaker:
        return f"{speaker}开口说：{quoted}"
    return f"人物开口说：{quoted}"


def _sound_description(snapshot: Mapping[str, object]) -> str:
    sound_design = _clean_text(snapshot.get("sound_design"))
    if not sound_design or sound_design.casefold() in _EMPTY_SOUND_DESIGN_VALUES:
        return ""
    return f"环境中可听见{sound_design}。"


def _unstructured_snapshot(shot: Mapping[str, object]) -> Mapping[str, object]:
    raw = shot.get("structured_data")
    return raw if isinstance(raw, Mapping) else {}


def _visual_value(visual: Mapping[str, object], field_name: str) -> str:
    return _clean_text(visual.get(field_name))


def _contains_disallowed_execution_metadata(value: str) -> bool:
    return any(marker in value for marker in _DISALLOWED_EXECUTION_METADATA_MARKERS)


def _without_disallowed_execution_metadata(value: str) -> str:
    """旧式字段不进入执行 Prompt；整行丢弃，避免把标签误当真实台词改写。"""

    return "\n".join(
        line for line in value.splitlines() if not _contains_disallowed_execution_metadata(line)
    ).strip()


def render_unstructured_video_execution_prompt(
    shots: Sequence[Mapping[str, object]],
    *,
    background: str | None = None,
    duration_seconds: int | None = None,
    subject_names_by_ref: Mapping[str, str] | None = None,
    include_constraint_lock: bool = True,
) -> str:
    """从非完整结构快照与镜头内容构建新的执行 Prompt，不读取旧式镜头行。"""

    if duration_seconds is not None and (type(duration_seconds) is not int or duration_seconds <= 0):
        raise ValueError("duration_seconds 必须是正整数")

    prepared: list[tuple[int, int, int, Mapping[str, object]]] = []
    for source_index, shot in enumerate(shots):
        start = _shot_time(shot, "start_seconds")
        end = _shot_time(shot, "end_seconds")
        if start is None or end is None or end <= start:
            return ""
        if duration_seconds is not None and end > duration_seconds:
            raise ValueError("镜头结束时间不能超过镜头组时长")
        display_no = _shot_time(shot, "display_no") or source_index + 1
        prepared.append((start, display_no, end, shot))

    if not prepared:
        return ""

    names_by_ref = subject_names_by_ref or {}
    lines: list[str] = []
    for start, display_no, end, shot in sorted(prepared, key=lambda item: item[:3]):
        snapshot = _unstructured_snapshot(shot)
        visual = _as_mapping(snapshot.get("visual"))
        shot_size = _visual_value(visual, "shot_size")
        camera_angle = _visual_value(visual, "camera_angle")
        camera_movement = _visual_value(visual, "camera_movement")
        movement = _CAMERA_MOVEMENT_DESCRIPTIONS.get(
            camera_movement,
            f"镜头以{camera_movement}运动" if camera_movement else "",
        )
        heading = f"【镜头{display_no}:{start}s～{end}s】"
        if shot_size and camera_angle:
            heading += f"{shot_size}·{camera_angle}"
        elif shot_size or camera_angle:
            heading += shot_size or camera_angle
        if movement:
            heading += f"，{movement}"

        parts = [_sentence(heading) if len(heading) > len(f"【镜头{display_no}:{start}s～{end}s】") else heading]
        content = _clean_text(shot.get("content"))
        if content and not _contains_disallowed_execution_metadata(content):
            parts.append(_sentence(content))
        for field_name in ("lighting", "frame_description"):
            sentence = _sentence(visual.get(field_name))
            if sentence:
                parts.append(sentence)

        props = _props_description(shot)
        if props:
            parts.append(_sentence(props))

        action = _visual_value(visual, "action")
        if action:
            characters = _string_list(shot.get("characters"))
            subject = "、".join(characters) if characters else "画面主体"
            visible_action = action if action.startswith(subject) else f"{subject}{action}"
            parts.append(_sentence(visible_action))

        composition = _composition_description(visual)
        if composition:
            parts.append(_sentence(composition))

        emotion = _visual_value(visual, "emotion")
        if emotion:
            parts.append(_sentence(f"人物呈现{emotion}"))
        dialogue = _dialogue_description(shot, snapshot, names_by_ref)
        if dialogue:
            parts.append(dialogue)

        sound = _sound_description(snapshot)
        if sound:
            parts.append(sound)
        line = "".join(parts)
        if len(line) == len(f"【镜头{display_no}:{start}s～{end}s】"):
            line += "画面主体保持当前动作与空间关系。"
        lines.append(line)

    result_parts = [f"故事发生在{_clean_text(background)}。"] if _clean_text(background) else []
    result_parts.append("\n".join(lines))
    if include_constraint_lock:
        result_parts.append(_EXECUTION_CONSTRAINT_LOCK)
    return _without_disallowed_execution_metadata("\n".join(result_parts))


def render_video_execution_prompt(
    shots: Sequence[Mapping[str, object]],
    *,
    background: str | None = None,
    duration_seconds: int | None = None,
    subject_names_by_ref: Mapping[str, str] | None = None,
    include_constraint_lock: bool = True,
) -> str:
    """将结构化镜头渲染为逐时段、自然语言化的视频执行提示词。

    渲染只消费完整 storyboard.v2 的可验证事实，不读取旧式 ``shot_line``。任何
    镜头不是完整 v2 快照时返回空串，由无结构回退渲染器基于结构字段与 content 重建。
    """

    if duration_seconds is not None and (type(duration_seconds) is not int or duration_seconds <= 0):
        raise ValueError("duration_seconds 必须是正整数")

    prepared: list[tuple[int, int, int, Mapping[str, object], Mapping[str, object]]] = []
    for source_index, shot in enumerate(shots):
        start = _shot_time(shot, "start_seconds")
        end = _shot_time(shot, "end_seconds")
        snapshot = _structured_snapshot(shot)
        visual = _as_mapping(snapshot.get("visual"))
        shot_size = _clean_text(visual.get("shot_size"))
        camera_angle = _clean_text(visual.get("camera_angle"))
        camera_movement = _clean_text(visual.get("camera_movement"))
        action = _clean_text(visual.get("action"))
        if (
            start is None
            or end is None
            or end <= start
            or not shot_size
            or not camera_angle
            or not camera_movement
            or not action
        ):
            return ""
        if duration_seconds is not None and end > duration_seconds:
            raise ValueError("镜头结束时间不能超过镜头组时长")
        display_no = _shot_time(shot, "display_no") or source_index + 1
        prepared.append((start, display_no, end, shot, snapshot))

    if not prepared:
        return ""

    names_by_ref = subject_names_by_ref or {}
    lines: list[str] = []
    for start, display_no, end, shot, snapshot in sorted(prepared, key=lambda item: item[:3]):
        visual = _as_mapping(snapshot.get("visual"))
        shot_size = _clean_text(visual.get("shot_size"))
        camera_angle = _clean_text(visual.get("camera_angle"))
        camera_movement = _clean_text(visual.get("camera_movement"))
        movement = _CAMERA_MOVEMENT_DESCRIPTIONS.get(
            camera_movement,
            f"镜头以{camera_movement}运动",
        )
        parts = [f"【镜头{display_no}:{start}s～{end}s】{shot_size}·{camera_angle}，{movement}。"]
        for value in (visual.get("lighting"), visual.get("frame_description")):
            sentence = _sentence(value)
            if sentence:
                parts.append(sentence)

        props = _props_description(shot)
        if props:
            parts.append(_sentence(props))

        action = _clean_text(visual.get("action"))
        characters = _string_list(shot.get("characters"))
        subject = "、".join(characters) if characters else "画面主体"
        visible_action = action if action.startswith(subject) else f"{subject}{action}"
        parts.append(_sentence(visible_action))

        composition = _composition_description(visual)
        if composition:
            parts.append(_sentence(composition))
        emotion = _clean_text(visual.get("emotion"))
        if emotion:
            parts.append(_sentence(f"人物呈现{emotion}"))
        dialogue = _dialogue_description(shot, snapshot, names_by_ref)
        if dialogue:
            parts.append(dialogue)
        sound = _sound_description(snapshot)
        if sound:
            parts.append(sound)
        lines.append("".join(parts))

    parts = [f"故事发生在{_clean_text(background)}。"] if _clean_text(background) else []
    parts.append("\n".join(lines))
    if include_constraint_lock:
        parts.append(_EXECUTION_CONSTRAINT_LOCK)
    return _without_disallowed_execution_metadata("\n".join(parts))


def _insert_before_constraint_tail(prompt: str, block: str) -> str:
    if not block:
        return prompt.strip()
    if block in prompt:
        return prompt.strip()
    prefix, constraint_tail = split_protected_constraint_tail(prompt.strip())
    parts = [part for part in (prefix, block, constraint_tail) if part]
    return "\n".join(parts)


def _execution_shot_signatures(execution_block: str) -> list[str]:
    """提取不依赖角色名称的逐镜摄影首句，用于识别已经存在的执行块。"""

    signatures: list[str] = []
    for line in execution_block.splitlines():
        if not line.startswith("【镜头"):
            continue
        first_sentence, separator, _ = line.partition("。")
        if separator:
            signatures.append(f"{first_sentence}{separator}")
    return signatures


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
    shots: Sequence[Mapping[str, object]] = (),
    duration_seconds: int | None = None,
) -> str:
    """将镜头事实补入基础提示词；旧式组提示词会被新的执行 Prompt 整体替换。"""

    source = prompt.strip()
    source_has_disallowed_metadata = _contains_disallowed_execution_metadata(source)
    execution_block = render_video_execution_prompt(
        shots,
        duration_seconds=duration_seconds,
        include_constraint_lock=False,
    )
    if not execution_block:
        execution_block = render_unstructured_video_execution_prompt(
            shots,
            duration_seconds=duration_seconds,
            include_constraint_lock=False,
        )
    if not execution_block:
        return _without_disallowed_execution_metadata(source) if source_has_disallowed_metadata else source
    if source_has_disallowed_metadata:
        _, constraint_tail = split_protected_constraint_tail(source)
        constraint = constraint_tail or _EXECUTION_CONSTRAINT_LOCK
        return "\n".join((execution_block, constraint))
    has_constraint_tail = bool(split_protected_constraint_tail(source)[1])
    signatures = _execution_shot_signatures(execution_block)
    if signatures and all(signature in source for signature in signatures):
        if has_constraint_tail:
            return source
        return _insert_before_constraint_tail(source, _EXECUTION_CONSTRAINT_LOCK)
    if execution_block in source:
        return source
    block = execution_block if has_constraint_tail else f"{execution_block}\n{_EXECUTION_CONSTRAINT_LOCK}"
    return _insert_before_constraint_tail(source, block)


__all__ = [
    "append_boundary_facts",
    "compose_video_prompt",
    "render_unstructured_video_execution_prompt",
    "render_video_execution_prompt",
]
