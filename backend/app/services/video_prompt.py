"""可复用、无 I/O 的视频执行提示词渲染与结构增强函数。"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from app.providers.prompt_adapter import split_protected_constraint_tail
from app.schemas.storyboard_contracts import StoryboardV2ShotContract

_EMPTY_SOUND_DESIGN_VALUES = frozenset(
    {"", "none", "null", "nil", "n/a", "无", "暂无", "无声", "无声音", "无音效"}
)
_COMPOSITION_DESCRIPTIONS: dict[str, str] = {
    "单人": "镜头聚焦单一人物",
    "双人": "两人同框互动清晰",
    "多人": "多人层次清晰",
    "过肩": "以前景人物肩部取景",
    "对称": "画面对称稳定",
    "三分法": "主体居一侧留出环境空间",
    "中央构图": "主体居中",
    "框景": "以门窗前景形成取景框",
    "前景遮挡": "前景虚化物遮挡画面边缘",
    "留白": "画面保留留白",
    "纵深构图": "画面有前中后景纵深",
}
# 构图模板语的去重关键词。只在画面描述已把同一构图讲清时才丢弃，宁可保留
# 一句略冗余的构图，也不丢掉画面描述并未表达的构图意图：
# ``min_hits`` 要求命中多个景层词，``all_of`` 要求关键词共现。
_COMPOSITION_REDUNDANCY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "纵深构图": ("前景", "中景", "后景", "远景", "纵深"),
    "前景遮挡": ("前景", "遮挡", "虚化"),
    "中央构图": ("画面中央", "居中", "正中"),
    "框景": ("门框", "窗框", "取景框"),
    "留白": ("留白",),
}
_COMPOSITION_REDUNDANCY_MIN_HITS: dict[str, int] = {"纵深构图": 2}
_COMPOSITION_REDUNDANCY_ALL_OF: dict[str, tuple[str, ...]] = {
    "前景遮挡": ("前景",),
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
_FRAGMENT_TAIL_PUNCTUATION = ("。", "，")
_OPENING_QUOTES = ("“", "「", '"')
_CLOSING_QUOTES = ("”", "」", '"')
# 逐镜签名：镜头号 + 景别·机位 + 运镜，止于下一个分句标点。缺摄影字段时退化为镜头号。
_EXECUTION_SHOT_HEADING_RE = re.compile(
    r"^(?P<signature>【镜头\d+\s*:\s*[^】]+】(?:[^，。\n]+·[^，。\n]+，[^，。\n]+)?)"
)
_INTERNAL_SENTENCE_PUNCTUATION = ("。", "！", "？", "!", "?", "…")
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
    if text.endswith(("”", "」", '"')) and len(text) > 1 and text[-2] in _TERMINAL_PUNCTUATION:
        return text
    if text.endswith(_TERMINAL_PUNCTUATION):
        return text
    return f"{text}。"


def _normalize_fragment(value: object) -> str:
    text = _clean_text(value)
    while text.endswith(_FRAGMENT_TAIL_PUNCTUATION):
        text = text[:-1].rstrip()
    return text


def _join_fragments(fragments: Sequence[object]) -> str:
    """把镜头片段压成连续句子，保留片段内部已有的完整句。"""

    sentences: list[str] = []
    buffer = ""
    for value in fragments:
        fragment = _normalize_fragment(value)
        if not fragment:
            continue
        if any(mark in fragment for mark in _INTERNAL_SENTENCE_PUNCTUATION):
            # 只剩镜头号的缓冲区不能单独成句，否则输出 `【镜头1:0s～3s】。`。
            if buffer.endswith("】"):
                sentences.append(_sentence(f"{buffer}{fragment}"))
            else:
                if buffer:
                    sentences.append(_sentence(buffer))
                sentences.append(_sentence(fragment))
            buffer = ""
            continue
        if not buffer:
            buffer = fragment
        elif buffer.endswith("】"):
            # 头部缺摄影字段时只剩镜头号，逗号不能紧跟右括号。
            buffer = f"{buffer}{fragment}"
        else:
            buffer = f"{buffer}，{fragment}"
    if buffer:
        sentences.append(_sentence(buffer))
    return "".join(sentences)


def _shot_time(shot: Mapping[str, object], field_name: str) -> int | None:
    value = shot.get(field_name)
    return value if type(value) is int and value >= 0 else None


def _structured_snapshot(shot: Mapping[str, object]) -> Mapping[str, object]:
    raw = shot.get("structured_data")
    if not isinstance(raw, Mapping) or raw.get("schema_version") != "storyboard.shot-structure.v1":
        return {}
    payload = {key: value for key, value in raw.items() if key not in _STRUCTURED_SNAPSHOT_EXTRA_FIELDS}
    if "dialogue" not in payload:
        payload["dialogue"] = None
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
    return "，".join(dict.fromkeys(descriptions))


def _composition_already_in_frame(value: str, frame_description: str) -> bool:
    keywords = _COMPOSITION_REDUNDANCY_KEYWORDS.get(value, ())
    if not keywords:
        return False
    required = _COMPOSITION_REDUNDANCY_ALL_OF.get(value, ())
    if any(keyword not in frame_description for keyword in required):
        return False
    hits = sum(1 for keyword in keywords if keyword in frame_description)
    return hits >= _COMPOSITION_REDUNDANCY_MIN_HITS.get(value, 1)


def _composition_description(visual: Mapping[str, object]) -> str:
    """构图模板语与画面描述重复时丢弃，避免同一空间事实写两遍。"""

    frame_description = _visual_value(visual, "frame_description")
    values = [
        value
        for value in _string_list(visual.get("composition"))
        if not _composition_already_in_frame(value, frame_description)
    ]
    return _composition_description_from_values(values)


def _props_description(shot: Mapping[str, object], visual: Mapping[str, object]) -> str:
    props = _string_list(shot.get("props"))
    if not props:
        return ""
    visible_text = " ".join(
        value
        for value in (
            _visual_value(visual, "frame_description"),
            _visual_value(visual, "action"),
        )
        if value
    )
    # 画面描述已点名的道具不再复读，只补画面里没被提到的。
    unmentioned = [prop for prop in props if prop not in visible_text]
    if not unmentioned:
        return ""
    return f"画面中保留{'、'.join(unmentioned)}等道具"


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
    """台词自带引号时原样保留，只给裸台词补引号。

    剧本台词必须逐字保留，因此不改写引号内外的标点；只清掉句尾多余的逗号与
    空白，并把仅由引号和标点组成的空台词判为无台词。
    """

    text = text.strip()
    # 只清句尾多余的逗号；句号属于台词原文标点，剥掉即为改写。
    while text.endswith("，"):
        text = text[:-1].rstrip()
    if not text:
        return ""
    if text.startswith(_OPENING_QUOTES):
        if not text.strip("".join((*_OPENING_QUOTES, *_CLOSING_QUOTES))).strip(
            "".join((*_TERMINAL_PUNCTUATION, *_FRAGMENT_TAIL_PUNCTUATION, " "))
        ):
            return ""
        return _sentence(text)
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
    if not quoted:
        return ""
    if kind == "voice_over":
        if quoted.endswith("”。"):
            quoted = quoted[:-1]
        prefix = f"{speaker}" if speaker else ""
        return f"{prefix}画外音：{quoted}，画面中人物不张口"
    if kind == "inner_monologue":
        if quoted.endswith("”。"):
            quoted = quoted[:-1]
        prefix = f"{speaker}" if speaker else ""
        return f"{prefix}内心独白（画外音）：{quoted}，画面中人物不张口"
    return f"{speaker or '人物'}开口说：{quoted}"


def _sound_description(snapshot: Mapping[str, object]) -> str:
    sound_design = _clean_text(snapshot.get("sound_design"))
    if not sound_design or sound_design.casefold() in _EMPTY_SOUND_DESIGN_VALUES:
        return ""
    return f"环境音为{sound_design}"


def _unstructured_snapshot(shot: Mapping[str, object]) -> Mapping[str, object]:
    raw = shot.get("structured_data")
    return raw if isinstance(raw, Mapping) else {}


def _visual_value(visual: Mapping[str, object], field_name: str) -> str:
    return _clean_text(visual.get(field_name))


def _same_content_as_frame(content: str, frame_description: str) -> bool:
    def normalize(value: str) -> str:
        text = value.strip()
        while text.endswith(("。", ".")):
            text = text[:-1].rstrip()
        return text

    return bool(normalize(content)) and normalize(content) == normalize(frame_description)


def _contains_disallowed_execution_metadata(value: str) -> bool:
    return any(marker in value for marker in _DISALLOWED_EXECUTION_METADATA_MARKERS)


def _without_disallowed_execution_metadata(value: str) -> str:
    """旧式字段不进入执行 Prompt；整行丢弃，避免把标签误当真实台词改写。"""

    return "\n".join(
        line for line in value.splitlines() if not _contains_disallowed_execution_metadata(line)
    ).strip()


def _render_shot_line(
    heading: str,
    shot: Mapping[str, object],
    snapshot: Mapping[str, object],
    visual: Mapping[str, object],
    subject_names_by_ref: Mapping[str, str],
    *,
    content: str = "",
    heading_has_details: bool = True,
) -> str:
    fragments: list[object] = [heading]
    for field_name in ("lighting", "frame_description"):
        value = _visual_value(visual, field_name)
        if value:
            fragments.append(value)

    if content and not _contains_disallowed_execution_metadata(content):
        frame_description = _visual_value(visual, "frame_description")
        if not _same_content_as_frame(content, frame_description):
            fragments.append(content)

    props = _props_description(shot, visual)
    if props:
        fragments.append(props)

    action = _visual_value(visual, "action")
    characters = _string_list(shot.get("characters"))
    if action:
        if characters:
            subject = "、".join(characters)
            visible_action = action if action.startswith(subject) else f"{subject}{action}"
        else:
            visible_action = action
        fragments.append(visible_action)

    composition = _composition_description(visual)
    if composition:
        fragments.append(composition)

    if characters:
        emotion = _visual_value(visual, "emotion")
        if emotion:
            fragments.append(f"情绪{emotion}")

    dialogue = _dialogue_description(shot, snapshot, subject_names_by_ref)
    sound = _sound_description(snapshot)
    standalone = [value for value in (dialogue, sound) if value]

    if len(fragments) == 1 and not standalone:
        if heading_has_details:
            return _sentence(heading)
        default_action = "画面主体保持当前动作与空间关系" if characters else "保持当前动作与空间关系"
        return f"{heading}{_sentence(default_action)}"

    line = _join_fragments(fragments)
    return line + "".join(_join_fragments([value]) for value in standalone)


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

        content = _clean_text(shot.get("content"))
        line = _render_shot_line(
            heading,
            shot,
            snapshot,
            visual,
            names_by_ref,
            content=content,
            heading_has_details=bool(shot_size or camera_angle or movement),
        )
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
        lines.append(
            _render_shot_line(
                f"【镜头{display_no}:{start}s～{end}s】{shot_size}·{camera_angle}，{movement}",
                shot,
                snapshot,
                visual,
                names_by_ref,
            )
        )

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
    """提取逐镜的镜头号与摄影抬头，用于识别已经存在的执行块。

    签名只取到运镜描述为止，不含光影、动作与角色名，因此对"逐镜多句"与
    "逐镜逗号连缀单句"两种排版都稳定；否则改排版会让历史落库 Prompt 匹配
    不上，被再追加一份执行块。
    """

    signatures: list[str] = []
    for line in execution_block.splitlines():
        match = _EXECUTION_SHOT_HEADING_RE.match(line)
        if match:
            signatures.append(match.group("signature"))
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
