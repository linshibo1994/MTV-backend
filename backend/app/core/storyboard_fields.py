"""从历史分镜文本中只读提取可展示的摄影字段。

分镜 v1 将摄影信息写在 ``description`` 与 ``shot_line`` 两段文本中。这里仅接受
服务端约定的完整模板；任何一段不符合模板时，只放弃该段能够提供的字段，绝不从自由
文本猜测内容。这样既能兼容历史记录，也不会把用户编辑过的提示词误显示为结构化事实。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_DESCRIPTION_PATTERN = re.compile(
    r"^在【(?P<background>[^】]+)】中采用【(?P<camera>[^】]+)】拍摄视角，"
    r"画面主体【(?P<characters>[^】]+)】的关键动作是【(?P<action>[^】]+)】，"
    r"整体表现的画面是【(?P<frame_description>[^】]+)】，"
    r"表达了【(?P<emotion>[^】]+)】，"
    r"整体画面的光影效果是【(?P<lighting>[^】]+)】，"
    r"整个镜头的叙事目的是【(?P<narrative_purpose>[^】]+)】。$"
)

# v2 可在末尾追加构图、声音设计与台词类型标记。台词捕获使用非贪婪匹配，
# 确保可选标记不被误并入 dialogue_text；v1 的无标记行仍完全兼容。
_SHOT_LINE_PATTERN = re.compile(
    r"^【镜头(?P<display_no>\d+):(?P<start_seconds>\d+)s～(?P<end_seconds>\d+)s】"
    r"场景是(?P<scene>[^，]+)，"
    r"(?P<shot_scale>[^·，]+)·(?P<camera>[^，]+)，"
    r"(?P<camera_movement>[^，]+)，"
    r"(?P<lighting>[^，]+)，"
    r"(?P<action>[^，]+)，"
    r"(?P<emotion>[^，]+)，"
    r"(?P<characters>[^:：，]+)[:：]"
    r"(?P<dialogue_text>.*?)"
    r"(?:，构图是【(?P<composition>[^】]+)】)?"
    r"(?:，声音设计是【(?P<sound_design>[^】]+)】)?"
    r"(?:，台词类型是【(?P<dialogue_kind>[^】]+)】)?$",
    flags=re.DOTALL,
)


def _clean(value: object) -> str | None:
    """仅接受非空字符串，清除模板字段两端的无意义空白。"""

    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _match_description(description: object) -> dict[str, str] | None:
    text = _clean(description)
    if text is None:
        return None
    match = _DESCRIPTION_PATTERN.fullmatch(text)
    if match is None:
        return None
    values = {name: _clean(value) for name, value in match.groupdict().items()}
    if any(value is None for value in values.values()):
        return None
    return {name: value for name, value in values.items() if value is not None}


def _match_shot_line(
    shot_line: object,
    *,
    display_no: int | None,
    start_seconds: int | None,
    end_seconds: int | None,
) -> dict[str, str | None] | None:
    text = _clean(shot_line)
    if text is None:
        return None
    match = _SHOT_LINE_PATTERN.fullmatch(text)
    if match is None:
        return None

    try:
        if display_no is not None and int(match["display_no"]) != display_no:
            return None
        if start_seconds is not None and int(match["start_seconds"]) != start_seconds:
            return None
        if end_seconds is not None and int(match["end_seconds"]) != end_seconds:
            return None
    except ValueError:
        return None

    required_names = (
        "scene",
        "shot_scale",
        "camera",
        "camera_movement",
        "lighting",
        "action",
        "emotion",
        "characters",
    )
    values = {name: _clean(match[name]) for name in required_names}
    if any(value is None for value in values.values()):
        return None
    values["dialogue_text"] = _clean(match["dialogue_text"])
    values["composition"] = _clean(match["composition"])
    values["sound_design"] = _clean(match["sound_design"])
    dialogue_kind = _clean(match["dialogue_kind"])
    values["dialogue_kind"] = (
        dialogue_kind
        if dialogue_kind in {"dialogue", "voice_over", "inner_monologue", "none"}
        else None
    )
    return values


@dataclass(frozen=True)
class StoryboardFields:
    """从既有分镜文本可靠提取出的展示字段。

    ``shot_size``/``shot_scale`` 与 ``camera_angle``/``camera`` 是兼容不同前端
    命名的同义字段；二者均来自同一个已验证的模板片段。
    """

    scene: str | None = None
    background: str | None = None
    shot_size: str | None = None
    shot_scale: str | None = None
    camera_angle: str | None = None
    camera: str | None = None
    camera_movement: str | None = None
    action: str | None = None
    frame_description: str | None = None
    lighting: str | None = None
    emotion: str | None = None
    narrative_purpose: str | None = None
    dialogue_text: str | None = None
    dialogue_kind: str | None = None
    composition: str | None = None
    sound_design: str | None = None

    def model_dump(self) -> dict[str, str | None]:
        """返回可直接传给 Pydantic 出参的稳定字段映射。"""

        return {
            "scene": self.scene,
            "background": self.background,
            "shot_size": self.shot_size,
            "shot_scale": self.shot_scale,
            "camera_angle": self.camera_angle,
            "camera": self.camera,
            "camera_movement": self.camera_movement,
            "action": self.action,
            "frame_description": self.frame_description,
            "lighting": self.lighting,
            "emotion": self.emotion,
            "narrative_purpose": self.narrative_purpose,
            "dialogue_text": self.dialogue_text,
            "dialogue_kind": self.dialogue_kind,
            "composition": self.composition,
            "sound_design": self.sound_design,
        }


def parse_storyboard_fields(
    description: object,
    shot_line: object,
    *,
    display_no: int | None = None,
    start_seconds: int | None = None,
    end_seconds: int | None = None,
) -> StoryboardFields:
    """解析 v1/v2 固定模板，无法验证的字段一律返回 ``None``。

    两段文本独立验证：description 可提供背景、画面描述和叙事目的；shot_line
    可提供景别、运镜、台词和 v2 构图。相同语义字段优先采用 description 的
    明确模板值，description 不可解析时才使用已验证 shot_line 的值。
    """

    description_fields = _match_description(description) or {}
    shot_line_fields = _match_shot_line(
        shot_line,
        display_no=display_no,
        start_seconds=start_seconds,
        end_seconds=end_seconds,
    ) or {}

    background = description_fields.get("background") or shot_line_fields.get("scene")
    scene = shot_line_fields.get("scene") or description_fields.get("background")
    shot_scale = shot_line_fields.get("shot_scale")
    camera = description_fields.get("camera") or shot_line_fields.get("camera")
    action = description_fields.get("action") or shot_line_fields.get("action")
    lighting = description_fields.get("lighting") or shot_line_fields.get("lighting")
    emotion = description_fields.get("emotion") or shot_line_fields.get("emotion")

    return StoryboardFields(
        scene=scene,
        background=background,
        shot_size=shot_scale,
        shot_scale=shot_scale,
        camera_angle=camera,
        camera=camera,
        camera_movement=shot_line_fields.get("camera_movement"),
        action=action,
        frame_description=description_fields.get("frame_description"),
        lighting=lighting,
        emotion=emotion,
        narrative_purpose=description_fields.get("narrative_purpose"),
        dialogue_text=shot_line_fields.get("dialogue_text"),
        dialogue_kind=shot_line_fields.get("dialogue_kind"),
        composition=shot_line_fields.get("composition"),
        sound_design=shot_line_fields.get("sound_design"),
    )


def storyboard_fields_from_shot(shot: Any) -> StoryboardFields:
    """从 ORM Shot 或同字段测试对象构造展示字段。"""

    return parse_storyboard_fields(
        getattr(shot, "description", None),
        getattr(shot, "shot_line", None),
        display_no=getattr(shot, "display_no", None),
        start_seconds=getattr(shot, "start_seconds", None),
        end_seconds=getattr(shot, "end_seconds", None),
    )


__all__ = [
    "StoryboardFields",
    "parse_storyboard_fields",
    "storyboard_fields_from_shot",
]
