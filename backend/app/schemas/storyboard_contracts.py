"""storyboard.v1 Provider 响应契约。

该模块只做结构与业务契约验证，不访问数据库或外部服务。
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

DESCRIPTION_RE = re.compile(
    r"^在【[^】]+】中采用【[^】]+】拍摄视角，"
    r"画面主体【[^】]+】的关键动作是【[^】]+】，"
    r"整体表现的画面是【[^】]+】，"
    r"表达了【[^】]+】，"
    r"整体画面的光影效果是【[^】]+】，"
    r"整个镜头的叙事目的是【[^】]+】。$"
)
SHOT_LINE_RE = re.compile(r"^【镜头(?P<n>\d+):(?P<start>\d+)s～(?P<end>\d+)s】.+")


class StoryboardShotContract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    display_no: int
    content: str
    description: str
    shot_line: str
    start_seconds: int
    end_seconds: int
    characters: list[str] = Field(default_factory=list)
    dialogue_character: str = ""
    props: list[str] = Field(default_factory=list)
    sounds: list[str] = Field(default_factory=list)


class StoryboardShotGroupContract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    group_no: str
    narrative_purpose: str
    duration_seconds: int
    shots: list[StoryboardShotContract] = Field(min_length=1)


class StoryboardSceneContract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    scene_no: int
    background: str
    shot_groups: list[StoryboardShotGroupContract] = Field(min_length=1)


class StoryboardContract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["storyboard.v1"] = "storyboard.v1"
    scenes: list[StoryboardSceneContract] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_continuity(self) -> StoryboardContract:
        for expected_scene_no, scene in enumerate(self.scenes, start=1):
            if scene.scene_no != expected_scene_no:
                raise ValueError("scene_no must be consecutive natural numbers")
            for expected_group_index, group in enumerate(scene.shot_groups, start=1):
                expected_group_no = f"G{expected_group_index}"
                if group.group_no != expected_group_no:
                    raise ValueError("group_no must be consecutive natural numbers")
                if not 3 <= group.duration_seconds <= 15:
                    raise ValueError("duration_seconds must be between 3 and 15")
                if not 2 <= len(group.shots) <= 5:
                    raise ValueError("each group must contain 2 to 5 shots")
                cursor = 0
                for expected_display_no, shot in enumerate(group.shots, start=1):
                    if shot.display_no != expected_display_no:
                        raise ValueError("display_no must be consecutive natural numbers")
                    if shot.start_seconds != cursor:
                        raise ValueError("shot time ranges must be continuous")
                    if shot.end_seconds <= shot.start_seconds:
                        raise ValueError("shot end_seconds must be greater than start_seconds")
                    cursor = shot.end_seconds
                    if not DESCRIPTION_RE.match(shot.description):
                        raise ValueError("description must match the fixed template")
                    match = SHOT_LINE_RE.match(shot.shot_line)
                    if match is None:
                        raise ValueError("shot_line must use the segmented time format")
                    if (
                        int(match.group("n")) != shot.display_no
                        or int(match.group("start")) != shot.start_seconds
                        or int(match.group("end")) != shot.end_seconds
                    ):
                        raise ValueError("shot_line time range must match shot fields")
                if cursor != group.duration_seconds:
                    raise ValueError("shot durations must sum to group duration")
        return self


def validate_storyboard_contract(payload: dict, subject_catalog: dict[str, str]) -> StoryboardContract:
    """验证 Provider JSON，并校验引用主体均来自服务端主体清单。"""
    try:
        contract = StoryboardContract.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc

    allowed = set(subject_catalog)

    def require_subject_type(name: str, expected: set[str], field_name: str) -> None:
        if name not in allowed:
            raise ValueError(f"subject not allowed: {name}")
        actual = subject_catalog[name]
        if actual not in expected:
            expected_label = "/".join(sorted(expected))
            raise ValueError(
                f"{field_name} subject type mismatch: {name} is {actual}, expected {expected_label}"
            )

    for scene in contract.scenes:
        require_subject_type(scene.background, {"background", "scene"}, "background")
        for group in scene.shot_groups:
            for shot in group.shots:
                for name in shot.characters:
                    require_subject_type(name, {"character"}, "character")
                if shot.dialogue_character:
                    require_subject_type(shot.dialogue_character, {"character"}, "character")
                for name in shot.props:
                    require_subject_type(name, {"prop"}, "prop")
                for name in shot.sounds:
                    require_subject_type(name, {"sound"}, "sound")
    return contract


__all__ = [
    "StoryboardContract",
    "StoryboardSceneContract",
    "StoryboardShotContract",
    "StoryboardShotGroupContract",
    "validate_storyboard_contract",
]
