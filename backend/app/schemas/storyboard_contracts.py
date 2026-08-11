"""分镜 Provider 响应契约。

``storyboard.v1`` 是已经落库的兼容形态；``storyboard.v2`` 是 Provider 的
结构化单一事实源。v2 不让模型重复生成可由服务端确定性渲染的
``description`` / ``shot_line``，从而减少格式漂移和无效重试。
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

DESCRIPTION_RE = re.compile(
    r"^在【[^】]+】中采用【[^】]+】拍摄视角，"
    r"画面主体【[^】]+】的关键动作是【[^】]+】，"
    r"整体表现的画面是【[^】]+】，"
    r"表达了【[^】]+】，"
    r"整体画面的光影效果是【[^】]+】，"
    r"整个镜头的叙事目的是【[^】]+】。$"
)
SHOT_LINE_RE = re.compile(r"^【镜头(?P<n>\d+):(?P<start>\d+)s～(?P<end>\d+)s】.+")

SHOT_SIZE_VALUES = (
    "极远景",
    "大远景",
    "远景",
    "全景",
    "中全景",
    "中景",
    "中近景",
    "近景",
    "特写",
    "大特写",
    "俯拍特写",
)
COMPOSITION_VALUES = (
    "单人",
    "双人",
    "多人",
    "过肩",
    "对称",
    "三分法",
    "中央构图",
    "框景",
    "前景遮挡",
    "留白",
    "纵深构图",
)
CAMERA_ANGLE_VALUES = (
    "平视",
    "俯拍",
    "仰拍",
    "侧拍",
    "顶拍",
    "斜侧",
    "主观",
)
CAMERA_MOVEMENT_VALUES = (
    "固定",
    "推镜",
    "拉镜",
    "摇镜",
    "移镜",
    "跟拍",
    "升降",
    "环绕",
    "手持",
)

ShotSize = Literal[
    "极远景",
    "大远景",
    "远景",
    "全景",
    "中全景",
    "中景",
    "中近景",
    "近景",
    "特写",
    "大特写",
    "俯拍特写",
]
Composition = Literal[
    "单人",
    "双人",
    "多人",
    "过肩",
    "对称",
    "三分法",
    "中央构图",
    "框景",
    "前景遮挡",
    "留白",
    "纵深构图",
]
CameraAngle = Literal["平视", "俯拍", "仰拍", "侧拍", "顶拍", "斜侧", "主观"]
CameraMovement = Literal[
    "固定",
    "推镜",
    "拉镜",
    "摇镜",
    "移镜",
    "跟拍",
    "升降",
    "环绕",
    "手持",
]

ShortText = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
]
LongText = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=1200),
]
SubjectRef = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        max_length=15,
        pattern=r"^[A-Z]{1,5}\d{2,10}$",
    ),
]


class StrictStoryboardModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class StoryboardSemanticValidationError(ValueError):
    """可安全出站的分镜结构语义错误。

    Pydantic 的 model validator 会把跨字段错误归到根节点；若只保留
    ``value_error/<root>``，修复请求无法判断究竟该修正编号、时码还是时长。
    此异常只保存预定义规则码和静态字段路径，绝不携带模型输出内容。
    """

    def __init__(self, code: str, path: str) -> None:
        self.code = code
        self.path = path
        super().__init__(code)


class StoryboardShotContract(StrictStoryboardModel):
    """已经落库的 v1 镜头兼容形态。"""

    display_no: int = Field(ge=1)
    content: str
    description: str
    shot_line: str
    start_seconds: int = Field(ge=0)
    end_seconds: int = Field(ge=0)
    characters: list[str] = Field(default_factory=list)
    dialogue_character: str = ""
    props: list[str] = Field(default_factory=list)
    sounds: list[str] = Field(default_factory=list)


class StoryboardShotGroupContract(StrictStoryboardModel):
    group_no: str
    narrative_purpose: str
    duration_seconds: int = Field(ge=3, le=15)
    shots: list[StoryboardShotContract] = Field(min_length=1)


class StoryboardSceneContract(StrictStoryboardModel):
    scene_no: int = Field(ge=1)
    background: str
    shot_groups: list[StoryboardShotGroupContract] = Field(min_length=1)


class StoryboardContract(StrictStoryboardModel):
    """服务端现有持久化链路使用的 v1 合约。"""

    schema_version: Literal["storyboard.v1"] = "storyboard.v1"
    scenes: list[StoryboardSceneContract] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_continuity(self) -> StoryboardContract:
        _validate_storyboard_continuity(self.scenes)
        return self


class StoryboardV2VisualContract(StrictStoryboardModel):
    """单镜头摄影设计；这些字段是 Provider 的唯一事实源。"""

    shot_size: ShotSize
    composition: list[Composition] = Field(min_length=1, max_length=3)
    camera_angle: CameraAngle
    camera_movement: CameraMovement
    action: LongText
    frame_description: LongText
    lighting: ShortText
    emotion: ShortText

    @field_validator("composition")
    @classmethod
    def composition_must_be_unique(cls, values: list[Composition]) -> list[Composition]:
        if len(values) != len(set(values)):
            raise ValueError("composition must not contain duplicates")
        return values


class StoryboardV2DialogueContract(StrictStoryboardModel):
    speaker_ref: SubjectRef | None
    text: LongText
    kind: Literal["dialogue", "voice_over", "inner_monologue"]


class StoryboardV2ShotContract(StrictStoryboardModel):
    display_no: int = Field(ge=1)
    start_seconds: int = Field(ge=0)
    end_seconds: int = Field(ge=0)
    visual: StoryboardV2VisualContract
    narrative_purpose: ShortText
    character_refs: list[SubjectRef] = Field(max_length=20)
    prop_refs: list[SubjectRef] = Field(max_length=20)
    sound_refs: list[SubjectRef] = Field(max_length=20)
    sound_design: ShortText
    dialogue: StoryboardV2DialogueContract | None

    @model_validator(mode="after")
    def dialogue_speaker_must_be_visible(self) -> StoryboardV2ShotContract:
        if self.dialogue is None:
            return self
        if self.dialogue.kind in {"dialogue", "inner_monologue"}:
            if self.dialogue.speaker_ref is None:
                raise ValueError("dialogue and inner_monologue require speaker_ref")
            if self.dialogue.speaker_ref not in self.character_refs:
                raise ValueError("dialogue speaker_ref must also appear in character_refs")
        return self


class StoryboardV2ShotGroupContract(StrictStoryboardModel):
    group_no: str
    narrative_purpose: ShortText
    duration_seconds: int = Field(ge=3, le=15)
    # 单镜头组也是合法的 3～15 秒视频单元。保留上限以避免把整场戏塞入
    # 一个生成请求；建议性“多镜头”由 prompt 引导，而不应造成任务失败。
    shots: list[StoryboardV2ShotContract] = Field(min_length=1, max_length=5)


class StoryboardV2SceneContract(StrictStoryboardModel):
    scene_no: int = Field(ge=1)
    background_ref: SubjectRef
    shot_groups: list[StoryboardV2ShotGroupContract] = Field(min_length=1)


class StoryboardV2Contract(StrictStoryboardModel):
    """Provider 输出的结构化 v2 合约。"""

    schema_version: Literal["storyboard.v2"]
    scenes: list[StoryboardV2SceneContract] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_continuity(self) -> StoryboardV2Contract:
        _validate_storyboard_continuity(self.scenes)
        return self


def _validate_storyboard_continuity(scenes: list) -> None:
    """校验 v1/v2 共用的编号、镜头数量和连续整数时码。"""
    for expected_scene_no, scene in enumerate(scenes, start=1):
        if scene.scene_no != expected_scene_no:
            raise StoryboardSemanticValidationError(
                "scene_no_not_consecutive",
                "scenes[].scene_no",
            )
        for expected_group_index, group in enumerate(scene.shot_groups, start=1):
            expected_group_no = f"G{expected_group_index}"
            if group.group_no != expected_group_no:
                raise StoryboardSemanticValidationError(
                    "group_no_not_consecutive",
                    "scenes[].shot_groups[].group_no",
                )
            if not 3 <= group.duration_seconds <= 15:
                raise StoryboardSemanticValidationError(
                    "group_duration_out_of_range",
                    "scenes[].shot_groups[].duration_seconds",
                )
            if not 1 <= len(group.shots) <= 5:
                raise StoryboardSemanticValidationError(
                    "shot_count_out_of_range",
                    "scenes[].shot_groups[].shots",
                )
            cursor = 0
            for expected_display_no, shot in enumerate(group.shots, start=1):
                if shot.display_no != expected_display_no:
                    raise StoryboardSemanticValidationError(
                        "display_no_not_consecutive",
                        "scenes[].shot_groups[].shots[].display_no",
                    )
                if shot.start_seconds != cursor:
                    raise StoryboardSemanticValidationError(
                        "shot_time_not_continuous",
                        "scenes[].shot_groups[].shots[].start_seconds",
                    )
                if shot.end_seconds <= shot.start_seconds:
                    raise StoryboardSemanticValidationError(
                        "shot_end_must_be_greater_than_start",
                        "scenes[].shot_groups[].shots[].end_seconds",
                    )
                cursor = shot.end_seconds
                if isinstance(shot, StoryboardShotContract):
                    if not DESCRIPTION_RE.match(shot.description):
                        raise StoryboardSemanticValidationError(
                            "description_template_mismatch",
                            "scenes[].shot_groups[].shots[].description",
                        )
                    match = SHOT_LINE_RE.match(shot.shot_line)
                    if match is None:
                        raise StoryboardSemanticValidationError(
                            "shot_line_format_invalid",
                            "scenes[].shot_groups[].shots[].shot_line",
                        )
                    if (
                        int(match.group("n")) != shot.display_no
                        or int(match.group("start")) != shot.start_seconds
                        or int(match.group("end")) != shot.end_seconds
                    ):
                        raise StoryboardSemanticValidationError(
                            "shot_line_time_mismatch",
                            "scenes[].shot_groups[].shots[].shot_line",
                        )
            if cursor != group.duration_seconds:
                raise StoryboardSemanticValidationError(
                    "shot_duration_total_mismatch",
                    "scenes[].shot_groups[].duration_seconds",
                )


_DIAGNOSTIC_FIELDS = frozenset(
    {
        "schema_version",
        "scenes",
        "scene_no",
        "background",
        "background_ref",
        "shot_groups",
        "group_no",
        "narrative_purpose",
        "duration_seconds",
        "shots",
        "display_no",
        "content",
        "description",
        "shot_line",
        "start_seconds",
        "end_seconds",
        "characters",
        "character_refs",
        "dialogue_character",
        "dialogue",
        "speaker_ref",
        "text",
        "kind",
        "props",
        "prop_refs",
        "sounds",
        "sound_refs",
        "sound_design",
        "visual",
        "shot_size",
        "composition",
        "camera_angle",
        "camera_movement",
        "action",
        "frame_description",
        "lighting",
        "emotion",
    }
)


def _diagnostic_path(loc: tuple[object, ...]) -> str:
    parts: list[str] = []
    for item in loc:
        if isinstance(item, int):
            if parts:
                parts[-1] = f"{parts[-1]}[]"
            continue
        if isinstance(item, str) and item in _DIAGNOSTIC_FIELDS:
            parts.append(item)
        else:
            parts.append("<unknown_field>")
    return ".".join(parts) or "<root>"


def storyboard_contract_diagnostic(
    schema_name: str,
    exc: ValueError,
) -> dict[str, object]:
    """生成不含输入值和异常原文的安全诊断。"""
    provider_diagnostic = getattr(exc, "diagnostic", None)
    if isinstance(provider_diagnostic, dict) and provider_diagnostic.get("category") in {
        "invalid_json",
        "non_object_json",
    }:
        response = provider_diagnostic.get("response")
        if isinstance(response, dict):
            return {
                "version": 1,
                "category": provider_diagnostic["category"],
                "schema": schema_name,
                "response": {
                    "kind": response.get("kind"),
                    "length_bucket": response.get("length_bucket"),
                    "top_level_kind": response.get("top_level_kind"),
                },
            }
    if isinstance(exc, ValidationError):
        errors = exc.errors(include_url=False)
        semantic_errors: list[StoryboardSemanticValidationError] = []
        for item in errors:
            context = item.get("ctx")
            error = context.get("error") if isinstance(context, dict) else None
            if isinstance(error, StoryboardSemanticValidationError):
                semantic_errors.append(error)
        if semantic_errors:
            return {
                "version": 1,
                "category": "semantic_validation",
                "schema": schema_name,
                "validation": {
                    "error_count": min(len(semantic_errors), 8),
                    "codes": list(
                        dict.fromkeys(error.code for error in semantic_errors)
                    )[:8],
                    "paths": list(
                        dict.fromkeys(error.path for error in semantic_errors)
                    )[:8],
                },
            }
        codes = sorted({str(item.get("type", "validation_error")) for item in errors})[:8]
        paths = list(
            dict.fromkeys(_diagnostic_path(tuple(item.get("loc", ()))) for item in errors)
        )[:8]
        return {
            "version": 1,
            "category": "schema_validation",
            "schema": schema_name,
            "validation": {
                "error_count": min(len(errors), 8),
                "codes": codes,
                "paths": paths,
            },
        }
    return {
        "version": 1,
        "category": "semantic_validation",
        "schema": schema_name,
        "validation": {
            "error_count": 1,
            "codes": ["semantic_validation"],
            "paths": ["<root>"],
        },
    }


class StoryboardContractValidationError(ValueError):
    """携带脱敏诊断的内部合约错误；不得直接出站。"""

    def __init__(self, message: str, *, diagnostic: dict[str, object]) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


def _validation_error(schema_name: str, exc: ValueError) -> StoryboardContractValidationError:
    return StoryboardContractValidationError(
        str(exc),
        diagnostic=storyboard_contract_diagnostic(schema_name, exc),
    )


def validate_storyboard_contract(
    payload: dict,
    subject_catalog: dict[str, str],
) -> StoryboardContract:
    """验证 v1 JSON，并校验引用主体均来自服务端主体清单。"""
    try:
        contract = StoryboardContract.model_validate(payload)
    except ValidationError as exc:
        raise _validation_error("StoryboardContract", exc) from exc

    allowed = set(subject_catalog)

    def require_subject_type(name: str, expected: set[str], field_name: str) -> None:
        if name not in allowed:
            exc = ValueError(f"subject not allowed: {name}")
            raise _validation_error("StoryboardContract", exc) from exc
        actual = subject_catalog[name]
        if actual not in expected:
            expected_label = "/".join(sorted(expected))
            exc = ValueError(
                f"{field_name} subject type mismatch: {name} is {actual}, expected {expected_label}"
            )
            raise _validation_error("StoryboardContract", exc) from exc

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


def validate_storyboard_v2_contract(
    payload: dict,
    subject_reference_types: dict[str, str],
) -> StoryboardV2Contract:
    """验证 v2 JSON 与短引用类型，服务端 UUID 永不交给模型生成。"""
    try:
        contract = StoryboardV2Contract.model_validate(payload)
    except ValidationError as exc:
        raise _validation_error("StoryboardV2Contract", exc) from exc

    def require_reference(ref: str, expected: set[str], field_name: str) -> None:
        actual = subject_reference_types.get(ref)
        if actual is None:
            exc = ValueError(f"subject reference not allowed: {ref}")
            raise _validation_error("StoryboardV2Contract", exc) from exc
        if actual not in expected:
            expected_label = "/".join(sorted(expected))
            exc = ValueError(
                f"{field_name} subject type mismatch: {ref} is {actual}, expected {expected_label}"
            )
            raise _validation_error("StoryboardV2Contract", exc) from exc

    for scene in contract.scenes:
        require_reference(scene.background_ref, {"background", "scene"}, "background_ref")
        for group in scene.shot_groups:
            for shot in group.shots:
                for ref in shot.character_refs:
                    require_reference(ref, {"character"}, "character_refs")
                if shot.dialogue is not None:
                    speaker_ref = shot.dialogue.speaker_ref
                    if speaker_ref is not None:
                        expected_types = (
                            {"character", "sound"}
                            if shot.dialogue.kind == "voice_over"
                            else {"character"}
                        )
                        require_reference(speaker_ref, expected_types, "speaker_ref")
                for ref in shot.prop_refs:
                    require_reference(ref, {"prop"}, "prop_refs")
                for ref in shot.sound_refs:
                    require_reference(ref, {"sound"}, "sound_refs")
    return contract


__all__ = [
    "CAMERA_ANGLE_VALUES",
    "CAMERA_MOVEMENT_VALUES",
    "COMPOSITION_VALUES",
    "DESCRIPTION_RE",
    "SHOT_LINE_RE",
    "SHOT_SIZE_VALUES",
    "StoryboardContract",
    "StoryboardContractValidationError",
    "StoryboardSceneContract",
    "StoryboardShotContract",
    "StoryboardShotGroupContract",
    "StoryboardV2Contract",
    "StoryboardV2DialogueContract",
    "StoryboardV2SceneContract",
    "StoryboardV2ShotContract",
    "StoryboardV2ShotGroupContract",
    "StoryboardV2VisualContract",
    "storyboard_contract_diagnostic",
    "validate_storyboard_contract",
    "validate_storyboard_v2_contract",
]
