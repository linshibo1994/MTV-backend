"""分镜 Provider 响应契约。

``storyboard.v1`` 是已经落库的兼容形态；``storyboard.v2`` 是 Provider 的
结构化单一事实源。v2 不让模型重复生成可由服务端确定性渲染的
``description`` / ``shot_line``，从而减少格式漂移和无效重试。
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import (
    AliasChoices,
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
CoverageBlockRef = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        max_length=8,
        pattern=r"^B0*[1-9]\d{0,5}$",
    ),
]

POSITION9_GRID_VALUES = (
    "左上",
    "正上",
    "右上",
    "左中",
    "正中",
    "右中",
    "左下",
    "正下",
    "右下",
)
Position9Grid = Literal[
    "左上",
    "正上",
    "右上",
    "左中",
    "正中",
    "右中",
    "左下",
    "正下",
    "右下",
]
FACING_DIRECTION_VALUES = (
    "面朝左",
    "面朝右",
    "面朝镜头",
    "背对镜头",
    "侧身左",
    "侧身右",
)
FacingDirection = Literal[
    "面朝左",
    "面朝右",
    "面朝镜头",
    "背对镜头",
    "侧身左",
    "侧身右",
]
ActionBeat = Literal["trigger", "peak", "aftermath"]
CoverageStatus = Literal["covered", "partial", "repeated"]
AudienceVisibilityStatus = Literal["show_now", "withhold_now"]


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
    # 显式值是动作拍点的唯一 Provider 事实；缺失时服务端按动作与组内位置派生。
    action_beat: ActionBeat | None = None
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


class CharacterPosition(StrictStoryboardModel):
    """角色在单镜画面中的可验证空间位置。"""

    ref: SubjectRef
    position: Position9Grid | None = None
    facing: FacingDirection | None = None


class BoundaryHands(StrictStoryboardModel):
    """边界时刻双手的可见状态；未入画的手保持为空。"""

    left: ShortText | None = None
    right: ShortText | None = None


class ShotBoundary(StrictStoryboardModel):
    """镜头起止时刻的可见事实快照。

    所有字段均可选，允许旧 Provider 继续输出原有 storyboard.v2。边界语义
    不一致由质量规则产生告警，不在 Pydantic 层阻断整集生成。
    """

    positions: list[CharacterPosition] = Field(default_factory=list, max_length=20)
    pose_balance: ShortText | None = None
    gaze: ShortText | None = None
    hands: BoundaryHands | None = None
    held_props: list[SubjectRef] = Field(default_factory=list, max_length=20)
    visible_state: list[ShortText] = Field(default_factory=list, max_length=20)


class AudienceVisibilityItem(StrictStoryboardModel):
    """一个叙事事实在当前镜头对观众的披露权限。"""

    fact: ShortText
    # ``status`` 是新合约的权威字段；``permission`` 仅作为旧响应输入别名。
    status: AudienceVisibilityStatus = Field(
        validation_alias=AliasChoices("status", "permission")
    )
    evidence: list[ShortText] = Field(default_factory=list, max_length=5)
    reveal_trigger: ShortText | None = None
    protection_method: ShortText | None = None
    rationale: ShortText | None = None

    @model_validator(mode="after")
    def withheld_fact_requires_trigger(self) -> AudienceVisibilityItem:
        if self.status == "withhold_now" and self.reveal_trigger is None:
            raise StoryboardSemanticValidationError(
                "withhold_now_requires_reveal_trigger",
                "scenes[].shot_groups[].shots[].audience_visibility[].reveal_trigger",
            )
        return self

    @property
    def permission(self) -> AudienceVisibilityStatus:
        """兼容仍读取旧 Python 属性名的内部调用方。"""

        return self.status


class CoverageSourceRange(StrictStoryboardModel):
    """原文片段内的可选字符范围，采用左闭右开区间。"""

    char_start: int = Field(ge=0)
    char_end: int = Field(ge=1)

    @model_validator(mode="after")
    def end_must_follow_start(self) -> CoverageSourceRange:
        if self.char_end <= self.char_start:
            raise ValueError("char_end must be greater than char_start")
        return self


class CoverageItem(StrictStoryboardModel):
    """一镜对某个原文段的结构化落实记录。"""

    block_ref: CoverageBlockRef
    status: CoverageStatus
    source_range: CoverageSourceRange | None = None
    summary: ShortText | None = None
    repeat_reason: ShortText | None = None


class OmittedSourceBlock(StrictStoryboardModel):
    """未落实到任何镜头的剧本段及其显式省略原因。"""

    block_ref: CoverageBlockRef
    reason: ShortText | None = None


class StoryboardSceneSplitOmittedContract(StrictStoryboardModel):
    """场景划分阶段的显式省略项；该控制面必须给出省略理由。"""

    block_ref: CoverageBlockRef
    reason: ShortText


class StoryboardScenePlanContract(StrictStoryboardModel):
    """LLM 输出的单个场景生成计划，不包含任何镜头细节。"""

    scene_no: int = Field(ge=1)
    background_ref: SubjectRef
    source_block_refs: list[CoverageBlockRef] = Field(min_length=1, max_length=200)
    narrative_purpose: ShortText
    # 单场调用的硬输出预算。上限将单次 LLM 响应控制在可预测范围内。
    estimated_groups: int = Field(ge=1, le=5)


class StoryboardSceneSplitContract(StrictStoryboardModel):
    """按场景拆分的专用控制面合约，不能复用完整 storyboard.v2。"""

    scenes: list[StoryboardScenePlanContract] = Field(min_length=1, max_length=30)
    omitted_source_blocks: list[StoryboardSceneSplitOmittedContract] = Field(
        default_factory=list,
        max_length=200,
    )


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
    source_block_refs: list[CoverageBlockRef] = Field(default_factory=list, max_length=20)
    coverage: list[CoverageItem] = Field(default_factory=list, max_length=20)
    start_boundary: ShotBoundary | None = None
    end_boundary: ShotBoundary | None = None
    character_positions: list[CharacterPosition] = Field(default_factory=list, max_length=20)
    audience_visibility: list[AudienceVisibilityItem] = Field(default_factory=list, max_length=5)

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
    # 保留历史分镜中的续帧意图；不再触发自动首尾帧衔接。
    is_continuation: bool = False
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
    # 省略的原文段没有对应 Shot，因此必须位于根级，不能挂在镜头字段上。
    omitted_source_blocks: list[OmittedSourceBlock] = Field(default_factory=list, max_length=200)

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
        "is_continuation",
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
        "action_beat",
        "frame_description",
        "lighting",
        "emotion",
        "source_block_refs",
        "estimated_groups",
        "coverage",
        "status",
        "source_range",
        "char_start",
        "char_end",
        "summary",
        "repeat_reason",
        "omitted_source_blocks",
        "block_ref",
        "reason",
        "start_boundary",
        "end_boundary",
        "positions",
        "character_positions",
        "ref",
        "position",
        "facing",
        "pose_balance",
        "gaze",
        "hands",
        "left",
        "right",
        "held_props",
        "visible_state",
        "audience_visibility",
        "fact",
        "permission",
        "evidence",
        "reveal_trigger",
        "protection_method",
        "rationale",
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
                for position in shot.character_positions:
                    require_reference(position.ref, {"character"}, "character_positions")
                for boundary_name, boundary in (
                    ("start_boundary", shot.start_boundary),
                    ("end_boundary", shot.end_boundary),
                ):
                    if boundary is None:
                        continue
                    for position in boundary.positions:
                        require_reference(position.ref, {"character"}, f"{boundary_name}.positions")
                    for ref in boundary.held_props:
                        require_reference(ref, {"prop"}, f"{boundary_name}.held_props")
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
    "ActionBeat",
    "AudienceVisibilityItem",
    "AudienceVisibilityStatus",
    "BoundaryHands",
    "CAMERA_ANGLE_VALUES",
    "CAMERA_MOVEMENT_VALUES",
    "CharacterPosition",
    "COMPOSITION_VALUES",
    "CoverageBlockRef",
    "CoverageItem",
    "CoverageSourceRange",
    "CoverageStatus",
    "DESCRIPTION_RE",
    "FacingDirection",
    "FACING_DIRECTION_VALUES",
    "OmittedSourceBlock",
    "Position9Grid",
    "POSITION9_GRID_VALUES",
    "SHOT_LINE_RE",
    "SHOT_SIZE_VALUES",
    "ShotBoundary",
    "StoryboardContract",
    "StoryboardContractValidationError",
    "StoryboardScenePlanContract",
    "StoryboardSceneContract",
    "StoryboardSceneSplitContract",
    "StoryboardSceneSplitOmittedContract",
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
