"""画布模块 Pydantic 模式 —— Materials / Canvas / ImageAsset / Delta。

节点 data 字段完整参照 PLAN 第 3.2 节与 04 文档 1.11 节 (imageUpload 节点全字段)。
prompt 自动构造用 06 文档第 5 节固定模板。
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

# ---------------------------------------------------------------------------
# 主体类型 → 中文映射 (06 文档固定模板使用中文主体类型)
# ---------------------------------------------------------------------------
_SUBJECT_TYPE_CN: dict[str, str] = {
    "character": "角色",
    "scene": "场景",
    "background": "场景",  # background 与 scene 同义
    "prop": "道具",
    "sound": "声音",
}

SUBJECT_DESIGN_PROMPT_TEMPLATE_VERSION = 3
SUBJECT_DESIGN_PROMPT_MARKER = "【视觉资产设定提示词】"
CHARACTER_SHEET_MARKER = "【人物设定图构图硬约束 v2】"
LEGACY_CHARACTER_SHEET_MARKERS = ("【人物设定图构图硬约束】",)
CHARACTER_SHEET_CONSTRAINT = (
    f"{CHARACTER_SHEET_MARKER}单张角色设定组合图，严格固定版式：左侧 33% 仅为正面极近面部特写"
    "（extreme facial close-up，头顶留 5% 空间，下颌/颈部截止，肩部不入镜），中性直视镜头，不复制剧情动作。"
    "右侧 67% 严格分为三个等宽、不重叠的全身栏，依次仅为正面 0°、标准侧面 90°、背面 180° 的"
    "正交投影视图（orthographic projection）；同一人物中性 A-Pose，头脚完整，无裁切、遮挡或透视变形。"
    "四视图身份、脸部、发型、服装、配色、体型和稳定配饰完全一致；白色无缝摄影棚背景、均匀柔光。"
    "禁止文字、标签、尺寸线、水印、边框、分栏线、额外人物、额外视图或剧情动作。"
)
_LEGACY_CHARACTER_SHEET_CONSTRAINTS = (
    "【人物设定图构图硬约束】单张组合图，所有视图位于同一张图内。"
    "左侧约 45% 为头肩或半身正面主视觉；右侧约 55% 恰好依次为正面、标准侧面、"
    "背面三幅等比例全身视图。三幅全身视图采用自然 A-Pose 或放松站姿，从头到脚"
    "无遮挡、无裁切。",
)


def _strip_character_sheet_block(prompt: str, marker: str) -> str:
    """删除标记后的旧构图块，直到下一个结构化段落或风格段。

    历史提示词有单行和多行两种形态。不能只删 marker 所在行，否则旧版
    ``左侧 45% / 右侧 55%`` 残句会和新版版式同时传入模型。
    """
    return re.sub(
        rf"{re.escape(marker)}.*?(?=\n【[^】]+】|\Z)",
        "",
        prompt,
        flags=re.DOTALL,
    )


def strip_legacy_character_sheet_constraints(prompt: str) -> str:
    """移除已知旧版角色三视图约束，同时保留其余主体事实与风格段。"""
    normalized = prompt
    for legacy_constraint in _LEGACY_CHARACTER_SHEET_CONSTRAINTS:
        normalized = normalized.replace(legacy_constraint, "")
    for legacy_marker in LEGACY_CHARACTER_SHEET_MARKERS:
        normalized = _strip_character_sheet_block(normalized, legacy_marker)
    return re.sub(r"\n{3,}", "\n\n", normalized).strip()


def ensure_character_sheet_constraint(prompt: str) -> str:
    """规范为唯一且完整的 v2 人物设定图约束。"""
    normalized = strip_legacy_character_sheet_constraints(prompt)
    if (
        normalized.count(CHARACTER_SHEET_MARKER) == 1
        and CHARACTER_SHEET_CONSTRAINT in normalized
    ):
        return normalized

    normalized = normalized.replace(CHARACTER_SHEET_CONSTRAINT, "")
    normalized = _strip_character_sheet_block(normalized, CHARACTER_SHEET_MARKER)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
    return f"{normalized}\n{CHARACTER_SHEET_CONSTRAINT}".strip()


def ensure_subject_prompt_constraints(prompt: str, subject_type: str) -> str:
    """为增强结果补齐不可缺失的主体类型硬约束。"""
    normalized = prompt.strip()
    if subject_type in {"character", "scene", "background", "prop"} and (
        SUBJECT_DESIGN_PROMPT_MARKER not in normalized
    ):
        normalized = f"{SUBJECT_DESIGN_PROMPT_MARKER}\n{normalized}"
    if subject_type == "character":
        return ensure_character_sheet_constraint(normalized)
    return normalized


def _subject_identity(subject_name: str, subject_type: str, subject_description: str) -> tuple[str, str, str]:
    name = subject_name.strip() or "未命名主体"
    type_cn = _SUBJECT_TYPE_CN.get(subject_type, subject_type or "主体")
    description = subject_description.strip() or "未提供额外外观描述，仅依据主体名称做克制设计"
    return name, type_cn, description


def _build_character_prompt(name: str, type_cn: str, description: str) -> str:
    return "\n".join(
        [
            SUBJECT_DESIGN_PROMPT_MARKER,
            "请为以下角色制作一张可直接用于短剧视觉资产库和后续多图一致性生成的人物设定参考图。",
            f"主体名称：{name}",
            f"主体类型：{type_cn}",
            f"主体事实：{description}",
            "【1 身份】严格保留主体事实中的年龄、性别、身份、地域、职业和生活状态；未明确处只做克制补全。",
            "【2 面容】锁定脸型、五官、肤质、皱纹和伤痕等稳定特征；左侧特写中性直视，右侧三视图不得扭头。",
            "【3 发型与服装】落实发型、帽饰、外套、内搭、下装、鞋袜和配饰；互斥方案只选一种。保留颜色、材质、磨损与分层，不把分体服装画成连体服或长袍。",
            "【4 体态与道具】采用符合年龄、职业和经历的真实比例；稳定道具须有真实接触、重力和受力。坐地、咀嚼、说话等剧情动作只作身份线索，不进入设定姿态。",
            "【5 时代】服装、发式、道具和工艺符合对应年代、地域与社会身份，避免跨时代和戏曲化夸张。",
            CHARACTER_SHEET_CONSTRAINT,
            "【7 一致与质感】四视图不得换装、镜像错位或正背面穿帮。整图服从调用方所选风格，清晰呈现皮肤、毛发、布料和道具材质；左侧可用 85mm 人像视角，右侧必须正交投影。",
        ]
    )


def _build_scene_prompt(name: str, type_cn: str, description: str) -> str:
    return "\n".join(
        [
            SUBJECT_DESIGN_PROMPT_MARKER,
            "请为以下场景制作一张可直接用于短剧视觉资产库和后续多镜头一致性生成的场景设定参考图。",
            f"主体名称：{name}",
            f"主体类型：{type_cn}",
            f"主体事实：{description}",
            "【1 空间身份锚定】严格保留描述中的年代、地域、用途、季节、时段与天气；未明确的信息采用克制的中性补全，不擅自加入改变叙事的地点或事件。",
            "【2 空间布局】明确前景、中景、后景、入口、窗户、通道、主要家具和固定设施的相对位置，透视关系可读，空间尺度真实，后续镜头可复现。",
            "【3 材质与使用痕迹】准确呈现墙面、地面、木材、金属、玻璃、织物和自然地貌的材质，以及符合使用年限的磨损、尘土、潮湿、锈蚀或修补痕迹。",
            "【4 时代与地域考据】建筑、陈设、照明、标识形制和生活用品符合主体事实对应的时代与地域，排除跨时代设备、现代化误植和主题乐园式夸张。",
            "【5 场景构图】单张 16:9 横版场景设定图，以完整主视角展示可拍摄空间；机位接近人眼高度，广角透视自然不过度畸变，不使用拼贴式人物三视图。",
            "【6 光线与色彩连续性】主光方向、环境光、色温、天气和时间统一，亮暗层次可辨且保留暗部细节，为后续镜头提供稳定的色彩与照明基准。",
            "【7 物理真实锁】所有物体有明确支撑、接触阴影和合理尺度，禁止悬浮、穿模、重复家具、断裂结构或不可能的透视关系。",
            "【8 资产纯净锁】默认不出现人物、剧情动作、摄影器材和无关车辆；不得出现水印、品牌 LOGO、乱码、字幕、边框或无意义标注。",
            "【9 质感与成像】成像媒介严格服从主体事实或调用方所选风格，整张设定图保持统一；采用电影美术置景级高细节设定品质，使用相当于 24mm 广角视角的自然透视，空间层次清晰、不过度 HDR。",
        ]
    )


def _build_prop_prompt(name: str, type_cn: str, description: str) -> str:
    return "\n".join(
        [
            SUBJECT_DESIGN_PROMPT_MARKER,
            "请为以下道具制作一张可直接用于短剧视觉资产库和后续多图一致性生成的产品设定参考图。",
            f"主体名称：{name}",
            f"主体类型：{type_cn}",
            f"主体事实：{description}",
            "【1 形制与尺度锚定】严格保留描述中的品类、年代、用途、结构和尺寸线索，明确主要部件的比例与连接方式；未明确的信息采用符合用途的克制补全。",
            "【2 材质与配色】准确表现木材、金属、皮革、陶瓷、塑料、纸张或织物等材质，锁定已描述的颜色、反光、纹理和透明度，各视图完全一致。",
            "【3 使用痕迹】磨损、划痕、锈蚀、污渍、折痕、缺口和修补应符合受力与使用逻辑，不随机添加破损，也不把旧物处理成崭新品。",
            "【4 结构与可用性】零件连接、开合关系、握持部位和功能结构真实可用，禁止多余部件、结构粘连、穿模或无法制造的连接。",
            "【5 多视图构图】单张 16:9 横版产品设定图，左侧为占画面主要面积的三分之四主视图，右侧依次展示正面、标准侧面、背面与关键细节；所有视图尺度、材质和损耗位置一致。",
            "【6 物理支撑锁】道具稳定放置在浅色无缝台面或由合理支架支撑，保留自然接触阴影和重力形变，禁止悬浮、透视错位或与背景粘连。",
            "【7 时代考据】工艺、五金、印刷、包装和使用方式符合主体事实对应的年代与地域，避免现代品牌、错误接口和后代式样。",
            "【8 白底布光】三点柔光箱均匀布光，边缘轮廓清楚，无死黑、无强烈投影，微观材质和使用痕迹清晰可辨。",
            "【9 画面纯净锁】不得出现人物、手部、额外同类道具、文字说明、尺寸线、边框、品牌 LOGO、水印、签名或乱码；原物不可避免的文字仅保留形制，不要求生成可读内容。",
            "【10 质感与成像】成像媒介严格服从主体事实或调用方所选风格，整张设定图保持统一；采用产品档案级高细节设定品质，使用相当于 100mm 视角的低畸变透视，色彩中性，结构与微观材质清楚。",
        ]
    )


def build_subject_prompt(
    subject_name: str,
    subject_type: str,
    subject_description: str,
) -> str:
    """按主体类型构造可直接生图的详细设计参考 prompt。"""
    name, type_cn, description = _subject_identity(
        subject_name,
        subject_type,
        subject_description,
    )
    if subject_type == "character":
        return _build_character_prompt(name, type_cn, description)
    if subject_type in {"scene", "background"}:
        return _build_scene_prompt(name, type_cn, description)
    if subject_type == "prop":
        return _build_prop_prompt(name, type_cn, description)
    return "\n".join(
        [
            "以下主体不进入视觉生图流程。",
            f"主体名称：{name}",
            f"主体类型：{type_cn}",
            f"主体描述：{description}",
        ]
    )


# ---------------------------------------------------------------------------
# 出参
# ---------------------------------------------------------------------------
class MaterialsOut(BaseModel):
    """画布项目元数据。"""

    id: str
    name: str
    script_id: str
    version: int = 0
    created_at: str | None = None
    updated_at: str | None = None


class CanvasNodeOut(BaseModel):
    """画布节点 (基于 React Flow)。"""

    id: str
    type: CanvasNodeType
    position: dict[str, Any] | None = None
    data: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_data_for_type(self) -> CanvasNodeOut:
        if self.data is not None:
            self.data = normalize_node_data(self.type, self.data)
        return self


class CanvasEdgeOut(BaseModel):
    """画布连线 (React Flow 边)。"""

    id: str
    source: str
    target: str
    data: dict[str, Any] | None = None


class CanvasStateOut(BaseModel):
    """画布完整状态 {material_id, nodes[], edges[]}。"""

    material_id: str
    nodes: list[CanvasNodeOut] = Field(default_factory=list)
    edges: list[CanvasEdgeOut] = Field(default_factory=list)
    # 当前画布单调版本令牌，客户端下次 delta 必须携带。
    version: int = 0


class ImageAssetOut(BaseModel):
    """图片资产元数据 —— GET /materials/<mid>/image/<image_id>。

    实际图片 URL 由前端拼接 ``settings.media_proxy_prefix + storage_key``。
    """

    image_id: str
    storage_key: str
    mime_type: str
    size_bytes: int
    source: str  # generate_images | upload_images


# ---------------------------------------------------------------------------
# 增量保存入参 (Delta)
# ---------------------------------------------------------------------------
CanvasEntityId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=255),
]


class NodeMoveIn(BaseModel):
    """节点位置移动。"""

    # 与 canvas_nodes.id 的 VARCHAR(255) 保持一致，避免数据库层截断异常。
    id: CanvasEntityId
    position: dict[str, Any]


CanvasNodeType = Literal[
    "imageUpload", "text", "generated", "uploaded", "storyboard", "assets"
]


class CanvasNodeData(BaseModel):
    """节点 data 的共同字段，保留前端尚未建模的扩展字段。"""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    title: str | None = None
    variant: Literal["generated", "uploaded", "text", "storyboard"] | None = None


class ImageOutputData(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    status: str
    asset_id: str | None = Field(default=None, alias="assetId")
    storage_key: str | None = Field(default=None, alias="storageKey")
    width: int | None = Field(default=None, ge=1)
    height: int | None = Field(default=None, ge=1)


class SubjectImageNodeData(CanvasNodeData):
    """imageUpload/generated/uploaded 节点的完整主体设计图数据。"""

    image_url: str | None = Field(default=None, alias="imageUrl")
    prompt: str | None = None
    model: Literal[
        "banana-2",
        "gpt-image-2",
        "wan2.1-t2i-plus",
        "wan2.1-t2i-turbo",
        "wanx-v1",
        "qwen-image",
        "qwen-image-2.0",
    ] | None = None
    size: str | None = None
    aspect: str | None = None
    is_generating: bool | None = Field(default=None, alias="isGenerating")
    generation_job_id: str | None = Field(default=None, alias="generationJobId")
    subject_id: str | None = Field(default=None, alias="subjectId")
    subject_name: str | None = Field(default=None, alias="subjectName")
    subject_type: Literal["character", "scene", "background", "prop", "sound"] | None = Field(
        default=None, alias="subjectType"
    )
    subject_description: str | None = Field(default=None, alias="subjectDescription")
    subject_episode_ids: list[str] | None = Field(default=None, alias="subjectEpisodeIds")
    subject_episode_nos: list[int] | None = Field(default=None, alias="subjectEpisodeNos")
    subject_design_status: str | None = Field(default=None, alias="subjectDesignStatus")
    prompt_variation_index: int | None = Field(default=None, alias="promptVariationIndex")
    images_expanded: bool | None = Field(default=None, alias="imagesExpanded")
    generating_started_at: int | None = Field(default=None, alias="generatingStartedAt")
    asset_id: str | None = Field(default=None, alias="assetId")
    image_outputs: list[ImageOutputData] | None = Field(default=None, alias="imageOutputs")
    primary_image_index: int | None = Field(default=None, alias="primaryImageIndex")


class TextNodeData(CanvasNodeData):
    content: str | None = None


class StoryboardNodeData(CanvasNodeData):
    shot_group_id: str | None = Field(default=None, alias="shotGroupId")
    prompt: str | None = None


def _data_model_for_type(node_type: CanvasNodeType) -> type[CanvasNodeData]:
    if node_type in {"imageUpload", "generated", "uploaded", "assets"}:
        return SubjectImageNodeData
    if node_type == "storyboard":
        return StoryboardNodeData
    return TextNodeData


def normalize_node_data(node_type: CanvasNodeType, data: dict[str, Any]) -> dict[str, Any]:
    """按节点类型校验 data，并以 API 契约的 camelCase 完整保留。

    未建模的前端扩展字段显式采用 ``extra=allow`` 策略；核心字段仍按节点
    类型强制校验，避免不同节点共享一个宽松 JSON 字典。
    """
    model = _data_model_for_type(node_type).model_validate(data)
    if node_type == "text":
        text_model = cast(TextNodeData, model)
        _require_nonempty(text_model.content, "content")
    elif node_type == "storyboard":
        storyboard_model = cast(StoryboardNodeData, model)
        _require_nonempty(storyboard_model.shot_group_id, "shotGroupId")
        _require_nonempty(storyboard_model.prompt, "prompt")
    elif node_type == "generated":
        generated_model = cast(SubjectImageNodeData, model)
        _require_nonempty(generated_model.prompt, "prompt")
        if generated_model.model is None:
            raise ValueError("generated 节点缺少 model")
    elif node_type == "uploaded":
        uploaded_model = cast(SubjectImageNodeData, model)
        if not (uploaded_model.asset_id or uploaded_model.image_url):
            raise ValueError("uploaded 节点缺少 assetId 或 imageUrl")
    elif node_type == "imageUpload":
        image_model = cast(SubjectImageNodeData, model)
        if not image_model.prompt and not (
            image_model.subject_name
            and image_model.subject_type
            and image_model.subject_description is not None
        ):
            raise ValueError(
                "imageUpload 节点必须提供 prompt，或提供完整主体字段以构造 prompt"
            )

    normalized = model.model_dump(
        by_alias=True, exclude_none=False, exclude_unset=True
    )
    # 只保留来访字段，避免模型默认值把未提供字段注入到 round-trip payload。
    return {key: normalized[key] for key in data}


def _require_nonempty(value: str | None, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"节点缺少非空 {field_name}")


def build_prompt_from_node_data(data: dict[str, Any]) -> str:
    """从节点 data 构造主体 prompt，避免调用方拆散三个主体字段。"""
    node = SubjectImageNodeData.model_validate(data)
    if not (
        node.subject_name
        and node.subject_type
        and node.subject_description is not None
    ):
        raise ValueError("主体设计图节点缺少 subjectName/subjectType/subjectDescription")
    return build_subject_prompt(
        node.subject_name,
        node.subject_type,
        node.subject_description,
    )


class NodeUpsertIn(BaseModel):
    """节点新增/更新 (含参数变更)。

    id 已存在则更新，不存在则新增。
    """

    id: CanvasEntityId
    type: CanvasNodeType
    position: dict[str, Any] | None = None
    data: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_data_for_type(self) -> NodeUpsertIn:
        if self.data is None:
            raise ValueError("节点 data 不得为空")
        self.data = normalize_node_data(self.type, self.data)
        return self


class EdgeUpsertIn(BaseModel):
    """边新增/更新。

    id 提供且已存在则更新，否则新增。
    """

    id: CanvasEntityId | None = None
    source: CanvasEntityId
    target: CanvasEntityId
    data: dict[str, Any] | None = None


class CanvasDeltaIn(BaseModel):
    """画布增量保存入参 —— 节点移动/增删/参数变更的 diff。

    POST /materials/<mid>/canvas/delta
    """

    move_nodes: list[NodeMoveIn] = Field(default_factory=list)
    upsert_nodes: list[NodeUpsertIn] = Field(default_factory=list)
    delete_nodes: list[CanvasEntityId] = Field(default_factory=list)
    upsert_edges: list[EdgeUpsertIn] = Field(default_factory=list)
    delete_edges: list[CanvasEntityId] = Field(default_factory=list)
    # 乐观锁：客户端必须携带已知版本，不一致则 409 强制刷新。
    version: int = Field(ge=0)
