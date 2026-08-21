"""画布模块 Pydantic 模式 —— Materials / Canvas / ImageAsset / Delta。

节点 data 字段完整参照 PLAN 第 3.2 节与 04 文档 1.11 节 (imageUpload 节点全字段)。
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from app.providers.storage import public_url_for_output_key, public_urls_for_payload


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
            self.data = public_urls_for_payload(
                normalize_node_data(self.type, self.data, require_frontend_prompt=False)
            )
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

    ``storage_key`` 用于站内鉴权代理，``public_url`` 用于外部可读取资源。
    """

    image_id: str
    storage_key: str
    public_url: str | None = None
    mime_type: str
    size_bytes: int
    source: str  # generate_images | upload_images

    @model_validator(mode="after")
    def populate_public_url(self) -> ImageAssetOut:
        self.public_url = public_url_for_output_key(self.storage_key)
        return self


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
    public_url: str | None = Field(default=None, alias="publicUrl")
    width: int | None = Field(default=None, ge=1)
    height: int | None = Field(default=None, ge=1)


class SubjectImageNodeData(CanvasNodeData):
    """imageUpload/generated/uploaded 节点的完整主体设计图数据。"""

    image_url: str | None = Field(default=None, alias="imageUrl")
    prompt: str | None = None
    model: Literal[
        "banana-2",
        "gpt-image-2",
        "gp-im-2",
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


def normalize_node_data(
    node_type: CanvasNodeType,
    data: dict[str, Any],
    *,
    require_frontend_prompt: bool = True,
) -> dict[str, Any]:
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
        if require_frontend_prompt:
            _require_nonempty(image_model.prompt, "prompt")

    normalized = model.model_dump(
        by_alias=True, exclude_none=False, exclude_unset=True
    )
    # 只保留来访字段，避免模型默认值把未提供字段注入到 round-trip payload。
    return {key: normalized[key] for key in data}


def _require_nonempty(value: str | None, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"节点缺少非空 {field_name}")


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
