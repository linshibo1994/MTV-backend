"""资产库模块 Pydantic v2 入参/出参 schema。

background 类型在 schema 层统一 label 为 "场景"（scene 与 background 同义）。
"""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.providers.storage import public_url_for_output_key

# ---- 枚举常量 ----

# 主体类型：character=角色, background/scene=场景, prop=道具, sound=声音
SUBJECT_TYPE_LABELS: dict[str, str] = {
    "character": "角色",
    "background": "场景",
    "scene": "场景",
    "prop": "道具",
    "sound": "声音",
}

# 审核状态标签
# superseded 用于 AssetEntry 维度（旧正式图片被新版本替换），
# Subject.review_status 仍只取 not_designed/pending/approved 三态。
REVIEW_STATUS_LABELS: dict[str, str] = {
    "not_designed": "未通过",
    "approved": "已通过",
    "pending": "未审核",
    "superseded": "已替换",
}


def normalize_subject_type(raw: str | None) -> str:
    """scene 与 background 同义，资产库统一输出 background。"""
    if raw == "scene":
        return "background"
    return raw or "background"


def subject_type_label(subject_type: str) -> str:
    """主体类型 → 中文标签。"""
    return SUBJECT_TYPE_LABELS.get(subject_type, subject_type)


def review_status_label(review_status: str) -> str:
    """审核状态 → 中文标签。"""
    return REVIEW_STATUS_LABELS.get(review_status, review_status)


# ---- 资产条目 ----


class AssetEntryOut(BaseModel):
    """资产条目出参 — image/audio 的对象存储记录。"""

    model_config = ConfigDict(from_attributes=True)

    asset_id: str
    storage_key: str
    public_url: str | None = None
    mime_type: str
    size_bytes: int
    source: str
    sha256: str | None = None
    kind: str  # image | audio
    status: str = "candidate"
    is_approved: bool = False
    # 旧正式图片被新版本替换后置 True（AssetEntry 维度的 superseded 标记）
    superseded: bool = False
    superseded_by_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @model_validator(mode="after")
    def populate_public_url(self) -> "AssetEntryOut":
        self.public_url = public_url_for_output_key(self.storage_key)
        return self


class CandidateOut(BaseModel):
    """审核候选出参 — 未通过审核的图片/声音候选。"""

    model_config = ConfigDict(from_attributes=True)

    candidate_id: str
    storage_key: str
    public_url: str | None = None
    mime_type: str
    size_bytes: int
    source: str
    sha256: str | None = None
    kind: str  # image | audio
    status: str = "pending"

    @model_validator(mode="after")
    def populate_public_url(self) -> "CandidateOut":
        self.public_url = public_url_for_output_key(self.storage_key)
        return self


# ---- 主体 ----


class SubjectAssetOut(BaseModel):
    """资产库主体出参 — 资产库详情页表格每行。"""

    model_config = ConfigDict(from_attributes=True)

    subject_id: str
    subject_name: str
    subject_type: str  # 统一为 background（scene 归一）
    subject_type_label: str  # 中文标签，background -> "场景"
    subject_description: str | None = None
    name_locked: bool = False
    description_locked: bool = False
    subject_version: int = 1
    # 出现的集（契约 02§3.2.3 / 04§1.7 三字段）：
    # episode_nos 为集号(int)，episodes/episode_ids 为兼容层冗余字段(str)。
    episodes: list[str] = Field(default_factory=list)
    episode_ids: list[str] = Field(default_factory=list)
    episode_nos: list[int] = Field(default_factory=list)
    parent_id: str | None = None
    parent_name: str | None = None
    is_parent: bool = False
    review_status: str
    review_status_label: str
    pending_candidate_count: int = 0
    image_entry: AssetEntryOut | None = None
    audio_entries: list[AssetEntryOut] = Field(default_factory=list)
    whitelist_status: str | None = None
    # T10.3 资产库列表封面 —— SVG 确定性占位图 (data:image/svg+xml 内联，零存储)
    cover_url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ParentSubjectOut(BaseModel):
    """父主体出参 — 同类子主体的跨集去重聚合。"""

    model_config = ConfigDict(from_attributes=True)

    subject_id: str
    name: str
    type: str  # character | background | prop | sound
    description: str | None = None
    child_count: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


# ---- 入参 ----

SubjectEpisodeNo = Annotated[int, Field(strict=True, gt=0)]


class SubjectCreateIn(BaseModel):
    """手动新增主体入参。

    手动主体必须具备可用于后续生成的完整描述，并且至少归属一个已有分集。
    scene/background 在 API 层均可接受，存储时由 CRUD 统一归一为 scene。
    """

    type: Literal["character", "background", "scene", "prop", "sound"]
    name: str = Field(min_length=1, max_length=255)
    description: str = Field(min_length=1, max_length=10_000)
    episode_nos: list[SubjectEpisodeNo] = Field(
        min_length=1,
        max_length=1_000,
    )

    @field_validator("name", "description")
    @classmethod
    def strip_non_empty_text(cls, value: str) -> str:
        """去除用户输入的首尾空白，避免产生伪不同名主体。"""
        value = value.strip()
        if not value:
            raise ValueError("不得为空")
        return value

    @model_validator(mode="after")
    def episode_numbers_are_unique(self) -> "SubjectCreateIn":
        """所属集号不允许重复，并固定按集号升序存储。"""
        if len(self.episode_nos) != len(set(self.episode_nos)):
            raise ValueError("episode_nos 不得重复")
        self.episode_nos = sorted(self.episode_nos)
        return self


class SubjectUpdateIn(BaseModel):
    """编辑主体入参；人工修改名称/描述时服务端默认锁定对应字段。"""

    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, min_length=1, max_length=10_000)
    # character | background | scene | prop | sound（scene 与 background 同义，
    # normalize_subject_type 会将 scene 归一为 background；Literal 拒绝非法枚举值）
    type: Literal["character", "background", "scene", "prop", "sound"] | None = None
    episode_nos: list[SubjectEpisodeNo] | None = Field(
        default=None,
        min_length=1,
        max_length=1_000,
    )
    name_locked: bool | None = None
    description_locked: bool | None = None
    expected_subject_version: int | None = Field(default=None, ge=1)

    @field_validator("name", "description")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("不得为空")
        return value

    @model_validator(mode="after")
    def episode_numbers_are_unique(self) -> "SubjectUpdateIn":
        """PATCH 与 POST 使用相同的正整数、唯一和排序规则。"""
        if self.episode_nos is None:
            return self
        if len(self.episode_nos) != len(set(self.episode_nos)):
            raise ValueError("episode_nos 不得重复")
        self.episode_nos = sorted(self.episode_nos)
        return self


class SubjectSuggestionResolveIn(BaseModel):
    """接受或忽略一条待处理差异建议。"""

    action: Literal["accept", "ignore"]
    expected_subject_version: int = Field(ge=1)


class SubjectChangeSuggestionOut(BaseModel):
    """流水线对人工锁字段提出的非破坏性差异建议。"""

    model_config = ConfigDict(from_attributes=True)

    suggestion_id: str = Field(validation_alias="id")
    subject_id: str
    field: Literal["name", "description"]
    current_value: str | None = None
    suggested_value: str | None = None
    base_subject_version: int
    source_run_id: str | None = None
    status: Literal["pending", "accepted", "ignored"]
    resolved_by_user_id: str | None = None
    resolved_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class SubjectChangeSuggestionListOut(BaseModel):
    items: list[SubjectChangeSuggestionOut]
    total: int


class ApproveImageIn(BaseModel):
    """通过图片入参 — 指定候选 ID 设为 image_entry。

    expected_image_entry_id: 客户端当前看到的正式图片 ID（乐观锁信号）。
        若服务端加锁后的 image_entry_id 与之不符，说明另一审核者已先通过
        新正式资产，返回 409 冲突，避免覆盖（AC-AL04）。可不传（向后兼容）。
    confirm: 替换被镜头引用的正式资产时的二次确认。
        confirm=False（默认）且替换目标被引用时，仅返回影响摘要不执行替换（dry-run）；
        confirm=True 时执行替换（AC-AL05）。
    """

    candidate_id: str
    expected_image_entry_id: str | None = None
    confirm: bool = False


class ApproveAudioIn(BaseModel):
    """通过声音入参 — 指定候选 ID 挂载为 audio_entries。

    设计差异（02§3.3.5 未要求音频替换影响摘要）：与 approve-image 不同，
    本操作直接返回 SubjectAssetOut，不返回 ApproveResultOut/impact。
    音频挂载为追加式（audio_entries 多条并存），无"替换被引用资产"风险，
    故无需两步确认。前端需分别处理两个审核操作的响应结构。
    """

    candidate_id: str


# ---- 列表响应 ----


class SubjectListOut(BaseModel):
    """主体列表响应。"""

    items: list[SubjectAssetOut]
    total: int


class ParentSubjectListOut(BaseModel):
    """父主体列表响应。"""

    items: list[ParentSubjectOut]


class AssetEntryListOut(BaseModel):
    """资产条目列表响应 — 列表页用于取计数。"""

    items: list[AssetEntryOut]
    total: int


class CandidateListOut(BaseModel):
    """候选列表响应 — 含图片候选与声音候选。"""

    image_candidates: list[CandidateOut] = Field(default_factory=list)
    audio_candidates: list[CandidateOut] = Field(default_factory=list)


class CanvasProjectEnsureOut(BaseModel):
    """画布项目 ensure 出参 — 幂等创建/返回 materials_id。"""

    materials_id: str
    name: str
    script_id: str
    created: bool = False  # 是否本次新建


class ImpactDetailOut(BaseModel):
    """影响摘要单条 — 引用该主体的镜头组。"""

    shot_group_id: str
    scene_no: int
    group_no: str
    episode_id: str
    materials_category: str  # character | background | prop | other


class ImpactSummaryOut(BaseModel):
    """替换被引用资产的影响摘要。"""

    referenced: bool
    shot_group_count: int
    details: list[ImpactDetailOut]


class ApproveResultOut(BaseModel):
    """审核操作结果 — 含主体最新状态与替换影响摘要。"""

    subject: SubjectAssetOut
    impact: ImpactSummaryOut | None = None
