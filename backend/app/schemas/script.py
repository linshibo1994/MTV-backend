"""剧本/分集/主体/流水线相关的 Pydantic v2 数据模型。

字段定义严格遵循 docs/Claude/04-数据模型与API文档.md 第 1.5~1.8 节与
02-功能规格文档.md 第 2.2 节。出参从 ORM 构造，入参由请求体校验。
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


# --------------------------------------------------------------------------- #
# Script
# --------------------------------------------------------------------------- #
class ScriptOut(BaseModel):
    """剧本详情 — GET /scripts/<id>。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str
    scope: Literal["personal", "team"]
    owner_user_id: str
    org_id: str | None = None
    created_by_user_id: str
    created_at: datetime
    updated_at: datetime


class ScriptListItem(BaseModel):
    """剧本列表项 — GET /scripts/page。

    团队项目带 current_user_project_role，个人项目为 null。
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str
    scope: Literal["personal", "team"]
    owner_user_id: str
    owner_username: str | None = None
    org_id: str | None = None
    created_by_user_id: str
    current_user_project_role: str | None = None
    updated_at: datetime


class ScriptCreateIn(BaseModel):
    """新建剧本入参 — POST /scripts。"""

    title: str = Field(min_length=1, max_length=512)
    scope: Literal["personal", "team"] = "personal"


class ScriptUpdateIn(BaseModel):
    """重命名剧本入参 — PATCH /scripts/<id>。"""

    title: str = Field(min_length=1, max_length=512)


class ScriptCopyIn(BaseModel):
    """复制剧本入参；缺省沿用来源标题与范围。"""

    title: str | None = Field(default=None, min_length=1, max_length=512)
    scope: Literal["personal", "team"] | None = None


class ScriptPageOut(BaseModel):
    """剧本分页响应。"""

    items: list[ScriptListItem]
    total: int


# --------------------------------------------------------------------------- #
# Episode
# --------------------------------------------------------------------------- #
class EpisodeListItem(BaseModel):
    """分集列表项 — GET /scripts/<id>/episodes。

    亦用作 PUT /scripts/<id>/episodes/<eid> 保存响应：必须回传递增后的
    client_save_seq，否则客户端连续保存时只能携带旧序号，WHERE client_save_seq=?
    不匹配导致 rowcount=0 误判为 409 冲突。
    """

    model_config = ConfigDict(from_attributes=True)

    episode_id: str
    script_id: str
    episode_no: int
    title: str
    episode_version: int
    client_save_seq: int
    updated_at: datetime


class EpisodeOut(BaseModel):
    """分集完整对象 — breakdown 中返回，含 script_text/乐观锁字段。"""

    model_config = ConfigDict(from_attributes=True)

    episode_id: str
    script_id: str
    episode_no: int
    title: str
    script_text: str | None = None
    script_file_storage_key: str | None = None
    schema_version: int
    episode_version: int
    client_save_seq: int
    created_at: datetime
    updated_at: datetime


class EpisodeSaveIn(BaseModel):
    """保存分集入参 — PUT /scripts/<id>/episodes/<eid>。

    带乐观锁: episode_version + client_save_seq，冲突返回 409。
    """

    script_text: str | None = None
    title: str | None = None
    episode_no: int | None = Field(default=None, ge=1)
    episode_version: int
    client_save_seq: int


class EpisodeCreateIn(BaseModel):
    """新增分集入参 — POST /scripts/<id>/episodes。"""

    title: str = Field(min_length=1, max_length=255)
    script_text: str | None = None
    # 集号可选，缺省自动取最大集号 + 1
    episode_no: int | None = Field(default=None, ge=1)


class LifecycleDependencySummary(BaseModel):
    """剧本/分集删除前的派生资源依赖摘要。"""

    episode_count: int = 0
    subject_count: int = 0
    asset_count: int = 0
    shot_group_count: int = 0
    shot_count: int = 0
    video_count: int = 0
    job_count: int = 0
    active_job_count: int = 0
    canvas_count: int = 0

    @property
    def has_dependencies(self) -> bool:
        return any(
            (
                self.episode_count,
                self.subject_count,
                self.asset_count,
                self.shot_group_count,
                self.shot_count,
                self.video_count,
                self.job_count,
                self.active_job_count,
                self.canvas_count,
            )
        )


class LifecycleDeleteOut(BaseModel):
    """显式确认删除后的响应。"""

    detail: str
    dependencies: LifecycleDependencySummary


# --------------------------------------------------------------------------- #
# Breakdown
# --------------------------------------------------------------------------- #
class BreakdownSubject(BaseModel):
    """breakdown 视角的主体（精简字段）。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    type: Literal["character", "scene", "prop", "sound"]
    name: str
    description: str | None = None
    is_child: bool
    episode_nos: list[int] | None = None


class BreakdownShotGroup(BaseModel):
    """breakdown 中的镜头组（分镜生成前为空）。"""

    model_config = ConfigDict(from_attributes=True)

    shot_group_id: str
    scene_no: int
    group_no: str
    prompt: str | None = None


class BreakdownShot(BaseModel):
    """breakdown 中的镜头（分镜生成前为空）。"""

    model_config = ConfigDict(from_attributes=True)

    shot_id: str
    scene_no: int
    shot_group_id: str
    group_no: str
    content: str | None = None


class BreakdownOut(BaseModel):
    """GET /scripts/<id>/episodes/<eid>/breakdown 响应。"""

    episode: EpisodeOut
    subjects: list[BreakdownSubject] = Field(default_factory=list)
    shots: list[BreakdownShot] = Field(default_factory=list)
    shot_groups: list[BreakdownShotGroup] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Pipeline status
# --------------------------------------------------------------------------- #
# 5 阶段顺序与中文名（与 04 文档 2.3 节一致）
_STAGE_LABELS: dict[str, str] = {
    "split": "剧集拆分",
    "extract": "提取主体",
    "merge": "合并主体",
    "enrich": "设计线索增强",
    "parents": "归纳父主体",
}

STAGE_ORDER: list[str] = ["split", "extract", "merge", "enrich", "parents"]


class PipelineStageOut(BaseModel):
    """单个流水线阶段状态。

    06§1.3 契约规定 detail 为展示字符串（如 "2 集"/"44 个子主体"），但流水线
    内部将中间结果以 dict 形式落库（split/extract/merge/enrich/parents 各自的
    detail 结构不同，extract 更是写入完整 episodes 数组供 merge 阶段消费）。
    出参时据此归一化为可读字符串，避免向前端返回 dict/大数组导致渲染异常
    （[object Object]）与体积膨胀。内部消费者（merge/extract 等）直接读 ORM
    列 row.detail，不经过本 schema，故转换不影响内部链路。
    """

    stage: str
    label: str
    status: Literal["pending", "running", "done", "failed"]
    detail: Any = None
    current: int | None = None
    total: int | None = None

    @model_validator(mode="after")
    def _detail_to_display_str(self) -> "PipelineStageOut":
        d = self.detail
        # None / 空态 -> 空字符串（契约 pending 阶段 detail 为 ""）
        if d is None:
            self.detail = ""
            return self
        # 已是字符串则原样返回
        if isinstance(d, str):
            return self
        # 非字符串/字典兜底转字符串
        if not isinstance(d, dict):
            self.detail = str(d)
            return self
        # 按阶段将内部 dict 归一化为展示字符串
        count = d.get("count")
        if self.stage == "split":
            # split done: {"count": N}；running 期仅 {"input_version":...} 无 count
            self.detail = f"{count} 集" if isinstance(count, int) else ""
        elif self.stage == "extract":
            # {"episodes": [...]}，len 为已提取集数；total 来自 stage 行的 total 列
            eps = d.get("episodes")
            n = len(eps) if isinstance(eps, list) else (count if isinstance(count, int) else None)
            if n is None:
                self.detail = ""
            elif isinstance(self.total, int) and self.total > 0:
                self.detail = f"{n}/{self.total} 集"
            else:
                self.detail = f"{n} 集"
        elif self.stage == "merge":
            self.detail = f"{count} 个子主体" if isinstance(count, int) else ""
        elif self.stage == "enrich":
            self.detail = f"{count} 条线索增强" if isinstance(count, int) else ""
        elif self.stage == "parents":
            self.detail = f"{count} 个父主体" if isinstance(count, int) else ""
        else:
            self.detail = ""
        return self


class PipelineStatusOut(BaseModel):
    """GET /scripts/<id>/pipeline-status 响应。"""

    stages: list[PipelineStageOut]
    running: bool
    failed: bool
    all_done: bool


# --------------------------------------------------------------------------- #
# Extract subjects status
# --------------------------------------------------------------------------- #
class ExtractEpisodeStatus(BaseModel):
    """单集提取状态。"""

    episode_id: str
    episode_no: int
    title: str
    extract_status: str
    extracted: bool


class ExtractSubjectsStatusOut(BaseModel):
    """GET /scripts/<id>/extract-subjects-status 响应。"""

    total_episodes: int
    extracted_episodes: int
    all_extracted: bool
    episodes: list[ExtractEpisodeStatus]
    merge_status: str
    merge_job_id: str | None = None
    has_active_merge: bool
