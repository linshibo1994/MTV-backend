"""分镜模块数据访问层。

提供分镜层级查询、视频工作台查询、封面查询。
严格遵循 04-数据模型与API文档.md 与 02-功能规格文档.md 第 5.3~5.6 节字段契约。
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.episode import Episode
from app.models.scene import Scene
from app.models.shot import Shot
from app.models.shot_group import (
    FINAL_SHOT_GROUP_UNIT_PREFIX,
    ShotGroup,
    shot_group_content_hash,
)
from app.models.video_generation_job import VideoGenerationJob
from app.schemas.shot import (
    CoverOut,
    MaterialItemOut,
    MaterialsOut,
    ReferenceFramesOut,
    ReferenceImageAuditOut,
    SceneHierarchyOut,
    SceneWorkbenchOut,
    ShotGroupHierarchyOut,
    ShotGroupWorkbenchOut,
    ShotHierarchyOut,
    ShotOut,
    VideoWorkbenchOut,
)
from app.services.reference_aggregator import aggregate_references
from app.services.reference_image_audit import (
    current_audit_state,
    include_first_frame,
    public_audit_state,
)

# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


def _to_reference_frames(reference_frames: dict | None) -> ReferenceFramesOut | None:
    """将 JSONB 字典转为 ReferenceFramesOut。"""
    if reference_frames is None:
        return None
    return ReferenceFramesOut(
        first_frame=reference_frames.get("first_frame"),
        last_frame=reference_frames.get("last_frame"),
        reference_frame=reference_frames.get("reference_frame"),
    )


def _to_materials(materials: dict | None) -> MaterialsOut | None:
    """将 JSONB 字典转为 MaterialsOut。"""
    if materials is None:
        return None
    visual = {
        key: materials.get(key, [])
        for key in ("character", "background", "prop", "other")
    }
    return MaterialsOut(
        character=_normalise_material_items(visual["character"]),
        background=_normalise_material_items(visual["background"]),
        prop=_normalise_material_items(visual["prop"]),
        other=_normalise_material_items(visual["other"]),
    )


def _normalise_material_items(items: object) -> list[MaterialItemOut]:
    """兼容旧 list[str] 与新 typed material item。"""
    if not isinstance(items, list):
        return []
    normalised: list[MaterialItemOut] = []
    for item in items:
        if isinstance(item, str):
            normalised.append(MaterialItemOut(name=item))
        elif isinstance(item, dict) and item.get("name"):
            normalised.append(
                MaterialItemOut(
                    name=str(item["name"]),
                    subject_id=item.get("subject_id"),
                    asset_entry_id=item.get("asset_entry_id"),
                )
            )
    return normalised


def _normalise_audio_elements(audio_elements: object) -> list[dict[str, str]]:
    """归一化音频主体，图片 materials 与音频审核状态分离。"""
    if not audio_elements:
        return []
    raw_items: list[object] = []
    if isinstance(audio_elements, dict):
        for section_key, item_type in (("characters", "character"), ("sounds", "sound")):
            values = audio_elements.get(section_key)
            if isinstance(values, list):
                for value in values:
                    if isinstance(value, dict):
                        raw_items.append({"type": item_type, **value})
                    else:
                        raw_items.append({"type": item_type, "name": value})
    elif isinstance(audio_elements, list):
        raw_items = audio_elements

    seen: set[tuple[str, str]] = set()
    normalised: list[dict[str, str]] = []
    for item in raw_items:
        if isinstance(item, str):
            entry = {"name": item, "type": "character"}
        elif isinstance(item, dict) and item.get("name"):
            entry = {
                "name": str(item["name"]),
                "type": str(item.get("type") or item.get("subject_type") or "character"),
            }
            if item.get("subject_id"):
                entry["subject_id"] = str(item["subject_id"])
            if item.get("asset_entry_id"):
                entry["asset_entry_id"] = str(item["asset_entry_id"])
        else:
            continue
        dedupe_key = (entry["type"], entry["name"])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalised.append(entry)
    return normalised


def _group_sort_key(group: Any) -> tuple[int, int, str]:
    """自然排序：同一场景内 G2 必须排在 G10 前。"""
    group_index = getattr(group, "group_index", 0) or 0
    if group_index > 0:
        return getattr(group, "scene_no", 0), group_index, group.group_no
    digits = "".join(ch for ch in (group.group_no or "") if ch.isdigit())
    numeric = int(digits) if digits else 0
    return getattr(group, "scene_no", 0), numeric, group.group_no


def _job_matches_group_snapshot(
    input_json: dict | None,
    group: ShotGroup,
    *,
    finished_at: Any | None = None,
) -> bool:
    """历史成功任务仅在镜头组更新时间快照一致时仍代表当前成片。"""

    if group.updated_at is None:
        return False
    version = (input_json or {}).get("shot_group_version") or {}
    content_hash = version.get("shot_group_content_hash")
    if content_hash:
        return content_hash == shot_group_content_hash(group)
    if version.get("shot_group_updated_at") == group.updated_at.isoformat():
        return True
    # 兼容上线前已完成的任务：末帧是在任务成功事务内写回，可能晚于创建快照；
    # 只要镜头组最后更新时间不晚于任务完成时间，仍属于该成片。
    return bool(finished_at is not None and group.updated_at <= finished_at)


def _shot_to_out(shot: Shot) -> ShotOut:
    return ShotOut(
        shot_id=shot.shot_id,
        scene_no=shot.scene_no,
        shot_group_id=shot.shot_group_id,
        group_no=shot.group_no,
        shot_no=shot.shot_no,
        display_no=shot.display_no,
        content=shot.content,
        description=shot.description,
        shot_line=getattr(shot, "shot_line", None),
        start_seconds=getattr(shot, "start_seconds", None),
        end_seconds=getattr(shot, "end_seconds", None),
        characters=list(getattr(shot, "characters", None) or []),
        props=list(getattr(shot, "props", None) or []),
        sounds=list(getattr(shot, "sounds", None) or []),
        dialogue_character=getattr(shot, "dialogue_character", "") or "",
    )


def _is_ungrouped(shot: Shot, group_ids: set[str]) -> bool:
    """判断镜头是否未分组 (shot_group_id 不在已知组集合中)。"""
    return shot.shot_group_id not in group_ids


def _infer_mime_type(storage_key: str | None) -> str | None:
    """根据 storage_key 扩展名推断图片 MIME，无法判定时默认 image/png。"""
    if not storage_key:
        return None
    lower = storage_key.lower()
    if lower.endswith(".jpg") or lower.endswith(".jpeg"):
        return "image/jpeg"
    if lower.endswith(".webp"):
        return "image/webp"
    if lower.endswith(".png"):
        return "image/png"
    # 兜底默认 png (平台首帧多为 png 输出)
    return "image/png"


# ---------------------------------------------------------------------------
# shot-hierarchy
# ---------------------------------------------------------------------------


async def get_shot_hierarchy(
    db: AsyncSession, script_id: str, episode_id: str
) -> ShotHierarchyOut:
    """获取分镜层级: Episode -> Scenes -> ShotGroups -> Shots。

    返回 shot-hierarchy 视角，含 groups[] 与 ungrouped_shots[]。
    """
    # 校验分集归属
    ep_result = await db.execute(
        select(Episode).where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
    )
    episode = ep_result.scalar_one_or_none()
    if episode is None:
        return ShotHierarchyOut(episode_id=episode_id, episode_version=0, scenes=[])

    # 查询该分集下所有场景 (按 scene_no 排序)
    scenes_result = await db.execute(
        select(Scene)
        .where(Scene.episode_id == episode_id)
        .order_by(Scene.scene_no)
    )
    scenes = scenes_result.scalars().all()

    if not scenes:
        return ShotHierarchyOut(
            episode_id=episode_id, episode_version=episode.episode_version, scenes=[]
        )

    # 查询该分集下所有镜头组 (selectin 加载 shots)
    groups_result = await db.execute(
        select(ShotGroup)
        .where(
            ShotGroup.episode_id == episode_id,
            ShotGroup.scene_no.in_([s.scene_no for s in scenes]),
        )
        .options(selectinload(ShotGroup.shots))
        .order_by(ShotGroup.scene_no, ShotGroup.group_no)
    )
    groups = groups_result.scalars().unique().all()

    # 查询该分集下所有镜头 (用于 ungrouped_shots)
    # 通过 join ShotGroup 按 episode_id 过滤，避免不同分集相同 scene_no 的镜头互相泄漏
    # (Shot 模型无 episode_id 列，必须经 ShotGroup 关联到分集)
    shots_result = await db.execute(
        select(Shot)
        .join(ShotGroup, ShotGroup.shot_group_id == Shot.shot_group_id)
        .where(ShotGroup.episode_id == episode_id)
        .order_by(ShotGroup.group_index, Shot.display_no, Shot.shot_id)
    )
    all_shots = shots_result.scalars().all()

    # 按 scene_no 分组镜头组
    groups_by_scene: dict[int, list[ShotGroup]] = {}
    group_ids: set[str] = set()
    for g in groups:
        groups_by_scene.setdefault(g.scene_no, []).append(g)
        group_ids.add(g.shot_group_id)
    for grouped_shot_groups in groups_by_scene.values():
        grouped_shot_groups.sort(key=_group_sort_key)

    # 按 scene_no 分组未分组镜头
    ungrouped_by_scene: dict[int, list[Shot]] = {}
    for shot in all_shots:
        if _is_ungrouped(shot, group_ids):
            ungrouped_by_scene.setdefault(shot.scene_no, []).append(shot)

    scene_list: list[SceneHierarchyOut] = []
    for scene in scenes:
        hierarchy_groups: list[ShotGroupHierarchyOut] = []
        for g in groups_by_scene.get(scene.scene_no, []):
            sorted_shots = sorted(g.shots, key=lambda s: s.display_no)
            hierarchy_groups.append(
                ShotGroupHierarchyOut(
                    shot_group_id=g.shot_group_id,
                    scene_no=g.scene_no,
                    group_no=g.group_no,
                    prompt=g.prompt,
                    reference_frames=_to_reference_frames(g.reference_frames),
                    shots=[_shot_to_out(s) for s in sorted_shots],
                )
            )
        scene_list.append(
            SceneHierarchyOut(
                scene_no=scene.scene_no,
                groups=hierarchy_groups,
                ungrouped_shots=[
                    _shot_to_out(s) for s in ungrouped_by_scene.get(scene.scene_no, [])
                ],
            )
        )

    return ShotHierarchyOut(
        episode_id=episode_id,
        episode_version=episode.episode_version,
        scenes=scene_list,
    )


# ---------------------------------------------------------------------------
# video-workbench
# ---------------------------------------------------------------------------


async def get_video_workbench(
    db: AsyncSession, script_id: str, episode_id: str
) -> VideoWorkbenchOut:
    """获取视频工作台: 含 unit_id/unit_runtime_key/materials/audio_elements。"""
    ep_result = await db.execute(
        select(Episode).where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
    )
    episode = ep_result.scalar_one_or_none()
    if episode is None:
        return VideoWorkbenchOut(episode_id=episode_id, episode_version=0, scenes=[])

    scenes_result = await db.execute(
        select(Scene)
        .where(Scene.episode_id == episode_id)
        .order_by(Scene.scene_no)
    )
    scenes = scenes_result.scalars().all()

    if not scenes:
        return VideoWorkbenchOut(
            episode_id=episode_id, episode_version=episode.episode_version, scenes=[]
        )

    groups_result = await db.execute(
        select(ShotGroup)
        .where(
            ShotGroup.episode_id == episode_id,
            ShotGroup.scene_no.in_([s.scene_no for s in scenes]),
            ShotGroup.unit_id.like(f"{FINAL_SHOT_GROUP_UNIT_PREFIX}%"),
        )
        .options(selectinload(ShotGroup.shots))
        .order_by(ShotGroup.scene_no, ShotGroup.group_no)
    )
    groups = groups_result.scalars().unique().all()

    shots_result = await db.execute(
        select(Shot)
        .join(ShotGroup, ShotGroup.shot_group_id == Shot.shot_group_id)
        .where(ShotGroup.episode_id == episode_id)
        .order_by(ShotGroup.group_index, Shot.display_no, Shot.shot_id)
    )
    all_shots = shots_result.scalars().all()

    groups_by_scene: dict[int, list[ShotGroup]] = {}
    group_ids: set[str] = set()
    for g in groups:
        groups_by_scene.setdefault(g.scene_no, []).append(g)
        group_ids.add(g.shot_group_id)
    for grouped_shot_groups in groups_by_scene.values():
        grouped_shot_groups.sort(key=_group_sort_key)

    ungrouped_by_scene: dict[int, list[Shot]] = {}
    for shot in all_shots:
        if _is_ungrouped(shot, group_ids):
            ungrouped_by_scene.setdefault(shot.scene_no, []).append(shot)

    # 统计各镜头组是否有 succeeded 视频生成任务 (按 unit_id 关联)
    # pending_shot_count 语义 (04§1.16 / 02§5.6.2): 待生成镜头数 =
    # 该场景下属镜头组中无 succeeded 视频生成任务的组内镜头数量之和
    # (视频生成以 unit=镜头组 为单位, 组内全部镜头随组成片, 故按镜头数累计)
    all_unit_ids = [g.unit_id for g in groups if g.unit_id]
    succeeded_unit_ids: set[str] = set()
    if all_unit_ids:
        succ_result = await db.execute(
            select(
                VideoGenerationJob.unit_id,
                VideoGenerationJob.input_json,
                VideoGenerationJob.finished_at,
            ).where(
                VideoGenerationJob.unit_id.in_(all_unit_ids),
                VideoGenerationJob.status == "succeeded",
            )
        )
        group_by_unit = {group.unit_id: group for group in groups if group.unit_id}
        for unit_id, input_json, finished_at in succ_result.all():
            group = group_by_unit.get(unit_id)
            if group is not None and _job_matches_group_snapshot(
                input_json,
                group,
                finished_at=finished_at,
            ):
                succeeded_unit_ids.add(unit_id)

    # 按场景统计待生成镜头数: 累加无 succeeded 视频的镜头组内镜头数量
    pending_shot_count_by_scene: dict[int, int] = {}
    for scene_no, pending_shots in ungrouped_by_scene.items():
        pending_shot_count_by_scene[scene_no] = len(pending_shots)
    for g in groups:
        if g.unit_id not in succeeded_unit_ids:
            shot_count = len(g.shots) if g.shots else 0
            pending_shot_count_by_scene[g.scene_no] = (
                pending_shot_count_by_scene.get(g.scene_no, 0) + shot_count
            )

    scene_list: list[SceneWorkbenchOut] = []
    for scene in scenes:
        workbench_groups: list[ShotGroupWorkbenchOut] = []
        for g in groups_by_scene.get(scene.scene_no, []):
            sorted_shots = sorted(g.shots, key=lambda s: s.display_no)
            references = await aggregate_references(db, g.shot_group_id)
            from app.services.video_generation import _resolve_first_frame

            include_first_frame(references, await _resolve_first_frame(db, g))
            audit_state = current_audit_state(g.reference_image_audit, references)
            workbench_groups.append(
                ShotGroupWorkbenchOut(
                    shot_group_id=g.shot_group_id,
                    scene_no=g.scene_no,
                    background=scene.background,
                    scene_key=scene.scene_key,
                    group_no=g.group_no,
                    unit_id=g.unit_id,
                    unit_runtime_key=g.unit_runtime_key,
                    prompt=g.prompt,
                    duration_seconds=g.duration_seconds,
                    video_duration_seconds=g.video_duration_seconds,
                    materials=_to_materials(g.materials),
                    audio_elements=_normalise_audio_elements(g.audio_elements),
                    shot_ids=[s.shot_id for s in sorted_shots],
                    shot_display_nos=[s.display_no for s in sorted_shots],
                    shot_nos=[s.shot_no for s in sorted_shots],
                    description_text=g.description_text,
                    reference_frames=_to_reference_frames(g.reference_frames),
                    reference_image_audit=ReferenceImageAuditOut(**public_audit_state(audit_state)),
                    content_hash=shot_group_content_hash(g),
                    updated_at=g.updated_at,
                )
            )
        scene_list.append(
            SceneWorkbenchOut(
                scene_key=scene.scene_key,
                scene_no=scene.scene_no,
                background=scene.background,
                label=scene.label,
                shot_group_count=len(workbench_groups),
                pending_shot_count=pending_shot_count_by_scene.get(scene.scene_no, 0),
                shot_groups=workbench_groups,
                pending_shots=[
                    _shot_to_out(s)
                    for s in ungrouped_by_scene.get(scene.scene_no, [])
                ],
            )
        )

    return VideoWorkbenchOut(
        episode_id=episode_id,
        episode_version=episode.episode_version,
        scenes=scene_list,
    )


# ---------------------------------------------------------------------------
# cover
# ---------------------------------------------------------------------------


def _cover_from_ordered_groups(groups: list[ShotGroup]) -> str | None:
    """只检查真正首个镜头组，不用后续组伪装封面。"""
    if not groups:
        return None
    first_group = groups[0]
    if not first_group.reference_frames:
        return None
    return first_group.reference_frames.get("first_frame") or first_group.reference_frames.get("last_frame")


async def get_cover(db: AsyncSession, script_id: str) -> CoverOut:
    """获取视频项目封面 — 首个镜头组的首帧 (reference_frames.first_frame)。

    查询顺序: 按 episode_no -> scene_no -> group_no 取第一个镜头组的 first_frame。
    若 first_frame 为 null, 返回 cover=null。
    """
    # 查询该剧本下所有分集 (按 episode_no 排序)
    episodes_result = await db.execute(
        select(Episode.episode_id, Episode.episode_no)
        .where(Episode.script_id == script_id)
        .order_by(Episode.episode_no)
    )
    episodes = episodes_result.all()

    if not episodes:
        return CoverOut(script_id=script_id, cover=None)

    first_episode_id = episodes[0][0]
    if not first_episode_id:
        return CoverOut(script_id=script_id, cover=None)

    for ep_id, _ep_no in episodes[:1]:
        # 查询该分集下第一个场景的第一个镜头组
        groups_result = await db.execute(
            select(ShotGroup)
            .where(
                ShotGroup.episode_id == ep_id,
                ShotGroup.unit_id.like(f"{FINAL_SHOT_GROUP_UNIT_PREFIX}%"),
            )
            .order_by(ShotGroup.scene_no, ShotGroup.group_no)
        )
        groups = sorted(groups_result.scalars().all(), key=_group_sort_key)
        first_frame = _cover_from_ordered_groups(groups)

        if first_frame:
            return CoverOut(
                script_id=script_id,
                cover=first_frame,
                mime_type=_infer_mime_type(first_frame),
            )

    return CoverOut(script_id=script_id, cover=None)


__all__ = [
    "get_shot_hierarchy",
    "get_video_workbench",
    "get_cover",
    "_cover_from_ordered_groups",
    "_group_sort_key",
    "_normalise_audio_elements",
]
