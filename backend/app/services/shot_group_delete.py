"""正式镜头组的安全删除与关联视频资源清理。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from sqlalchemy import delete, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import FINAL_SHOT_GROUP_UNIT_PREFIX, ShotGroup
from app.models.timeline_render_job import TimelineRenderJob
from app.models.video_generation_job import VideoGenerationJob
from app.models.video_timeline_arrangement import VideoTimelineArrangement
from app.services.storage_references import (
    StorageReferenceExclusions,
    acquire_storage_reference_barrier,
    collect_storage_keys,
    decide_storage_key_deletes,
    enqueue_storage_deletes,
)
from app.services.task_dispatch import cancel_outbox_task
from app.services.video_cleanup_policy import has_retryable_video_cleanup_debt

_ACTIVE_VIDEO_STATUSES = frozenset({"starting", "processing"})
_TERMINAL_VIDEO_STATUSES = frozenset({"succeeded", "failed", "canceled"})
_ACTIVE_TIMELINE_STATUSES = frozenset({"pending", "ready", "retry", "starting", "processing"})


@dataclass(frozen=True)
class ShotGroupDeleteResult:
    """镜头组删除后的可审计摘要。"""

    episode_version: int
    removed_shot_count: int
    removed_video_job_ids: tuple[str, ...]
    removed_timeline_arrangement_count: int
    scheduled_storage_delete_count: int


def _timeline_job_references_video_jobs(
    input_json: dict[str, Any] | None,
    video_job_ids: set[str],
) -> bool:
    """检查时间线冻结快照是否仍在使用待删除的视频任务。"""

    if not isinstance(input_json, dict) or not video_job_ids:
        return False
    segments = input_json.get("segments")
    if not isinstance(segments, list):
        return False
    return any(
        isinstance(segment, dict) and str(segment.get("job_id") or "") in video_job_ids
        for segment in segments
    )


async def cancel_active_video_jobs_for_shot_group(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_group_id: str,
    actor_user_id: str,
) -> tuple[str, ...]:
    """先按既有取消闭环停止普通运行任务，再进入镜头组删除事务。

    远端取消需要先把本地 canceled 与补偿债务单独提交，无法和随后硬删除做成
    单一数据库事务；因此只有既有取消链路确认安全终态后，删除服务才会继续。
    """

    active_job_ids = list(
        (
            await db.execute(
                select(VideoGenerationJob.job_id)
                .where(
                    VideoGenerationJob.script_id == script_id,
                    VideoGenerationJob.episode_id == episode_id,
                    VideoGenerationJob.shot_group_id == shot_group_id,
                    VideoGenerationJob.status.in_(sorted(_ACTIVE_VIDEO_STATUSES)),
                )
                .order_by(VideoGenerationJob.job_id.asc())
            )
        ).scalars()
    )
    if not active_job_ids:
        return ()

    # 延迟导入避免视频生成服务加载时与路由形成循环依赖。
    from app.services.video_generation import cancel_video_job

    canceled_ids: list[str] = []
    for job_id in active_job_ids:
        try:
            canceled = await cancel_video_job(
                db,
                script_id=script_id,
                job_id=job_id,
                actor_user_id=actor_user_id,
            )
        except ConflictError:
            # 并发下任务可能已由 worker/其他用户推进到终态；仅此种竞态可继续。
            refreshed = (
                await db.execute(
                    select(VideoGenerationJob.status).where(
                        VideoGenerationJob.job_id == job_id
                    )
                )
            ).scalar_one_or_none()
            if refreshed in _ACTIVE_VIDEO_STATUSES:
                raise
            continue
        if canceled.status == "canceled":
            canceled_ids.append(job_id)
    return tuple(canceled_ids)


async def _load_group_and_scene_for_delete(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_group_id: str,
) -> tuple[ShotGroup, Scene]:
    """按照 Script → Episode → Scene → ShotGroup 的锁序取得删除对象。"""

    ownership = (
        await db.execute(
            select(ShotGroup.script_id, ShotGroup.episode_id, ShotGroup.scene_no).where(
                ShotGroup.shot_group_id == shot_group_id
            )
        )
    ).one_or_none()
    if ownership is None:
        raise NotFoundError("镜头组不存在")
    owner_script_id, owner_episode_id, group_scene_no = ownership
    if owner_script_id != script_id or owner_episode_id != episode_id:
        raise ForbiddenError("镜头组不属于当前剧本/分集")

    scene = (
        await db.execute(
            select(Scene)
            .where(
                Scene.episode_id == episode_id,
                Scene.scene_no == group_scene_no,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if scene is None:
        raise ConflictError("镜头组所属场景不存在")

    group = (
        await db.execute(
            select(ShotGroup)
            .where(
                ShotGroup.shot_group_id == shot_group_id,
                ShotGroup.script_id == script_id,
                ShotGroup.episode_id == episode_id,
            )
            .options(noload(ShotGroup.shots))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if group is None:
        raise ConflictError("镜头组状态已变化，请刷新后重试")
    if group.scene_no != scene.scene_no:
        raise ConflictError("镜头组所属场景已变化，请刷新后重试")
    if group.unit_id != f"{FINAL_SHOT_GROUP_UNIT_PREFIX}{group.shot_group_id}":
        raise ConflictError("仅支持删除已合并的正式镜头组")
    return group, scene


async def _assert_no_active_timeline_dependency(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    video_job_ids: set[str],
) -> None:
    """运行中的时间线会读取冻结快照，不能在中途拿走其视频来源。"""

    if not video_job_ids:
        return
    timeline_jobs = list(
        (
            await db.execute(
                select(TimelineRenderJob)
                .where(
                    TimelineRenderJob.script_id == script_id,
                    TimelineRenderJob.episode_id == episode_id,
                    TimelineRenderJob.status.in_(sorted(_ACTIVE_TIMELINE_STATUSES)),
                )
                .options(noload(TimelineRenderJob.script), noload(TimelineRenderJob.episode))
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        ).scalars().all()
    )
    blocking_count = sum(
        _timeline_job_references_video_jobs(job.input_json, video_job_ids)
        for job in timeline_jobs
    )
    if blocking_count:
        raise ConflictError(
            f"当前镜头组正在被 {blocking_count} 个时间线合成任务使用，"
            "请先取消或等待合成任务结束后再删除"
        )


async def delete_shot_group(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_group_id: str,
    expected_episode_version: int,
) -> ShotGroupDeleteResult:
    """删除正式镜头组、组内分镜及已安全终态的视频资源。

    调用方负责在本方法前完成用户授权与用户确认；本方法不提交事务，保证媒体删除
    outbox、数据删除与审计可以在同一个事务内完成。
    """

    locked_script_id = (
        await db.execute(
            select(Script.id)
            .where(Script.id == script_id)
            .with_for_update(read=True, key_share=True)
        )
    ).scalar_one_or_none()
    if locked_script_id is None:
        raise NotFoundError("剧本不存在")

    episode = (
        await db.execute(
            select(Episode)
            .where(
                Episode.episode_id == episode_id,
                Episode.script_id == script_id,
            )
            .options(noload(Episode.script), noload(Episode.scenes))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.episode_version != expected_episode_version:
        raise ConflictError("episode_version 冲突")

    group, scene = await _load_group_and_scene_for_delete(
        db,
        script_id=script_id,
        episode_id=episode_id,
        shot_group_id=shot_group_id,
    )
    video_jobs = list(
        (
            await db.execute(
                select(VideoGenerationJob)
                .where(
                    VideoGenerationJob.script_id == script_id,
                    VideoGenerationJob.episode_id == episode_id,
                    VideoGenerationJob.shot_group_id == shot_group_id,
                )
                .options(noload(VideoGenerationJob.script))
                .execution_options(populate_existing=True)
                .order_by(VideoGenerationJob.job_id.asc())
                .with_for_update()
            )
        ).scalars().all()
    )
    active_count = sum(job.status not in _TERMINAL_VIDEO_STATUSES for job in video_jobs)
    if active_count:
        raise ConflictError(
            f"镜头组仍有 {active_count} 个视频任务正在生成，请等待取消完成后再删除"
        )
    debt_count = sum(
        has_retryable_video_cleanup_debt(
            job.status,
            job.output_json,
            job.error_type,
            job.retryable,
        )
        for job in video_jobs
    )
    if debt_count:
        raise ConflictError(
            f"镜头组仍有 {debt_count} 个视频任务处于人工核对或清理中，"
            "请等待后台处理完成后再删除"
        )

    video_job_ids = {job.job_id for job in video_jobs}
    if video_job_ids:
        reservations = list(
            (
                await db.execute(
                    select(CreditReservation)
                    .where(
                        CreditReservation.source_type == "generation_job",
                        CreditReservation.source_id.in_(sorted(video_job_ids)),
                    )
                    .order_by(CreditReservation.id.asc())
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
        reserved_count = sum(reservation.status == "reserved" for reservation in reservations)
        if reserved_count:
            raise ConflictError(
                f"镜头组仍有 {reserved_count} 笔冻结积分尚未结算或释放，"
                "请等待后台处理完成后再删除"
            )

    await _assert_no_active_timeline_dependency(
        db,
        script_id=script_id,
        episode_id=episode_id,
        video_job_ids=video_job_ids,
    )

    shots = list(
        (
            await db.execute(
                select(Shot)
                .where(Shot.shot_group_id == shot_group_id)
                .order_by(Shot.display_no.asc(), Shot.shot_id.asc())
                .with_for_update()
            )
        ).scalars().all()
    )

    storage_keys: set[str] = set()
    collect_storage_keys(group.reference_frames, storage_keys)
    collect_storage_keys(group.materials, storage_keys)
    collect_storage_keys(group.audio_elements, storage_keys)
    for job in video_jobs:
        collect_storage_keys(job.input_json, storage_keys)
        collect_storage_keys(job.output_json, storage_keys)
        collect_storage_keys(job.metrics, storage_keys)
    exclusions = StorageReferenceExclusions(
        shot_group_ids={shot_group_id},
        video_generation_job_ids=set(video_job_ids),
    )
    await acquire_storage_reference_barrier(db)
    storage_decision = await decide_storage_key_deletes(
        db,
        storage_keys,
        exclusions=exclusions,
    )
    await enqueue_storage_deletes(
        db,
        set(storage_decision.deletable_keys),
        exclusions=exclusions,
        protected_storage_keys=set(storage_decision.protected_keys),
    )

    for job in video_jobs:
        await cancel_outbox_task(
            db,
            task_type="video.generate",
            job_id=job.job_id,
            attempt=job.attempt,
        )

    removed_timeline_arrangement_count = 0
    if video_job_ids:
        arrangement_delete = cast(
            CursorResult[Any],
            await db.execute(
                delete(VideoTimelineArrangement).where(
                    VideoTimelineArrangement.job_id.in_(sorted(video_job_ids))
                )
            ),
        )
        removed_timeline_arrangement_count = int(arrangement_delete.rowcount or 0)
        await db.execute(
            delete(VideoGenerationJob).where(
                VideoGenerationJob.job_id.in_(sorted(video_job_ids))
            )
        )
    await db.execute(delete(Shot).where(Shot.shot_group_id == shot_group_id))
    await db.execute(delete(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id))

    scene.shot_group_count = max(0, int(scene.shot_group_count or 0) - 1)
    scene.pending_shot_count = max(0, int(scene.pending_shot_count or 0) - len(shots))
    episode.episode_version += 1
    await db.flush()

    return ShotGroupDeleteResult(
        episode_version=episode.episode_version,
        removed_shot_count=len(shots),
        removed_video_job_ids=tuple(sorted(video_job_ids)),
        removed_timeline_arrangement_count=removed_timeline_arrangement_count,
        scheduled_storage_delete_count=len(storage_decision.deletable_keys),
    )


__all__ = [
    "ShotGroupDeleteResult",
    "cancel_active_video_jobs_for_shot_group",
    "delete_shot_group",
]
