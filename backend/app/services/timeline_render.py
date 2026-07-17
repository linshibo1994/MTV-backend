"""时间线合成服务 — 持久化 RenderJob + outbox 编排。"""

from __future__ import annotations

import hashlib
import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.core.permissions import Action, check_timeline_script_access
from app.crud import timeline as timeline_crud
from app.models.credit import CreditAccount
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.services.credits import release as credits_release
from app.services.credits import reserve as credits_reserve
from app.services.task_dispatch import enqueue_task

_TIMELINE_RENDER_RESERVED_POINTS = 1


def _request_hash(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _extract_video_storage_key(job: VideoGenerationJob) -> str | None:
    """从视频生成 job 的 output_json 中提取产物 storage_key。"""

    if not job.output_json:
        return None
    data = job.output_json if isinstance(job.output_json, dict) else {}
    if data.get("storage_key"):
        return str(data["storage_key"])
    if data.get("video_storage_key"):
        return str(data["video_storage_key"])
    nested_video = data.get("video")
    if isinstance(nested_video, dict) and nested_video.get("storage_key"):
        return str(nested_video["storage_key"])
    return None


def _output_storage_key(render_job_id: str, script_id: str, episode_id: str) -> str:
    return f"generate_outputs/{script_id}/{episode_id}/{render_job_id}.mp4"


async def _resolve_user_account_id(db: AsyncSession, user_id: str) -> str:
    result = await db.execute(
        select(CreditAccount.id).where(
            CreditAccount.owner_type == "user",
            CreditAccount.owner_id == user_id,
        )
    )
    account_id = result.scalar_one_or_none()
    if account_id is None:
        raise ConflictError("当前用户无积分账户")
    return account_id


async def _build_snapshot(
    db: AsyncSession, script_id: str, episode_id: str
) -> tuple[list[dict], int]:
    """按 row_order 固化当前选中片段，worker 不再读取可变时间线。"""

    arrangements = await timeline_crud.list_arrangements(db, script_id, episode_id)
    selected = [item for item in arrangements if item.is_selected]
    if not arrangements:
        raise NotFoundError("时间线为空，无可合成的片段")
    if not selected:
        raise NotFoundError("没有选中的片段可合成")

    job_ids = [item.job_id for item in selected]
    result = await db.execute(
        select(VideoGenerationJob)
        .where(VideoGenerationJob.job_id.in_(job_ids))
        .options(noload(VideoGenerationJob.script))
    )
    jobs = {job.job_id: job for job in result.scalars().all()}
    segments: list[dict] = []
    timeline_version = 0
    for arrangement in selected:
        timeline_version = max(timeline_version, int(arrangement.version or 0))
        job = jobs.get(arrangement.job_id)
        if job is None:
            raise NotFoundError("视频生成任务不存在")
        if job.script_id != script_id or job.episode_id != episode_id:
            raise ForbiddenError("片段关联的视频任务不属于该剧本分集")
        if job.status != "succeeded":
            raise ConflictError("片段关联的视频任务尚未成功")
        storage_key = _extract_video_storage_key(job)
        if not storage_key:
            raise ConflictError("片段关联的视频任务缺少产物")
        segments.append(
            {
                "arrangement_id": arrangement.id,
                "arrangement_version": arrangement.version,
                "unit_runtime_key": arrangement.unit_runtime_key,
                "job_id": job.job_id,
                "row_order": arrangement.row_order,
                "storage_key": storage_key,
                "trim_start_seconds": float(arrangement.trim_start_seconds or 0.0),
                "trim_end_seconds": arrangement.trim_end_seconds,
            }
        )
    segments.sort(key=lambda item: item["row_order"])
    return segments, timeline_version


async def render(
    db: AsyncSession,
    script_id: str,
    episode_id: str,
    user: User,
    *,
    idempotency_key: str | None,
) -> dict:
    """触发时间线合成，返回持久化 render job。"""

    if not idempotency_key:
        raise ConflictError("缺少 Idempotency-Key")
    await check_timeline_script_access(db, user, script_id, Action.RUN_AI)
    episode = await timeline_crud.assert_episode_belongs(
        db, script_id=script_id, episode_id=episode_id
    )
    segments, timeline_version = await _build_snapshot(db, script_id, episode_id)
    request_payload = {
        "script_id": script_id,
        "episode_id": episode_id,
        "episode_version": episode.episode_version,
        "timeline_version": timeline_version,
        "segments": segments,
    }
    req_hash = _request_hash(request_payload)
    owner_user_id = user.user_id
    existing = await timeline_crud.find_render_job_by_idempotency(
        db, owner_user_id=owner_user_id, idempotency_key=idempotency_key
    )
    if existing is not None:
        if existing.request_hash != req_hash:
            raise ConflictError("Idempotency-Key 已用于不同时间线快照")
        return {
            "job_id": existing.job_id,
            "status": existing.status,
            "output_storage_key": existing.output_storage_key,
        }

    job_id = new_id()
    output_key = _output_storage_key(job_id, script_id, episode_id)
    reservation_id = await credits_reserve(
        db,
        account_id=await _resolve_user_account_id(db, owner_user_id),
        source_type="timeline_render_job",
        source_id=job_id,
        estimated=_TIMELINE_RENDER_RESERVED_POINTS,
        actor_user_id=owner_user_id,
        commit=False,
    )
    input_json = {
        **request_payload,
        "output_storage_key": output_key,
        "reservation_id": reservation_id,
        "schema_version": "timeline.render.v1",
    }
    try:
        job = await timeline_crud.create_render_job(
            db,
            job_id=job_id,
            script_id=script_id,
            episode_id=episode_id,
            owner_user_id=owner_user_id,
            idempotency_key=idempotency_key,
            request_hash=req_hash,
            input_version=episode.episode_version,
            input_json=input_json,
            output_storage_key=output_key,
            reservation_id=reservation_id,
        )
    except ConflictError:
        existing = await timeline_crud.find_render_job_by_idempotency(
            db, owner_user_id=owner_user_id, idempotency_key=idempotency_key
        )
        if existing is not None and existing.request_hash == req_hash:
            return {
                "job_id": existing.job_id,
                "status": existing.status,
                "output_storage_key": existing.output_storage_key,
            }
        raise
    outbox = await enqueue_task(
        db,
        task_type="timeline.render",
        job_id=job.job_id,
        attempt=job.attempt,
        payload={"job_id": job.job_id, "script_id": script_id, "episode_id": episode_id},
    )
    job.dispatch_id = outbox.id
    await db.flush()
    return {
        "job_id": job.job_id,
        "status": job.status,
        "output_storage_key": job.output_storage_key,
    }


async def _load_render_job_or_404(
    db: AsyncSession, render_job_id: str
) -> TimelineRenderJob:
    job = await timeline_crud.get_render_job(db, render_job_id)
    if job is None:
        raise NotFoundError("渲染任务不存在")
    return job


async def get_render_status(
    render_job_id: str, db: AsyncSession, user: User
) -> dict:
    """查询渲染任务状态。不存在 404，存在但无权 403。"""

    job = await _load_render_job_or_404(db, render_job_id)
    await check_timeline_script_access(db, user, job.script_id, Action.READ)
    return timeline_crud.render_job_to_status(job)


async def cancel_render(
    render_job_id: str, db: AsyncSession, user: User
) -> dict:
    """取消渲染任务；必须具备当前剧本 RUN_AI 权限。"""

    job = await _load_render_job_or_404(db, render_job_id)
    await check_timeline_script_access(db, user, job.script_id, Action.RUN_AI)
    locked = await timeline_crud.lock_cancelable_render_job(db, render_job_id)
    if locked is None:
        existing = await timeline_crud.get_render_job(db, render_job_id)
        if existing is not None:
            return timeline_crud.render_job_to_status(existing)
        raise NotFoundError("渲染任务不存在")
    reservation_id = locked.reservation_id
    if reservation_id:
        try:
            await credits_release(
                db,
                reservation_id=reservation_id,
                reason=f"时间线合成取消 job_id={render_job_id}",
                actor_user_id=user.user_id,
                commit=False,
            )
        except Exception as exc:  # noqa: BLE001 - release 失败需留下可重试补偿
            await db.rollback()
            relocked = await timeline_crud.lock_cancelable_render_job(db, render_job_id)
            if relocked is None:
                existing = await timeline_crud.get_render_job(db, render_job_id)
                if existing is not None:
                    return timeline_crud.render_job_to_status(existing)
                raise NotFoundError("渲染任务不存在") from exc
            await timeline_crud.mark_locked_render_job_terminal(
                db,
                relocked,
                status="canceled",
                output_json={
                    **(relocked.output_json or {}),
                    "financial_cleanup": {
                        "status": "pending",
                        "action": "release",
                        "reservation_id": reservation_id,
                        "error": exc.__class__.__name__,
                    },
                },
            )
            return timeline_crud.render_job_to_status(relocked)
    await timeline_crud.mark_locked_render_job_terminal(
        db,
        locked,
        status="canceled",
    )
    return timeline_crud.render_job_to_status(locked)


async def reconcile_stale_render_jobs(
    db: AsyncSession, *, limit: int = 50
) -> int:
    """恢复可由主 outbox dispatcher 重新投递的 render job。"""

    return await timeline_crud.reconcile_stale_render_jobs(db, limit=limit)


async def retry_render(render_job_id: str, db: AsyncSession, user: User) -> dict:
    """failed + retryable 后创建新的不可变 attempt。"""

    source = await _load_render_job_or_404(db, render_job_id)
    await check_timeline_script_access(db, user, source.script_id, Action.RUN_AI)
    if source.status != "failed" or not source.retryable:
        raise ConflictError("渲染任务当前状态不可重试")
    job_id = new_id()
    output_key = _output_storage_key(job_id, source.script_id, source.episode_id)
    reservation_id = await credits_reserve(
        db,
        account_id=await _resolve_user_account_id(db, user.user_id),
        source_type="timeline_render_job",
        source_id=job_id,
        estimated=_TIMELINE_RENDER_RESERVED_POINTS,
        actor_user_id=user.user_id,
        commit=False,
    )
    job = await timeline_crud.create_retry_render_job(
        db,
        source=source,
        job_id=job_id,
        owner_user_id=user.user_id,
        idempotency_key=f"{source.idempotency_key}:retry:{int(source.attempt or 1) + 1}",
        output_storage_key=output_key,
        reservation_id=reservation_id,
    )
    outbox = await enqueue_task(
        db,
        task_type="timeline.render",
        job_id=job.job_id,
        attempt=job.attempt,
        payload={
            "job_id": job.job_id,
            "script_id": job.script_id,
            "episode_id": job.episode_id,
        },
    )
    job.dispatch_id = outbox.id
    await db.flush()
    return {
        "job_id": job.job_id,
        "status": job.status,
        "output_storage_key": job.output_storage_key,
    }
