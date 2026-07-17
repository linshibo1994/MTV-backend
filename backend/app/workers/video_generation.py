"""视频生成 Celery worker — 调 provider → 存视频 → settle 积分 → 更新 last_frame。

流程:
1. 读 VideoGenerationJob (status=starting)
2. 取激活 provider (provider_key)
3. provider.generate_all_reference(prompt, reference_images, reference_audios, model)
   → 返回 video storage_key
4. credits.settle (actual=settings.price_video_gen)
5. 更新 job.output_json (video storage_key) + metrics
6. 从视频末尾提帧（ffmpeg）更新 shot_group.reference_frames.last_frame
7. job.status=succeeded, progress=100

失败路径:
- provider 抛错 → job.status=failed, error_message 记录
- credits.release(settings.price_video_gen_frozen) 释放冻结
- 不自动重试（用户手动重试创建新 attempt，见 PLAN 5.5）

worker 内用同步 DB session（PLAN 11: async + Celery 混用，task 内用同步引擎或 asyncio.run）。
本实现采用 asyncio.run 包裹 async 逻辑，复用 async session。
"""

import asyncio
import hashlib
import logging
import os
import subprocess
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from app.workers.celery_app import app

logger = logging.getLogger(__name__)
_LEASE_SECONDS = 1800

# 跨模块依赖
try:
    from app.services.credits import release as _credits_release
    from app.services.credits import settle as _credits_settle
except ImportError:  # pragma: no cover - T2 未就绪
    logger.warning(
        "积分服务 app.services.credits 不可导入，视频生成 settle/release 将被跳过 — "
        "成功不结算、失败不释放，计费被旁路。请尽快接入 T2 积分模块。"
    )
    _credits_settle = None  # type: ignore[assignment]
    _credits_release = None  # type: ignore[assignment]


@app.task(name="app.workers.video_generation.run_video_generation", bind=True)
def run_video_generation(self, job_id: str) -> dict:
    """视频生成任务入口（同步签名，内部 asyncio.run 执行 async 逻辑）。

    Args:
        job_id: VideoGenerationJob.job_id

    Returns:
        {"job_id": ..., "status": ..., "storage_key": ...}
    """
    try:
        result = asyncio.run(_run_video_generation_async(job_id))
        return result
    except Exception as exc:
        # 兜底：确保任务标记失败并释放积分
        logger.exception("视频生成任务异常 job_id=%s", job_id)
        error_type = _safe_error_type(exc, fallback="worker_internal_error")
        try:
            asyncio.run(_mark_failed_and_release(job_id, error_type))
        except Exception:  # noqa: BLE001
            logger.exception("标记视频生成失败状态时再次异常 job_id=%s", job_id)
        return {"job_id": job_id, "status": "failed", "error": error_type}


async def _run_video_generation_async(job_id: str) -> dict:
    """异步执行视频生成核心逻辑。"""
    from app.core.database import async_session_factory

    async with async_session_factory() as db:
        import socket

        owner = f"celery:{socket.gethostname()}:{os.getpid()}"
        await run_video_generation_once(db, job_id, owner=owner)
        job = await _get_video_job(db, job_id)
        return {
            "job_id": job_id,
            "status": getattr(job, "status", "missing"),
            "storage_key": ((getattr(job, "output_json", None) or {}).get("video_storage_key")),
        }


async def _get_video_job(db: Any, job_id: str):
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob

    return (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
        )
    ).scalar_one_or_none()


async def _input_version_is_current(db: Any, job: Any) -> bool:
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.episode import Episode

    episode = (
        await db.execute(
            select(Episode)
            .where(Episode.episode_id == job.episode_id)
            .options(noload(Episode.script), noload(Episode.scenes))
        )
    ).scalar_one_or_none()
    return bool(episode and int(episode.episode_version or 1) == int(job.input_version or 1))


def _safe_error_type(exc: Exception, fallback: str = "provider_or_storage_error") -> str:
    if isinstance(exc, TimeoutError):
        return "provider_timeout"
    return fallback


def _safe_error_message(error_type: str) -> str:
    return error_type


async def _delete_generated_output(storage_key: str | None, *, storage: Any | None = None) -> None:
    """删除未形成成功任务的视频/帧；失败时写入持久化清理任务。"""
    if not storage_key:
        return
    from app.providers.storage import get_storage

    try:
        target = storage or get_storage()
        await target.delete(storage_key)
        return
    except Exception:  # noqa: BLE001 - 删除失败必须转入持久化补偿
        logger.warning("清理失败视频产物，转入媒体存储补偿 storage_key=%s", storage_key)

    try:
        from app.core.database import async_session_factory
        from app.services.media_storage_jobs import enqueue_delete

        digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()
        async with async_session_factory() as compensation_db:
            await enqueue_delete(
                compensation_db,
                idempotency_key=f"video-delete:{digest}",
                source_key=storage_key,
            )
            await compensation_db.commit()
    except Exception:  # noqa: BLE001 - 不覆盖原始任务终态
        logger.exception("写入视频产物删除补偿失败")


async def _fail_video_with_release(
    db: Any,
    job_id: str,
    lease_token: str,
    reservation_id: str | None,
    *,
    error_type: str,
    retryable: bool,
) -> None:
    from app.crud import video as video_crud

    if not await video_crud.heartbeat_video_job(db, job_id, lease_token):
        await db.rollback()
        return
    release_failed = False
    if reservation_id and _credits_release is not None:
        try:
            await _credits_release(db, reservation_id=reservation_id, reason=error_type, commit=False)
        except Exception:
            release_failed = True
    final_error_type = "financial_cleanup" if release_failed or error_type == "financial_cleanup" else error_type
    finished = await video_crud.finish_video_job(
        db,
        job_id,
        lease_token,
        "failed",
        error_type=final_error_type,
        error_message=_safe_error_message(final_error_type),
        retryable=retryable or release_failed or final_error_type == "financial_cleanup",
    )
    if finished:
        await db.commit()
    else:
        await db.rollback()


async def run_video_generation_once(
    db: Any,
    job_id: str,
    *,
    owner: str,
    provider: Any | None = None,
    extract_last_frame: Callable[[Any, str, str, str], Any] | None = None,
    storage: Any | None = None,
) -> None:
    """执行一次视频任务；测试可注入 fake provider / 抽帧函数。"""
    from app.core.config import settings
    from app.crud import video as video_crud
    from app.providers.video_gen import VideoGenerationRequest, get_video_provider

    claim = await video_crud.claim_video_job(
        db, job_id, owner=owner, lease_seconds=_LEASE_SECONDS
    )
    await db.commit()
    if claim is None:
        return
    job = await _get_video_job(db, job_id)
    if job is None:
        return
    input_json = job.input_json or {}
    output_json = job.output_json or {}
    reservation_id = job.reservation_id or input_json.get("reservation_id")
    if not await _input_version_is_current(db, job):
        await _fail_video_with_release(
            db,
            job_id,
            claim.lease_token,
            reservation_id,
            error_type="stale_input_version",
            retryable=False,
        )
        return
    first_frame = await _resolve_execution_first_frame(db, job, input_json)
    provider_task_id = output_json.get("provider_task_id")
    provider = provider or get_video_provider(job.provider_key)
    request = VideoGenerationRequest(
        prompt=input_json.get("prompt", ""),
        reference_images=input_json.get("reference_images", []) or [],
        reference_image_names=input_json.get("reference_image_names", []) or [],
        reference_audios=input_json.get("reference_audios", []) or [],
        reference_audio_names=input_json.get("reference_audio_names", []) or [],
        model=input_json.get("model") or settings.video_gen_model or settings.seedance_model,
        job_id=job_id,
        first_frame=first_frame,
        duration_seconds=input_json.get("duration_seconds"),
        metadata={"provider_task_id": provider_task_id} if provider_task_id else {},
    )
    video_storage_key: str | None = None
    last_frame_storage_key: str | None = None
    try:
        progress_queue: asyncio.Queue[Any] = asyncio.Queue()
        progress_stop = object()
        progress_errors: list[Exception] = []

        async def _consume_progress() -> None:
            while True:
                progress = await progress_queue.get()
                try:
                    if progress is progress_stop:
                        return
                    if not progress_errors:
                        await _persist_provider_progress(
                            db, job_id, claim.lease_token, progress
                        )
                except Exception as exc:  # noqa: BLE001 - 主流程统一清理产物和积分
                    progress_errors.append(exc)
                finally:
                    progress_queue.task_done()

        progress_consumer = asyncio.create_task(_consume_progress())

        def _on_progress(progress) -> None:
            progress_queue.put_nowait(progress)

        try:
            result = await provider.generate(request, _on_progress)
            await progress_queue.join()
        finally:
            progress_queue.put_nowait(progress_stop)
            await progress_consumer
        if progress_errors:
            raise progress_errors[0]
        video_storage_key = result.video_storage_key
        if not await video_crud.heartbeat_video_job(db, job_id, claim.lease_token):
            await db.rollback()
            await _delete_generated_output(video_storage_key, storage=storage)
            return
        provider_task_id = result.provider_job_id or provider_task_id
        output_json = {
            "video_storage_key": result.video_storage_key,
            "reference_image_names": request.reference_image_names,
            "model": request.model,
        }
        if provider_task_id:
            output_json["provider_task_id"] = provider_task_id
        metrics = {
            "model": request.model,
            "provider_key": job.provider_key,
            "reference_count": len(request.reference_images),
            **(result.metrics or {}),
        }
        if provider_task_id:
            metrics["provider_task_id"] = provider_task_id
        if _credits_settle is not None and reservation_id:
            try:
                await _credits_settle(
                    db,
                    reservation_id=reservation_id,
                    actual=settings.price_video_gen,
                    commit=False,
                )
            except Exception:
                await db.rollback()
                await _delete_generated_output(video_storage_key, storage=storage)
                await _fail_video_with_release(
                    db,
                    job_id,
                    claim.lease_token,
                    reservation_id,
                    error_type="financial_cleanup",
                    retryable=True,
                )
                return
        try:
            if extract_last_frame is None:
                last_frame_storage_key = await _update_last_frame(
                    db, job.unit_id, result.video_storage_key
                )
            else:
                maybe = extract_last_frame(db, job.unit_id, result.video_storage_key, claim.lease_token)
                if hasattr(maybe, "__await__"):
                    maybe = await maybe
                if isinstance(maybe, str):
                    last_frame_storage_key = maybe
        except Exception:
            await db.rollback()
            await _delete_generated_output(video_storage_key, storage=storage)
            await _fail_video_with_release(
                db,
                job_id,
                claim.lease_token,
                reservation_id,
                error_type="frame_extraction_failed",
                retryable=False,
            )
            return
        finished = await video_crud.finish_video_job(
            db,
            job_id,
            claim.lease_token,
            "succeeded",
            output_json=output_json,
            metrics=metrics,
        )
        if not finished:
            await db.rollback()
            await _delete_generated_output(last_frame_storage_key, storage=storage)
            await _delete_generated_output(video_storage_key, storage=storage)
            return
        await db.commit()
    except Exception as exc:
        await db.rollback()
        await _delete_generated_output(last_frame_storage_key, storage=storage)
        await _delete_generated_output(video_storage_key, storage=storage)
        await _fail_video_with_release(
            db,
            job_id,
            claim.lease_token,
            reservation_id,
            error_type=_safe_error_type(exc),
            retryable=False,
        )


async def _resolve_execution_first_frame(db: Any, job: Any, input_json: dict) -> str | None:
    """执行时重新解析首帧，避免创建时快照在批量任务中陈旧。"""
    from sqlalchemy import select

    from app.models.shot_group import ShotGroup
    from app.services.video_generation import _resolve_first_frame

    shot_group = (
        await db.execute(
            select(ShotGroup).where(ShotGroup.shot_group_id == job.shot_group_id)
        )
    ).scalar_one_or_none()
    if shot_group is None:
        return input_json.get("first_frame")
    latest = await _resolve_first_frame(db, shot_group)
    return latest or input_json.get("first_frame")


async def _persist_provider_progress(db: Any, job_id: str, lease_token: str, progress: Any) -> None:
    """持久化 provider 进度和提交后 task id，供崩溃恢复续轮询。"""
    from app.crud import video as video_crud

    provider_task_id = getattr(progress, "provider_task_id", None)
    output_json = {"provider_task_id": provider_task_id} if provider_task_id else None
    metrics = {"provider_task_id": provider_task_id} if provider_task_id else None
    updated = await video_crud.update_video_job_progress(
        db,
        job_id,
        progress=getattr(progress, "progress", None),
        progress_stage=getattr(progress, "stage", None),
        progress_message=getattr(progress, "message", None),
        output_json=output_json,
        metrics=metrics,
        lease_token=lease_token,
    )
    if not updated:
        await db.rollback()
        return
    if not await video_crud.heartbeat_video_job(db, job_id, lease_token):
        await db.rollback()
        return
    await db.commit()


async def reconcile_video_financial_cleanup(db: Any, *, limit: int = 100) -> int:
    """重试视频任务失败后的积分释放清理。"""
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob

    rows = list(
        (
            await db.execute(
                select(VideoGenerationJob)
                .where(
                    VideoGenerationJob.status == "failed",
                    VideoGenerationJob.error_type == "financial_cleanup",
                    VideoGenerationJob.retryable.is_(True),
                )
                .options(noload(VideoGenerationJob.script))
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    recovered = 0
    for job in rows:
        reservation_id = job.reservation_id or (job.input_json or {}).get("reservation_id")
        if reservation_id and _credits_release is not None:
            try:
                await _credits_release(
                    db,
                    reservation_id=reservation_id,
                    reason="video financial cleanup",
                    commit=False,
                )
            except Exception:
                continue
        job.retryable = False
        recovered += 1
    await db.commit()
    return recovered


@app.task(name="app.workers.video_generation.reconcile_video_financial_cleanup")
def reconcile_video_financial_cleanup_task(limit: int = 100) -> dict:
    """Celery 可调度入口；beat 接线由共享调度配置负责。"""
    async def _run() -> int:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await reconcile_video_financial_cleanup(db, limit=limit)

    try:
        recovered = asyncio.run(_run())
        return {"recovered": recovered}
    except Exception:  # noqa: BLE001
        logger.exception("视频财务清理重试失败")
        return {"recovered": 0}


async def _mark_failed_and_release_once(db: Any, job_id: str, error_type: str) -> None:
    """锁定任务后原子释放积分并写入失败终态。"""
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.credit_reservation import CreditReservation
    from app.models.video_generation_job import VideoGenerationJob

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None or job.status in {"succeeded", "failed", "canceled"}:
        await db.rollback()
        return
    reservation_id = job.reservation_id or (job.input_json or {}).get("reservation_id")
    if not reservation_id:
        reservation_id = (
            await db.execute(
                select(CreditReservation.id).where(
                    CreditReservation.source_type == "generation_job",
                    CreditReservation.source_id == job_id,
                    CreditReservation.status == "reserved",
                )
            )
        ).scalar_one_or_none()
    release_failed = bool(reservation_id and _credits_release is None)
    if reservation_id and _credits_release is not None:
        try:
            await _credits_release(
                db,
                reservation_id=reservation_id,
                reason=error_type,
                commit=False,
            )
        except Exception:  # noqa: BLE001 - 回滚后仍需落库补偿标记
            logger.exception("视频顶层兜底释放积分失败 job_id=%s", job_id)
            await db.rollback()
            release_failed = True
            job = (
                await db.execute(
                    select(VideoGenerationJob)
                    .where(VideoGenerationJob.job_id == job_id)
                    .options(noload(VideoGenerationJob.script))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if job is None or job.status in {"succeeded", "failed", "canceled"}:
                await db.rollback()
                return
    final_error_type = "financial_cleanup" if release_failed else error_type
    job.status = "failed"
    job.progress_stage = "failed"
    job.error_type = final_error_type
    job.error_message = _safe_error_message(final_error_type)
    job.retryable = release_failed
    job.finished_at = datetime.now(UTC)
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.version += 1
    await db.commit()


async def _mark_failed_and_release(job_id: str, error_type: str) -> None:
    """兜底：标记任务失败并释放积分（task 顶层异常时调用）。"""
    from app.core.database import async_session_factory

    async with async_session_factory() as db:
        await _mark_failed_and_release_once(db, job_id, error_type)


async def _update_last_frame(db, unit_id: str, video_storage_key: str) -> str:
    """从视频末尾提取一帧，存为图片，更新 shot_group.reference_frames.last_frame。

    unit_id 形如 group:<shot_group_id>。用 ffmpeg 提取末尾帧：
      ffmpeg -sseek <duration-0.1> -i <video> -frames:v 1 <out.png>
    简化：用 ffmpeg 末尾取帧 (-sseof -0.1)。

    失败不阻断主流程（视频已生成成功）。
    """
    import re

    from app.models.shot_group import ShotGroup
    from app.providers.storage import get_storage

    match = re.match(r"group:(.+)", unit_id or "")
    if not match:
        raise RuntimeError("frame_extraction_failed")
    shot_group_id = match.group(1)

    storage = get_storage()
    try:
        video_bytes = await storage.get(video_storage_key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("提取末帧失败：无法读取视频 %s", video_storage_key)
        raise RuntimeError("frame_extraction_failed") from exc

    # 写临时文件供 ffmpeg
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as vf:
        vf.write(video_bytes)
        video_tmp = vf.name
    frame_tmp = video_tmp.replace(".mp4", "_lastframe.png")

    frame_storage_key: str | None = None
    try:
        # 取末尾帧：先探测时长，再 seek 到末尾前 0.1s
        # ffprobe 取时长
        probe = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", video_tmp,
            ],
            capture_output=True, text=True, timeout=30,
        )
        duration = 0.0
        try:
            duration = float(probe.stdout.strip() or 0)
        except ValueError:
            duration = 0.0
        seek = max(0.0, duration - 0.1)

        subprocess.run(
            [
                "ffmpeg", "-y", "-ss", str(seek), "-i", video_tmp,
                "-frames:v", "1", frame_tmp,
            ],
            capture_output=True, timeout=60,
        )
        if not os.path.exists(frame_tmp):
            raise RuntimeError("frame_extraction_failed")
        with open(frame_tmp, "rb") as f:
            frame_bytes = f.read()
        if not frame_bytes:
            raise RuntimeError("frame_extraction_failed")
        frame_storage_key = f"/generate_images/video/frames/{shot_group_id}_last.png"
        await storage.put(frame_storage_key, frame_bytes)
    except Exception as exc:  # noqa: BLE001
        if frame_storage_key is not None:
            await _delete_generated_output(frame_storage_key, storage=storage)
        logger.warning("ffmpeg 提取末帧失败 unit_id=%s", unit_id, exc_info=True)
        raise RuntimeError("frame_extraction_failed") from exc
    finally:
        for tmp in (video_tmp, frame_tmp):
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass

    if frame_storage_key is None:
        raise RuntimeError("frame_extraction_failed")

    # 更新 shot_group.reference_frames.last_frame
    from sqlalchemy import select

    sg_result = await db.execute(
        select(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id)
    )
    shot_group = sg_result.scalar_one_or_none()
    if shot_group is None:
        raise RuntimeError("frame_extraction_failed")
    ref_frames = dict(shot_group.reference_frames or {})
    ref_frames["last_frame"] = frame_storage_key
    shot_group.reference_frames = ref_frames
    return frame_storage_key


async def reconcile_stale_video_jobs(db: Any, *, limit: int = 50) -> list[str]:
    """将卡死视频任务恢复到事务 outbox，重复扫描不会重复重置。"""
    from sqlalchemy import select

    from app.crud import video as video_crud
    from app.models.task_dispatch_outbox import TaskDispatchOutbox
    from app.services.task_dispatch import enqueue_task

    current = datetime.now(UTC)
    stale = await video_crud.list_stale_processing_jobs(db, lease_seconds=_LEASE_SECONDS, limit=limit)
    recovered: list[str] = []
    for job in stale:
        changed = False
        if job.status == "processing":
            job.status = "starting"
            job.lease_owner = None
            job.lease_token = None
            job.lease_until = None
            changed = True

        outbox = (
            await db.execute(
                select(TaskDispatchOutbox).where(
                    TaskDispatchOutbox.task_type == "video.generate",
                    TaskDispatchOutbox.job_id == job.job_id,
                    TaskDispatchOutbox.attempt == job.attempt,
                )
            )
        ).scalar_one_or_none()
        if outbox is None:
            outbox = await enqueue_task(
                db,
                task_type="video.generate",
                job_id=job.job_id,
                attempt=job.attempt,
                payload={
                    "job_id": job.job_id,
                    "script_id": job.script_id,
                    "episode_id": job.episode_id,
                    "shot_group_id": job.shot_group_id,
                    "input_version": job.input_version,
                },
            )
            job.dispatch_id = outbox.id
            changed = True
        elif outbox.status == "published" or (
            outbox.status == "running"
            and (outbox.lease_until is None or outbox.lease_until <= current)
        ):
            outbox.status = "pending"
            outbox.available_at = current
            outbox.published_at = None
            outbox.lease_owner = None
            outbox.lease_token = None
            outbox.lease_until = None
            changed = True
        if changed:
            recovered.append(job.job_id)
    await db.commit()
    return recovered


@app.task(name="app.workers.video_generation.recover_stale_jobs")
def recover_stale_jobs() -> dict:
    """扫描租约过期任务并经事务 outbox 可靠重投。"""

    async def _scan() -> list[str]:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await reconcile_stale_video_jobs(db)

    try:
        resubmitted = asyncio.run(_scan())
        return {"resubmitted": resubmitted, "count": len(resubmitted)}
    except Exception:  # noqa: BLE001
        logger.exception("扫描卡死视频任务失败")
        return {"resubmitted": [], "count": 0}
