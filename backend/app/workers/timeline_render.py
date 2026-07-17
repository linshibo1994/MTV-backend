"""时间线合成 worker — DB lease/CAS + async storage + ffmpeg。"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path

from app.core.database import async_session_factory
from app.crud import timeline as timeline_crud
from app.models.timeline_render_job import TimelineRenderJob
from app.providers.storage import get_storage
from app.services import credits
from app.workers.celery_app import app as celery_app

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
_TIMELINE_RENDER_ACTUAL_POINTS = 1
credits_release = credits.release
credits_settle = credits.settle


class _FinancialCleanupError(RuntimeError):
    error_type = "infrastructure_unavailable"
    retryable = True


def _pending_financial_cleanup(
    *,
    action: str,
    reservation_id: str | None,
    error: Exception,
) -> dict:
    return {
        "status": "pending",
        "action": action,
        "reservation_id": reservation_id,
        "error": error.__class__.__name__,
    }


def _trim_segment(
    src_path: str,
    trim_start: float,
    trim_end: float | None,
    out_path: str,
) -> None:
    """用 ffmpeg 裁剪单段。

    trim_end 为空时不加 -t，表示保留剩余时长；非空时 -t = end - start。
    """

    cmd = [FFMPEG_BIN, "-y", "-loglevel", "error"]
    start = max(float(trim_start or 0.0), 0.0)
    if start > 0:
        cmd += ["-ss", f"{start:.3f}"]
    cmd += ["-i", src_path]
    if trim_end is not None:
        duration = float(trim_end) - start
        if duration <= 0:
            raise ValueError("裁出点必须晚于裁入点")
        cmd += ["-t", f"{duration:.3f}"]
    cmd += [
        "-c:v",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        out_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True)


def _quote_concat_path(path: str) -> str:
    return path.replace("'", r"'\''")


def _concat_segments(segment_paths: list[str], out_path: str) -> None:
    """ffmpeg concat demuxer 拼接，清单只写临时目录内相对路径。"""

    if not segment_paths:
        raise ValueError("缺少可拼接的视频片段")
    base_dir = Path(out_path).parent
    list_file = base_dir / "concat.txt"
    try:
        with list_file.open("w", encoding="utf-8") as f:
            for path in segment_paths:
                rel = os.path.relpath(path, base_dir)
                if rel.startswith("..") or os.path.isabs(rel):
                    raise ValueError("concat 输入必须位于临时目录内")
                f.write(f"file '{_quote_concat_path(rel)}'\n")
        cmd = [
            FFMPEG_BIN,
            "-y",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "1",
            "-i",
            str(list_file),
            "-c",
            "copy",
            out_path,
        ]
        subprocess.run(cmd, check=True, capture_output=True)
    finally:
        list_file.unlink(missing_ok=True)


async def _load_claimed_job(job_id: str, owner: str) -> tuple[TimelineRenderJob, str] | None:
    async with async_session_factory() as db:
        claim = await timeline_crud.claim_render_job(db, job_id, owner=owner)
        if claim is None:
            await db.rollback()
            return None
        job = await timeline_crud.get_render_job(db, job_id)
        await db.commit()
        if job is None:
            return None
        return job, claim.lease_token


async def _heartbeat(job_id: str, token: str, **fields) -> bool:
    async with async_session_factory() as db:
        ok = await timeline_crud.heartbeat_render_job(db, job_id, token, **fields)
        await db.commit()
        return ok


async def _finish(
    job_id: str,
    token: str,
    *,
    status: str,
    output_storage_key: str | None = None,
    output_json: dict | None = None,
    error: Exception | None = None,
) -> bool:
    async with async_session_factory() as db:
        job = await timeline_crud.lock_current_render_job_for_finish(db, job_id, token)
        if job is None:
            await db.rollback()
            return False
        reservation_id = job.reservation_id
        actor_user_id = job.owner_user_id
        if status == "succeeded" and reservation_id:
            try:
                await credits_settle(
                    db,
                    reservation_id=reservation_id,
                    actual=_TIMELINE_RENDER_ACTUAL_POINTS,
                    actor_user_id=actor_user_id,
                    commit=False,
                )
            except Exception as exc:  # noqa: BLE001 - 成功扣费失败必须释放预约
                await db.rollback()
                return await _fail_after_settle_error(
                    job_id,
                    token,
                    reservation_id=reservation_id,
                    actor_user_id=actor_user_id,
                    settle_error=exc,
                )
        elif status in {"failed", "canceled"} and reservation_id:
            try:
                await credits_release(
                    db,
                    reservation_id=reservation_id,
                    reason=f"timeline render {status}",
                    actor_user_id=actor_user_id,
                    commit=False,
                )
            except Exception as exc:  # noqa: BLE001 - 失败/取消释放失败需记录补偿
                await db.rollback()
                return await _mark_release_cleanup_pending(
                    job_id,
                    token,
                    status=status,
                    reservation_id=reservation_id,
                    release_error=exc,
                )
        await timeline_crud.mark_locked_render_job_terminal(
            db,
            job,
            status=status,
            output_storage_key=output_storage_key,
            output_json=output_json,
            error=error,
        )
        await db.commit()
        return True


async def _fail_after_settle_error(
    job_id: str,
    token: str,
    *,
    reservation_id: str,
    actor_user_id: str | None,
    settle_error: Exception,
) -> bool:
    async with async_session_factory() as db:
        job = await timeline_crud.lock_current_render_job_for_finish(db, job_id, token)
        if job is None:
            await db.rollback()
            return False
        try:
            await credits_release(
                db,
                reservation_id=reservation_id,
                reason=f"timeline render settle failed: {settle_error.__class__.__name__}",
                actor_user_id=actor_user_id,
                commit=False,
            )
        except Exception as release_error:  # noqa: BLE001 - 留给补偿任务幂等释放
            await db.rollback()
            return await _mark_release_cleanup_pending(
                job_id,
                token,
                status="failed",
                reservation_id=reservation_id,
                release_error=release_error,
            )
        await timeline_crud.mark_locked_render_job_terminal(
            db,
            job,
            status="failed",
            error=_FinancialCleanupError("timeline render settle failed; reservation released"),
        )
        await db.commit()
        return False


async def _mark_release_cleanup_pending(
    job_id: str,
    token: str,
    *,
    status: str,
    reservation_id: str | None,
    release_error: Exception,
) -> bool:
    async with async_session_factory() as db:
        job = await timeline_crud.lock_current_render_job_for_finish(db, job_id, token)
        if job is None:
            await db.rollback()
            return False
        output_json = {
            **(job.output_json or {}),
            "financial_cleanup": _pending_financial_cleanup(
                action="release",
                reservation_id=reservation_id,
                error=release_error,
            ),
        }
        await timeline_crud.mark_locked_render_job_terminal(
            db,
            job,
            status=status,
            output_json=output_json,
            error=(
                _FinancialCleanupError("timeline render financial cleanup failed")
                if status == "failed"
                else None
            ),
        )
        await db.commit()
        return False


async def _run_one_financial_cleanup(job_id: str) -> bool:
    async with async_session_factory() as db:
        job = await timeline_crud.lock_pending_render_financial_cleanup(db, job_id)
        if job is None:
            await db.rollback()
            return False
        cleanup = (job.output_json or {}).get("financial_cleanup") or {}
        reservation_id = str(cleanup["reservation_id"])
        try:
            await credits_release(
                db,
                reservation_id=reservation_id,
                reason=f"timeline render financial cleanup job_id={job_id}",
                actor_user_id=job.owner_user_id,
                commit=False,
            )
        except Exception as exc:  # noqa: BLE001 - 单行失败不影响其他 job
            await db.rollback()
            await _record_financial_cleanup_error(job_id, exc)
            return False
        job.output_json = {
            **(job.output_json or {}),
            "financial_cleanup": {
                **cleanup,
                "status": "done",
                "last_error": None,
            },
        }
        await db.flush()
        await db.commit()
        return True


async def _record_financial_cleanup_error(job_id: str, error: Exception) -> None:
    async with async_session_factory() as db:
        job = await timeline_crud.lock_pending_render_financial_cleanup(db, job_id)
        if job is None:
            await db.rollback()
            return
        cleanup = (job.output_json or {}).get("financial_cleanup") or {}
        job.output_json = {
            **(job.output_json or {}),
            "financial_cleanup": {
                **cleanup,
                "status": "pending",
                "last_error": error.__class__.__name__,
            },
        }
        await db.flush()
        await db.commit()


async def reconcile_render_financial_cleanup(*, limit: int = 50) -> int:
    """逐行隔离地补偿 failed/canceled render job 的 reservation release。"""

    async with async_session_factory() as db:
        job_ids = await timeline_crud.list_pending_render_financial_cleanup_ids(
            db, limit=limit
        )
        await db.rollback()
    released = 0
    for cleanup_job_id in job_ids:
        try:
            if await _run_one_financial_cleanup(cleanup_job_id):
                released += 1
        except Exception:
            continue
    return released


async def _runtime_state(job_id: str, token: str) -> str:
    async with async_session_factory() as db:
        job = await timeline_crud.get_render_job(db, job_id)
        if job is None:
            return "missing"
        if job.status == "canceled":
            return "canceled"
        if job.status != "processing" or job.lease_token != token:
            return "lost_lease"
        return "active"


async def _has_current_lease(job_id: str, token: str) -> bool:
    async with async_session_factory() as db:
        return await timeline_crud.has_current_render_lease(db, job_id, token)


async def _delete_storage_keys(storage, *keys: str | None) -> None:
    for key in keys:
        if not key:
            continue
        try:
            await storage.delete(key)
        except Exception:
            pass


async def run_render_job(
    job_id: str,
    *,
    owner: str | None = None,
    temp_root: str | Path | None = None,
) -> dict:
    """执行一个 render job；供 Celery task 与单元测试复用。"""

    worker_owner = owner or f"timeline:{socket.gethostname()}:{os.getpid()}"
    claimed = await _load_claimed_job(job_id, worker_owner)
    if claimed is None:
        return {"job_id": job_id, "status": "skipped"}
    job, token = claimed
    storage = get_storage()
    tmp_dir = tempfile.mkdtemp(prefix="timeline_render_", dir=str(temp_root) if temp_root else None)
    stored_temp_key: str | None = None
    stored_key: str | None = None
    try:
        segments = list((job.input_json or {}).get("segments") or [])
        total = len(segments)
        if total == 0:
            raise ValueError("缺少可合成的时间线片段")
        trimmed_paths: list[str] = []
        for index, segment in enumerate(segments):
            state = await _runtime_state(job_id, token)
            if state == "canceled":
                return {"job_id": job_id, "status": "canceled"}
            if state != "active":
                return {"job_id": job_id, "status": "skipped"}
            source_key = str(segment["storage_key"])
            source_path = os.path.join(tmp_dir, f"src_{index:04d}.mp4")
            Path(source_path).write_bytes(await storage.get(source_key))
            trimmed_path = os.path.join(tmp_dir, f"seg_{index:04d}.mp4")
            _trim_segment(
                source_path,
                float(segment.get("trim_start_seconds", 0.0) or 0.0),
                segment.get("trim_end_seconds"),
                trimmed_path,
            )
            trimmed_paths.append(trimmed_path)
            await _heartbeat(
                job_id,
                token,
                progress=int((index + 1) / (total + 1) * 80),
                progress_stage="trimming",
                progress_message=f"已裁剪 {index + 1}/{total} 段",
            )

        state = await _runtime_state(job_id, token)
        if state == "canceled":
            return {"job_id": job_id, "status": "canceled"}
        if state != "active":
            return {"job_id": job_id, "status": "skipped"}
        final_path = os.path.join(tmp_dir, "final.mp4")
        await _heartbeat(
            job_id,
            token,
            progress=85,
            progress_stage="concatenating",
            progress_message="拼接片段中",
        )
        _concat_segments(trimmed_paths, final_path)
        state = await _runtime_state(job_id, token)
        if state == "canceled":
            return {"job_id": job_id, "status": "canceled"}
        if state != "active":
            return {"job_id": job_id, "status": "skipped"}
        await _heartbeat(
            job_id,
            token,
            progress=95,
            progress_stage="uploading",
            progress_message="上传最终视频",
        )
        final_data = Path(final_path).read_bytes()
        temp_key = f"{job.output_storage_key}.attempt-{job.attempt}.{token}.tmp"
        stored_temp_key = await storage.put(temp_key, final_data)
        if not await _has_current_lease(job_id, token):
            await _delete_storage_keys(storage, stored_temp_key, job.output_storage_key)
            return {"job_id": job_id, "status": "skipped"}
        stored_key = await storage.promote(stored_temp_key, job.output_storage_key)
        ok = await _finish(
            job_id,
            token,
            status="succeeded",
            output_storage_key=stored_key,
            output_json={"storage_key": stored_key, "segments": total},
        )
        if not ok:
            await _delete_storage_keys(storage, stored_temp_key, stored_key)
        return {"job_id": job_id, "status": "succeeded" if ok else "skipped", "storage_key": stored_key}
    except subprocess.CalledProcessError as exc:
        await _delete_storage_keys(storage, stored_temp_key, stored_key, job.output_storage_key)
        await _finish(job_id, token, status="failed", error=exc)
        return {"job_id": job_id, "status": "failed"}
    except Exception as exc:  # noqa: BLE001 - worker 顶层必须落安全失败
        await _delete_storage_keys(storage, stored_temp_key, stored_key, job.output_storage_key)
        await _finish(job_id, token, status="failed", error=exc)
        return {"job_id": job_id, "status": "failed"}
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


async def reconcile_stale_render_jobs(db, *, limit: int = 50) -> int:
    """扫描可恢复的 stale render job，由主 outbox 接线后重新投递。"""

    recovered = await timeline_crud.reconcile_stale_render_jobs(db, limit=limit)
    await db.commit()
    return recovered


@celery_app.task(name="timeline.render.reconcile_stale", bind=True, max_retries=0)
def reconcile_stale_render_jobs_task(self, limit: int = 50) -> int:
    async def _run() -> int:
        async with async_session_factory() as db:
            return await reconcile_stale_render_jobs(db, limit=limit)

    return asyncio.run(_run())


@celery_app.task(name="timeline.render.financial_cleanup", bind=True, max_retries=0)
def reconcile_render_financial_cleanup_task(self, limit: int = 50) -> int:
    return asyncio.run(reconcile_render_financial_cleanup(limit=limit))


@celery_app.task(name="timeline.render", bind=True, max_retries=0)
def render_timeline_task(self, job_id: str) -> dict:
    return asyncio.run(run_render_job(job_id))
