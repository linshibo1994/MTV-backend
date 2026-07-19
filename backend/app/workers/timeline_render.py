"""时间线合成 worker — DB lease/CAS + async storage + ffmpeg。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import shutil
import socket
import subprocess
import tempfile
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select

from app.core.config import settings
from app.core.database import async_session_factory
from app.crud import timeline as timeline_crud
from app.models.media_storage_job import MediaStorageJob
from app.models.timeline_render_job import TimelineRenderJob
from app.providers.storage import get_storage, validate_key_prefix
from app.services import credits
from app.workers.async_runtime import run_worker_async
from app.workers.celery_app import app as celery_app

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
_TIMELINE_RENDER_ACTUAL_POINTS = 1
_STORAGE_CHUNK_SIZE = 1024 * 1024
_DELETE_IDEMPOTENCY_PREFIX = "timeline-render-delete:"
_DELETE_INTENT_DELAY = timedelta(hours=24)
_FFMPEG_POLL_SECONDS = 1.0
_FFMPEG_HEARTBEAT_SECONDS = 60.0
_FFMPEG_TERMINATE_SECONDS = 5.0
credits_release = credits.release
credits_settle = credits.settle
logger = logging.getLogger(__name__)


class _FinancialCleanupError(RuntimeError):
    error_type = "infrastructure_unavailable"
    retryable = True


class _FfmpegStateChanged(RuntimeError):
    def __init__(self, state: str) -> None:
        super().__init__("timeline_ffmpeg_state_changed")
        self.state = state


async def _spawn_ffmpeg_process(command: list[str]) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


async def _terminate_ffmpeg_process(
    process: Any,
    wait_task: asyncio.Task[int],
) -> None:
    terminate_failed = False
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        except BaseException as exc:  # 清理错误不得覆盖原任务异常或取消
            terminate_failed = True
            logger.warning(
                "终止 ffmpeg 失败 error_type=%s",
                exc.__class__.__name__,
            )

    if terminate_failed and process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        except BaseException as exc:  # 清理错误不得覆盖原任务异常或取消
            logger.warning(
                "强制终止 ffmpeg 失败 error_type=%s",
                exc.__class__.__name__,
            )

    try:
        await asyncio.wait_for(asyncio.shield(wait_task), timeout=_FFMPEG_TERMINATE_SECONDS)
        return
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        pass
    except BaseException as exc:  # process.wait 异常也不能覆盖原任务异常
        logger.warning(
            "等待 ffmpeg 退出失败 error_type=%s",
            exc.__class__.__name__,
        )
        return

    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        except BaseException as exc:  # 清理错误不得覆盖原任务异常或取消
            logger.warning(
                "强制终止 ffmpeg 失败 error_type=%s",
                exc.__class__.__name__,
            )

    try:
        await asyncio.wait_for(asyncio.shield(wait_task), timeout=_FFMPEG_TERMINATE_SECONDS)
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        logger.error("ffmpeg 未在终止超时内退出")
        wait_task.cancel()
        await asyncio.gather(wait_task, return_exceptions=True)
    except BaseException as exc:  # process.wait 异常也不能覆盖原任务异常
        logger.warning(
            "等待 ffmpeg 退出失败 error_type=%s",
            exc.__class__.__name__,
        )


def _assert_ffmpeg_output_within_limit(
    output_path: str,
    output_limit_bytes: int,
) -> None:
    try:
        size = os.path.getsize(output_path)
    except FileNotFoundError:
        return
    if size > output_limit_bytes:
        raise ValueError("ffmpeg 输出文件过大")


async def _run_ffmpeg_command(
    command: list[str],
    *,
    output_path: str,
    job_id: str,
    token: str,
    heartbeat_interval: float = _FFMPEG_HEARTBEAT_SECONDS,
    poll_interval: float = _FFMPEG_POLL_SECONDS,
    output_limit_bytes: int | None = None,
) -> None:
    """运行可回收 ffmpeg，并在长步骤中续租、检查取消及限制输出。"""

    effective_output_limit = (
        int(settings.max_media_read_bytes)
        if output_limit_bytes is None
        else output_limit_bytes
    )
    if effective_output_limit <= 0:
        raise ValueError("ffmpeg 输出文件过大")
    process = await _spawn_ffmpeg_process(command)
    wait_task = asyncio.create_task(process.wait())
    loop = asyncio.get_running_loop()
    next_heartbeat = loop.time() + heartbeat_interval
    try:
        while not wait_task.done():
            done, _pending = await asyncio.wait({wait_task}, timeout=poll_interval)
            if done:
                break
            try:
                _assert_ffmpeg_output_within_limit(output_path, effective_output_limit)
            except ValueError:
                await _terminate_ffmpeg_process(process, wait_task)
                raise
            state = await _runtime_state(job_id, token)
            if state != "active":
                await _terminate_ffmpeg_process(process, wait_task)
                raise _FfmpegStateChanged(state)
            if loop.time() < next_heartbeat:
                continue
            if not await _heartbeat(job_id, token):
                await _terminate_ffmpeg_process(process, wait_task)
                raise _FfmpegStateChanged("lost_lease")
            next_heartbeat = loop.time() + heartbeat_interval
        returncode = await wait_task
        _assert_ffmpeg_output_within_limit(output_path, effective_output_limit)
        if returncode != 0:
            raise subprocess.CalledProcessError(
                returncode,
                [command[0], "<redacted>"],
            )
    except asyncio.CancelledError:
        await _await_cleanup_operation(_terminate_ffmpeg_process(process, wait_task))
        raise
    except BaseException:
        await _await_cleanup_operation(_terminate_ffmpeg_process(process, wait_task))
        raise


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


async def _trim_segment(
    src_path: str,
    trim_start: float,
    trim_end: float | None,
    out_path: str,
    *,
    job_id: str,
    token: str,
    output_limit_bytes: int | None = None,
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
    await _run_ffmpeg_command(
        cmd,
        output_path=out_path,
        job_id=job_id,
        token=token,
        output_limit_bytes=output_limit_bytes,
    )


def _quote_concat_path(path: str) -> str:
    return path.replace("'", r"'\''")


async def _concat_segments(
    segment_paths: list[str],
    out_path: str,
    *,
    job_id: str,
    token: str,
    output_limit_bytes: int | None = None,
) -> None:
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
        await _run_ffmpeg_command(
            cmd,
            output_path=out_path,
            job_id=job_id,
            token=token,
            output_limit_bytes=output_limit_bytes,
        )
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
    delete_intent_keys: tuple[str, ...] = (),
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
        if status == "succeeded" and delete_intent_keys:
            await _cancel_delete_intents(db, delete_intent_keys)
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


async def _authoritative_attempt_succeeded(
    job_id: str,
    attempt_key: str,
) -> bool | None:
    """用新会话消解终态提交结果；读取失败时返回不确定，禁止破坏性清理。"""

    try:
        async with async_session_factory() as db:
            result = await db.execute(
                select(TimelineRenderJob)
                .where(TimelineRenderJob.job_id == job_id)
                .with_for_update()
            )
            job = result.scalar_one_or_none()
    except Exception as exc:  # noqa: BLE001 - 不确定终态必须 fail closed
        logger.warning(
            "核验时间线终态失败 error_type=%s",
            exc.__class__.__name__,
        )
        return None
    return bool(
        job is not None
        and job.status == "succeeded"
        and job.output_storage_key == attempt_key
    )


async def _await_cleanup_operation(operation: Coroutine[Any, Any, Any]) -> Any:
    """吞掉调用方取消并等待独立清理 task；组件自身取消交由补偿策略处理。"""

    task = asyncio.create_task(operation)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            while current is not None and current.cancelling():
                current.uncancel()
            if task.done():
                return task.result()


async def _await_storage_operation(operation: Coroutine[Any, Any, Any]) -> Any:
    """屏蔽调用方取消直至对象 I/O 确定结束，再恢复取消语义。"""

    task = asyncio.create_task(operation)
    caller_cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                caller_cancelled = True
                while current.cancelling():
                    current.uncancel()
                continue
            if caller_cancelled:
                raise asyncio.CancelledError from None
            raise
        except Exception:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                caller_cancelled = True
                while current.cancelling():
                    current.uncancel()
            if caller_cancelled:
                raise asyncio.CancelledError from None
            raise
        if caller_cancelled:
            raise asyncio.CancelledError
        return result


def _delete_intent_key(storage_key: str) -> str:
    digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()
    return f"{_DELETE_IDEMPOTENCY_PREFIX}{digest}"


async def _stage_delete_intents(*storage_keys: str) -> tuple[str, ...]:
    """对象写入前持久化延迟删除意图，进程失联后仍能收敛孤儿。"""

    from app.services.media_storage_jobs import enqueue_delete

    normalized_keys = tuple(dict.fromkeys(validate_key_prefix(key) for key in storage_keys))
    intent_keys = tuple(_delete_intent_key(key) for key in normalized_keys)
    available_at = datetime.now(UTC) + _DELETE_INTENT_DELAY
    async with async_session_factory() as db:
        for normalized_key, intent_key in zip(normalized_keys, intent_keys, strict=True):
            await enqueue_delete(
                db,
                idempotency_key=intent_key,
                source_key=normalized_key,
                available_at=available_at,
            )
        await db.commit()
    return intent_keys


async def _cancel_delete_intents(db: Any, intent_keys: tuple[str, ...]) -> None:
    """在成功终态事务内取消对应的延迟删除意图。"""

    rows = list(
        (
            await db.execute(
                select(MediaStorageJob)
                .where(
                    MediaStorageJob.job_type == "delete",
                    MediaStorageJob.idempotency_key.in_(intent_keys),
                )
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    if {row.idempotency_key for row in rows} != set(intent_keys):
        raise RuntimeError("timeline_delete_intent_missing")
    if any(row.status not in {"pending", "retry", "canceled"} for row in rows):
        raise RuntimeError("timeline_delete_intent_state_conflict")
    completed_at = datetime.now(UTC)
    for row in rows:
        if row.status == "canceled":
            continue
        row.status = "canceled"
        row.lease_owner = None
        row.lease_token = None
        row.lease_until = None
        row.completed_at = completed_at
    await db.flush()


async def _activate_delete_intents(intent_keys: tuple[str, ...]) -> bool:
    """失败/失租时将预置删除意图立即到期；错误只记录安全类型。"""

    if not intent_keys:
        return True
    try:
        async with async_session_factory() as db:
            rows = list(
                (
                    await db.execute(
                        select(MediaStorageJob)
                        .where(
                            MediaStorageJob.job_type == "delete",
                            MediaStorageJob.idempotency_key.in_(intent_keys),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if {row.idempotency_key for row in rows} != set(intent_keys):
                await db.rollback()
                logger.error(
                    "激活时间线删除意图失败 error_type=DeleteIntentMissing intent_count=%s",
                    len(intent_keys),
                )
                return False
            available_at = datetime.now(UTC)
            for row in rows:
                if row.status not in {"pending", "retry"}:
                    continue
                row.status = "pending"
                row.available_at = available_at
                row.lease_owner = None
                row.lease_token = None
                row.lease_until = None
                row.completed_at = None
            await db.flush()
            await db.commit()
        return True
    except Exception as exc:  # noqa: BLE001 - durable intent 已预置，日志不得泄露 SQL 参数
        logger.error(
            "激活时间线删除意图失败 error_type=%s intent_count=%s",
            exc.__class__.__name__,
            len(intent_keys),
        )
        return False


async def _cleanup_attempt_objects(
    storage: Any,
    intent_keys: tuple[str, ...],
    *storage_keys: str | None,
) -> None:
    try:
        await _await_cleanup_operation(_activate_delete_intents(intent_keys))
    except asyncio.CancelledError:
        logger.warning("激活时间线删除意图被组件取消 intent_count=%s", len(intent_keys))
    await _delete_storage_keys(storage, *storage_keys)


async def _persist_delete_compensation(normalized_key: str, digest: str) -> None:
    from app.services.media_storage_jobs import enqueue_delete

    async with async_session_factory() as db:
        base_intent_key = f"{_DELETE_IDEMPOTENCY_PREFIX}{digest}"
        generation = 0
        while True:
            intent_key = (
                base_intent_key
                if generation == 0
                else f"{base_intent_key}:retry:{generation}"
            )
            intent = await enqueue_delete(
                db,
                idempotency_key=intent_key,
                source_key=normalized_key,
                available_at=datetime.now(UTC),
            )
            # enqueue_delete 的首次查询不加锁；重读权威状态，避免覆盖刚被领取的 lease。
            if isinstance(intent, MediaStorageJob):
                intent = (
                    await db.execute(
                        select(MediaStorageJob)
                        .where(MediaStorageJob.idempotency_key == intent_key)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one()

            status = getattr(intent, "status", "pending")
            if status in {"pending", "retry"}:
                intent.status = "pending"
                intent.available_at = datetime.now(UTC)
                intent.lease_owner = None
                intent.lease_token = None
                intent.lease_until = None
                intent.completed_at = None
                break
            if status == "failed":
                # 保留失败终态及审计字段，在确定性的新代任务上重新承担删除债务。
                generation += 1
                continue
            if status in {"running", "succeeded", "canceled"}:
                # canceled 是成功渲染对删除意图的撤销，不得再次创建删除补偿。
                break
            raise RuntimeError("timeline_delete_compensation_state_invalid")
        await db.commit()


async def _enqueue_delete_compensation(normalized_key: str, digest: str) -> None:
    for attempt in range(2):
        try:
            await _await_cleanup_operation(
                _persist_delete_compensation(normalized_key, digest)
            )
            return
        except asyncio.CancelledError:
            if attempt == 0:
                logger.warning(
                    "时间线对象删除补偿被组件取消，使用新会话重试 key_sha256=%s",
                    digest,
                )
                continue
            logger.error(
                "时间线对象删除补偿连续被组件取消 key_sha256=%s",
                digest,
            )
            return
        except Exception as exc:  # noqa: BLE001 - 补偿失败只记录，不覆盖原任务状态或异常
            logger.error(
                "写入时间线对象删除补偿失败 error_type=%s key_sha256=%s",
                exc.__class__.__name__,
                digest,
            )
            return


async def _delete_storage_keys(storage, *keys: str | None) -> None:
    seen: set[str] = set()
    for key in keys:
        if not key:
            continue
        try:
            normalized_key = validate_key_prefix(key)
        except Exception:  # noqa: BLE001 - 清理失败不得覆盖原任务状态
            logger.warning("跳过非法的时间线清理对象 key")
            continue
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        try:
            await _await_cleanup_operation(storage.delete(normalized_key))
            continue
        except asyncio.CancelledError:
            logger.warning("时间线 attempt 对象删除被组件取消")
        except Exception:  # noqa: BLE001 - 删除失败转入持久化补偿
            logger.warning(
                "时间线 attempt 对象删除失败，转入持久化补偿"
            )

        digest = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()
        await _enqueue_delete_compensation(normalized_key, digest)


async def _download_storage_file(storage, storage_key: str, local_path: str) -> None:
    """生产存储分块下载；旧测试 fake 缺少流式方法时保持兼容。"""

    limit = int(settings.max_media_read_bytes)
    get_size = getattr(storage, "get_size", None)
    if callable(get_size):
        reported_size = int(await get_size(storage_key))
        if reported_size <= 0:
            raise ValueError("媒体文件为空")
        if reported_size > limit:
            raise ValueError("媒体文件过大")

    try:
        iter_chunks = getattr(storage, "iter_chunks", None)
        if not callable(iter_chunks):
            data = await storage.get(storage_key)
            if not data:
                raise ValueError("媒体文件为空")
            if len(data) > limit:
                raise ValueError("媒体文件过大")
            await asyncio.to_thread(Path(local_path).write_bytes, data)
            return

        total = 0
        with open(local_path, "wb") as output:
            async for chunk in iter_chunks(
                storage_key,
                chunk_size=_STORAGE_CHUNK_SIZE,
                start=0,
                end=None,
            ):
                if not chunk:
                    continue
                total += len(chunk)
                if total > limit:
                    raise ValueError("媒体文件过大")
                await asyncio.to_thread(output.write, chunk)
        if total == 0:
            raise ValueError("媒体文件为空")
    except Exception:
        Path(local_path).unlink(missing_ok=True)
        raise


async def _upload_storage_file(storage, storage_key: str, local_path: str) -> str:
    """生产存储从文件上传；旧测试 fake 缺少流式方法时保持兼容。"""

    size = await asyncio.to_thread(os.path.getsize, local_path)
    if size <= 0:
        raise ValueError("媒体文件为空")
    if size > int(settings.max_media_read_bytes):
        raise ValueError("媒体文件过大")
    put_file = getattr(storage, "put_file", None)
    if callable(put_file):
        return await _await_storage_operation(put_file(storage_key, local_path))
    data = await asyncio.to_thread(Path(local_path).read_bytes)
    return await _await_storage_operation(storage.put(storage_key, data))


def _storage_attempt_id(lease_token: str) -> str:
    return hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]


def _attempt_output_key(base_key: str, attempt: int, attempt_id: str) -> str:
    stem, extension = os.path.splitext(base_key)
    return f"{stem}.attempt-{attempt}-{attempt_id}{extension or '.mp4'}"


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
    temp_key: str | None = None
    attempt_key: str | None = None
    delete_intent_keys: tuple[str, ...] = ()
    try:
        segments = list((job.input_json or {}).get("segments") or [])
        total = len(segments)
        if total == 0:
            raise ValueError("缺少可合成的时间线片段")
        trimmed_paths: list[str] = []
        trimmed_total_bytes = 0
        for index, segment in enumerate(segments):
            state = await _runtime_state(job_id, token)
            if state == "canceled":
                return {"job_id": job_id, "status": "canceled"}
            if state != "active":
                return {"job_id": job_id, "status": "skipped"}
            source_key = str(segment["storage_key"])
            source_path = os.path.join(tmp_dir, f"src_{index:04d}.mp4")
            await _download_storage_file(storage, source_key, source_path)
            trimmed_path = os.path.join(tmp_dir, f"seg_{index:04d}.mp4")
            try:
                await _trim_segment(
                    source_path,
                    float(segment.get("trim_start_seconds", 0.0) or 0.0),
                    segment.get("trim_end_seconds"),
                    trimmed_path,
                    job_id=job_id,
                    token=token,
                    output_limit_bytes=int(settings.max_media_read_bytes)
                    - trimmed_total_bytes,
                )
            finally:
                Path(source_path).unlink(missing_ok=True)
            trimmed_size = await asyncio.to_thread(os.path.getsize, trimmed_path)
            if trimmed_size <= 0:
                raise ValueError("裁剪后的视频片段为空")
            trimmed_total_bytes += trimmed_size
            if trimmed_total_bytes > int(settings.max_media_read_bytes):
                raise ValueError("时间线临时片段总大小超过限制")
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
        await _concat_segments(
            trimmed_paths,
            final_path,
            job_id=job_id,
            token=token,
            output_limit_bytes=int(settings.max_media_read_bytes),
        )
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
        attempt_id = _storage_attempt_id(token)
        attempt_key = validate_key_prefix(
            _attempt_output_key(job.output_storage_key, job.attempt, attempt_id)
        )
        temp_key = validate_key_prefix(f"{attempt_key}.tmp")
        delete_intent_keys = (
            _delete_intent_key(temp_key),
            _delete_intent_key(attempt_key),
        )
        staged_intent_keys = await _stage_delete_intents(temp_key, attempt_key)
        if staged_intent_keys != delete_intent_keys:
            raise RuntimeError("timeline_delete_intent_mismatch")
        # put_file 可能在远端写入后才抛错，预先记录 key 才能在异常路径清理部分对象。
        stored_temp_key = temp_key
        returned_temp_key = await _upload_storage_file(storage, temp_key, final_path)
        if validate_key_prefix(returned_temp_key) != stored_temp_key:
            raise RuntimeError("storage_adapter_key_mismatch")
        if not await _has_current_lease(job_id, token):
            await _cleanup_attempt_objects(
                storage,
                delete_intent_keys,
                stored_temp_key,
                attempt_key,
            )
            return {"job_id": job_id, "status": "skipped"}
        # CAS 前只写本租约的 attempt key，失租 worker 永远不触碰共享基准 key。
        stored_key = attempt_key
        returned_key = await _await_storage_operation(
            storage.promote(stored_temp_key, stored_key)
        )
        if validate_key_prefix(returned_key) != stored_key:
            raise RuntimeError("storage_adapter_key_mismatch")
        try:
            ok = await _finish(
                job_id,
                token,
                status="succeeded",
                output_storage_key=stored_key,
                output_json={"storage_key": stored_key, "segments": total},
                delete_intent_keys=delete_intent_keys,
            )
        except Exception:
            authoritative = await _authoritative_attempt_succeeded(job_id, stored_key)
            if authoritative is True:
                return {"job_id": job_id, "status": "succeeded", "storage_key": stored_key}
            if authoritative is None:
                return {"job_id": job_id, "status": "skipped"}
            raise
        if not ok:
            authoritative = await _authoritative_attempt_succeeded(job_id, stored_key)
            if authoritative is True:
                return {"job_id": job_id, "status": "succeeded", "storage_key": stored_key}
            if authoritative is None:
                return {"job_id": job_id, "status": "skipped"}
            await _cleanup_attempt_objects(
                storage,
                delete_intent_keys,
                stored_temp_key,
                stored_key,
            )
        return {"job_id": job_id, "status": "succeeded" if ok else "skipped", "storage_key": stored_key}
    except _FfmpegStateChanged as exc:
        return {
            "job_id": job_id,
            "status": "canceled" if exc.state == "canceled" else "skipped",
        }
    except asyncio.CancelledError:
        if attempt_key is not None:
            authoritative = await _await_cleanup_operation(
                _authoritative_attempt_succeeded(job_id, attempt_key)
            )
            if authoritative is False:
                await _cleanup_attempt_objects(
                    storage,
                    delete_intent_keys,
                    temp_key,
                    attempt_key,
                )
        raise
    except subprocess.CalledProcessError as exc:
        if attempt_key is not None:
            authoritative = await _authoritative_attempt_succeeded(job_id, attempt_key)
            if authoritative is True:
                return {"job_id": job_id, "status": "succeeded", "storage_key": attempt_key}
            if authoritative is None:
                return {"job_id": job_id, "status": "skipped"}
            await _cleanup_attempt_objects(
                storage,
                delete_intent_keys,
                temp_key,
                attempt_key,
            )
        await _finish(job_id, token, status="failed", error=exc)
        return {"job_id": job_id, "status": "failed"}
    except Exception as exc:  # noqa: BLE001 - worker 顶层必须落安全失败
        if attempt_key is not None:
            authoritative = await _authoritative_attempt_succeeded(job_id, attempt_key)
            if authoritative is True:
                return {"job_id": job_id, "status": "succeeded", "storage_key": attempt_key}
            if authoritative is None:
                return {"job_id": job_id, "status": "skipped"}
            await _cleanup_attempt_objects(
                storage,
                delete_intent_keys,
                temp_key,
                attempt_key,
            )
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

    return run_worker_async(_run())


@celery_app.task(name="timeline.render.financial_cleanup", bind=True, max_retries=0)
def reconcile_render_financial_cleanup_task(self, limit: int = 50) -> int:
    return run_worker_async(reconcile_render_financial_cleanup(limit=limit))


@celery_app.task(name="timeline.render", bind=True, max_retries=0)
def render_timeline_task(self, job_id: str) -> dict:
    return run_worker_async(run_render_job(job_id))
