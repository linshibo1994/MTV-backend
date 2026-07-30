"""媒体存储恢复 worker。"""

from __future__ import annotations

import logging

from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

from app.core.errors import ConflictError
from app.core.job_state import MEDIA_STORAGE_JOB_LEASE_SECONDS
from app.models.media_storage_job import MediaStorageJob
from app.providers.storage import get_storage
from app.services import media_storage_jobs
from app.workers.async_runtime import run_worker_async
from app.workers.celery_app import app

logger = logging.getLogger(__name__)


async def _rollback_execution(db) -> bool:
    try:
        await db.rollback()
        return True
    except Exception:
        logger.exception("media_storage_transaction_rollback_failed")
        return False


async def _persist_execution_error(db, claim, exc: Exception) -> None:
    if not await _rollback_execution(db):
        return
    try:
        await media_storage_jobs.mark_execution_error(db, claim, exc)
        await db.commit()
    except Exception:
        await _rollback_execution(db)
        logger.exception(
            "media_storage_error_state_persist_failed job_id=%s",
            claim.id,
        )


async def _claim_commit_is_authoritatively_succeeded(db, claim) -> bool:
    try:
        return await media_storage_jobs.is_authoritative_claim_succeeded(db, claim)
    except Exception:
        await _rollback_execution(db)
        logger.exception("media_storage_authoritative_success_read_failed job_id=%s", claim.id)
        return False


async def process_one(db, storage, *, owner: str) -> str | None:
    """领取并执行一个媒体存储任务，返回处理的 job id。"""
    claim = await media_storage_jobs.claim_next_job(
        db,
        owner=owner,
        lease_seconds=MEDIA_STORAGE_JOB_LEASE_SECONDS,
    )
    if claim is None:
        await db.rollback()
        return None
    try:
        # 领取是独立事务，禁止把 Job 行锁带入远端存储 I/O。
        await db.commit()
    except Exception:
        await _rollback_execution(db)
        raise
    if claim.status in {"failed", "effect_failed"}:
        return claim.id
    try:
        asset_id = claim.asset_id
        if claim.job_type == "promote":
            if not claim.final_key:
                raise RuntimeError("invalid media storage job")
            await media_storage_jobs.promote_with_integrity(storage, claim)
            asset_id = await media_storage_jobs.finalize_upload_ready(db, claim)
        elif claim.job_type == "delete":
            if not await media_storage_jobs.prepare_delete_effect(db, claim):
                await db.commit()
                return claim.id
            # deleting tombstone 必须先于不可回滚的远端 effect 单独持久化。
            await db.commit()
            try:
                await storage.delete(claim.source_key)
            except FileNotFoundError:
                pass
            await media_storage_jobs.finalize_upload_delete(db, claim)
        else:
            raise RuntimeError("invalid media storage job")
        marked = await media_storage_jobs.mark_succeeded(
            db,
            claim.id,
            claim.lease_token,
            asset_id=asset_id,
            expected_status="deleting" if claim.job_type == "delete" else "running",
        )
        if not marked:
            raise ConflictError("媒体存储任务 lease 已失效")
        await db.commit()
        return claim.id
    except Exception as exc:
        await _persist_execution_error(db, claim, exc)
        if await _claim_commit_is_authoritatively_succeeded(db, claim):
            return claim.id
        try:
            setattr(exc, "media_storage_job_id", claim.id)
        except Exception:
            pass
        raise


async def process_due_jobs(db, storage, *, owner: str, limit: int = 20) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {
        "succeeded": [],
        "retried": [],
        "failed": [],
        "canceled": [],
    }
    for _ in range(limit):
        try:
            job_id = await process_one(db, storage, owner=owner)
        except (SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:
            job_id = getattr(exc, "media_storage_job_id", None)
            if job_id is None:
                raise
            row = await db.get(MediaStorageJob, job_id, populate_existing=True)
            if (
                row is not None
                and row.status == "succeeded"
                and await media_storage_jobs.is_authoritative_job_succeeded(db, job_id)
            ):
                result["succeeded"].append(job_id)
                continue
            if row is None or row.status not in {
                "retry",
                "failed",
                "effect_failed",
                "canceled",
            }:
                raise
            bucket = {
                "retry": "retried",
                "failed": "failed",
                "effect_failed": "failed",
                "canceled": "canceled",
            }[row.status]
            result[bucket].append(job_id)
            continue
        if job_id is None:
            break
        row = await db.get(MediaStorageJob, job_id, populate_existing=True)
        if row is not None and row.status in {
            "retry",
            "failed",
            "effect_failed",
            "canceled",
        }:
            bucket = {
                "retry": "retried",
                "failed": "failed",
                "effect_failed": "failed",
                "canceled": "canceled",
            }[row.status]
            result[bucket].append(job_id)
        else:
            result["succeeded"].append(job_id)
    return result


@app.task(name="app.workers.media_storage.recover_media_storage_jobs")
def recover_media_storage_jobs(limit: int = 20) -> dict:
    """Celery 定时恢复入口。"""

    async def _run() -> dict[str, list[str]]:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await process_due_jobs(
                db,
                get_storage(),
                owner="media-storage-worker",
                limit=limit,
            )

    try:
        processed = run_worker_async(_run())
        return {
            "processed": processed["succeeded"],
            "count": len(processed["succeeded"]),
            "retried": processed["retried"],
            "failed": processed["failed"],
            "canceled": processed["canceled"],
        }
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception:
        logger.exception("媒体存储恢复任务失败")
        return {
            "processed": [],
            "count": 0,
            "retried": [],
            "failed": [],
            "canceled": [],
        }


@app.task(name="app.workers.media_storage.reconcile_meizi_storage_objects")
def reconcile_meizi_storage_objects(limit: int | None = None) -> dict[str, int | bool]:
    """定期重试 MeiZi 中断上传及删除失败对象；其他存储驱动安全跳过。"""

    async def _run() -> dict[str, int | bool]:
        storage = get_storage()
        reconcile = getattr(storage, "reconcile_stale_deletions", None)
        if reconcile is None:
            return {"skipped": True, "claimed": 0, "deleted": 0, "failed": 0}
        result = await reconcile(limit=limit)
        return {"skipped": False, **result}

    try:
        return run_worker_async(_run())
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception:
        logger.exception("MeiZi 媒资孤儿清理任务失败")
        return {"skipped": False, "claimed": 0, "deleted": 0, "failed": 0}
