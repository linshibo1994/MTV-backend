"""媒体存储恢复 worker。"""

from __future__ import annotations

import asyncio
import logging

from app.providers.storage import get_storage
from app.services import media_storage_jobs
from app.workers.celery_app import app

logger = logging.getLogger(__name__)


async def process_one(db, storage, *, owner: str) -> str | None:
    """领取并执行一个媒体存储任务，返回处理的 job id。"""
    claim = await media_storage_jobs.claim_next_job(db, owner=owner, lease_seconds=300)
    if claim is None:
        return None
    try:
        if claim.job_type == "promote":
            if not claim.final_key:
                raise RuntimeError("invalid media storage job")
            await storage.promote(claim.source_key, claim.final_key)
        elif claim.job_type == "delete":
            await storage.delete(claim.source_key)
        else:
            raise RuntimeError("invalid media storage job")
        await media_storage_jobs.mark_succeeded(db, claim.id, claim.lease_token)
        await db.commit()
        return claim.id
    except Exception as exc:
        await media_storage_jobs.mark_retry(db, claim.id, claim.lease_token, exc)
        await db.commit()
        raise


async def process_due_jobs(db, storage, *, owner: str, limit: int = 20) -> list[str]:
    processed: list[str] = []
    for _ in range(limit):
        job_id = await process_one(db, storage, owner=owner)
        if job_id is None:
            break
        processed.append(job_id)
    return processed


@app.task(name="app.workers.media_storage.recover_media_storage_jobs")
def recover_media_storage_jobs(limit: int = 20) -> dict:
    """Celery 定时恢复入口。"""

    async def _run() -> list[str]:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await process_due_jobs(
                db,
                get_storage(),
                owner="media-storage-worker",
                limit=limit,
            )

    try:
        processed = asyncio.run(_run())
        return {"processed": processed, "count": len(processed)}
    except Exception:
        logger.exception("媒体存储恢复任务失败")
        return {"processed": [], "count": 0}
