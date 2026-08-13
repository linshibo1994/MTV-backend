"""参考图画风提取后台任务。"""

import socket

from app.core.database import async_session_factory
from app.services.art_style_extraction_jobs import (
    claim_art_style_extraction,
    execute_claimed_art_style_extraction,
    fail_expired_art_style_extractions,
)
from app.workers.async_runtime import run_worker_async
from app.workers.celery_app import app as celery_app


async def _run(job_id: str, attempt: int) -> str:
    async with async_session_factory() as db:
        claim = await claim_art_style_extraction(
            db,
            job_id=job_id,
            expected_attempt=attempt,
            owner=f"{socket.gethostname()}:{__name__}",
        )
        # running 与租约必须先落库；失联后由周期任务安全标记失败，不重复调用供应商。
        await db.commit()
        if claim is None:
            return job_id
        await execute_claimed_art_style_extraction(db, claim=claim)
        await db.commit()
        return job_id


@celery_app.task(
    name="art_style.extract",
    bind=True,
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True,
)
def extract_art_style_task(self, job_id: str, attempt: int) -> str:
    del self
    return run_worker_async(_run(job_id, attempt))


async def _recover_expired() -> int:
    async with async_session_factory() as db:
        count = await fail_expired_art_style_extractions(db)
        await db.commit()
        return count


@celery_app.task(name="art_style.recover-expired", bind=True, max_retries=0)
def recover_expired_art_style_tasks(self) -> int:
    del self
    return run_worker_async(_recover_expired())
