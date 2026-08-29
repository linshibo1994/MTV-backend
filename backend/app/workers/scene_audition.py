"""Coverage Audition 后台任务。"""

from app.core.database import async_session_factory
from app.services.pipeline_context import LLMSelection, llm_selection_scope
from app.services.scene_auditions import (
    claim_queued_audition,
    fail_expired_auditions,
    run_queued_audition,
)
from app.workers.async_runtime import run_worker_async
from app.workers.celery_app import app as celery_app


async def _run(
    audition_id: str,
    attempt: int | None = None,
    llm_provider: str | None = None,
    llm_model: str | None = None,
) -> str:
    # 请求级 LLM 选择来自任务 outbox payload；恢复为 contextvar 供
    # run_queued_audition 内的 complete_json 动态路由。
    selection = (
        LLMSelection(provider=llm_provider, model=llm_model)
        if (llm_provider or "").strip() or (llm_model or "").strip()
        else None
    )
    with llm_selection_scope(selection):
        async with async_session_factory() as db:
            row, should_generate = await claim_queued_audition(
                db,
                audition_id=audition_id,
                expected_attempt=attempt,
            )
            # 先单独提交 running，Worker 丢失时 API 才能识别并恢复超时任务。
            await db.commit()
            if not should_generate:
                return row.id if row is not None else audition_id
            if attempt is None:
                row = await run_queued_audition(
                    db,
                    audition_id=audition_id,
                    allow_running=True,
                )
            else:
                row = await run_queued_audition(
                    db,
                    audition_id=audition_id,
                    expected_attempt=attempt,
                    allow_running=True,
                )
            await db.commit()
            return row.id if row is not None else audition_id


@celery_app.task(
    name="scene_audition.generate",
    bind=True,
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True,
)
def generate_scene_audition_task(
    self,
    audition_id: str,
    attempt: int | None = None,
    llm_provider: str | None = None,
    llm_model: str | None = None,
) -> str:
    del self
    return run_worker_async(
        _run(audition_id, attempt, llm_provider, llm_model)
    )


async def _recover_expired() -> int:
    async with async_session_factory() as db:
        count = await fail_expired_auditions(db)
        await db.commit()
        return count


@celery_app.task(name="scene_audition.recover-expired", bind=True, max_retries=0)
def recover_expired_scene_auditions(self) -> int:
    del self
    return run_worker_async(_recover_expired())
