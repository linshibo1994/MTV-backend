"""上传剧本自动分析任务的运行级查询接口。"""

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ConflictError, NotFoundError
from app.core.permissions import Action, check_script_access
from app.models.pipeline_job import PipelineJob
from app.models.script import Script
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.schemas.script import AnalysisJobOut, AnalysisJobStageOut
from app.services.pipeline import STAGE_ORDER
from app.services.pipeline.orchestrator import PIPELINE_DAG_TASK_TYPE

router = APIRouter(prefix="/analysis-jobs", tags=["分析任务"])


async def _load_access_script(db: AsyncSession, script_id: str) -> Script:
    """只加载权限判断所需的 Script 本行，禁止默认 selectin 级联。"""
    script = (
        await db.execute(
            select(Script)
            .where(Script.id == script_id)
            .options(noload("*"))
        )
    ).scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    return script


def _selection_from_outbox_payload(payload: object) -> tuple[str | None, str | None]:
    """仅投影分析任务可公开的模型选择，绝不返回完整 Outbox 载荷。"""

    if not isinstance(payload, dict):
        return None, None
    provider = payload.get("llm_provider")
    if not isinstance(provider, str) or not provider.strip():
        return None, None
    model = payload.get("llm_model")
    return provider.strip(), model.strip() if isinstance(model, str) and model.strip() else None


@router.get("/{analysis_job_id}", response_model=AnalysisJobOut)
async def get_analysis_job(
    analysis_job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> AnalysisJobOut:
    """按运行 ID 查询上传分析任务，避免新运行覆盖旧任务的轮询视图。"""
    jobs = (
        await db.execute(
            select(
                PipelineJob.script_id,
                PipelineJob.episode_id,
                PipelineJob.dispatch_id,
                PipelineJob.stage,
                PipelineJob.status,
                PipelineJob.attempt,
                PipelineJob.error_type,
                PipelineJob.created_at,
                PipelineJob.updated_at,
                PipelineJob.started_at,
                PipelineJob.finished_at,
            )
            .where(PipelineJob.run_id == analysis_job_id)
            .order_by(PipelineJob.stage.asc(), PipelineJob.attempt.desc())
        )
    ).all()
    if not jobs:
        raise NotFoundError("分析任务不存在")
    script_id = jobs[0].script_id
    if any(job.script_id != script_id for job in jobs):
        raise ConflictError("分析任务数据异常")
    dispatch_id = jobs[0].dispatch_id
    if any(job.dispatch_id != dispatch_id for job in jobs):
        raise ConflictError("分析任务投递数据异常")
    script = await _load_access_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    payload = (
        await db.execute(
            select(TaskDispatchOutbox.payload)
            .where(
                TaskDispatchOutbox.id == dispatch_id,
                TaskDispatchOutbox.task_type == PIPELINE_DAG_TASK_TYPE,
                TaskDispatchOutbox.job_id == analysis_job_id,
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    provider, model = _selection_from_outbox_payload(payload)

    latest: dict[str, Any] = {}
    for job in jobs:
        current = latest.get(job.stage)
        if current is None or job.attempt > current.attempt:
            latest[job.stage] = job
    stages: list[AnalysisJobStageOut] = []
    for stage in STAGE_ORDER:
        if stage not in latest:
            continue
        job = latest[stage]
        started_at = getattr(job, "started_at", None)
        finished_at = getattr(job, "finished_at", None)
        duration_ms = (
            max(0, int((finished_at - started_at).total_seconds() * 1000))
            if started_at is not None and finished_at is not None
            else None
        )
        stages.append(
            AnalysisJobStageOut(
                stage=stage,
                status=job.status,
                attempt=job.attempt,
                error_type=job.error_type,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
            )
        )
    statuses = {stage.status for stage in stages}
    failed = next((stage for stage in stages if stage.status == "failed"), None)
    active = next((stage for stage in stages if stage.status in {"running", "ready"}), None)
    pending = next((stage for stage in stages if stage.status == "pending"), None)
    current_stage = active or pending
    if failed is not None:
        job_status = "failed"
    elif statuses and statuses == {"done"}:
        job_status = "succeeded"
    elif statuses and statuses <= {"skipped"}:
        job_status = "superseded"
    elif active is not None:
        job_status = "running"
    else:
        job_status = "queued"
    return AnalysisJobOut(
        id=analysis_job_id,
        script_id=script_id,
        source_episode_id=jobs[0].episode_id,
        provider=provider,
        model=model,
        status=job_status,
        current_stage=current_stage.stage if current_stage is not None else None,
        stages=stages,
        error_type=failed.error_type if failed else None,
        created_at=min(job.created_at for job in jobs),
        updated_at=max(job.updated_at for job in jobs),
    )
