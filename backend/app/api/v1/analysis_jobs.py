"""上传剧本自动分析任务的运行级查询接口。"""

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ConflictError, NotFoundError
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.pipeline_job import PipelineJob
from app.models.user import User
from app.schemas.script import AnalysisJobOut, AnalysisJobStageOut
from app.services.pipeline import STAGE_ORDER

router = APIRouter(prefix="/analysis-jobs", tags=["分析任务"])


@router.get("/{analysis_job_id}", response_model=AnalysisJobOut)
async def get_analysis_job(
    analysis_job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> AnalysisJobOut:
    """按运行 ID 查询上传分析任务，避免新运行覆盖旧任务的轮询视图。"""
    jobs = list(
        (
            await db.execute(
                select(PipelineJob)
                .where(PipelineJob.run_id == analysis_job_id)
                .order_by(PipelineJob.stage.asc(), PipelineJob.attempt.desc())
            )
        )
        .scalars()
        .all()
    )
    if not jobs:
        raise NotFoundError("分析任务不存在")
    script_id = jobs[0].script_id
    if any(job.script_id != script_id for job in jobs):
        raise ConflictError("分析任务数据异常")
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)

    latest: dict[str, PipelineJob] = {}
    for job in jobs:
        current = latest.get(job.stage)
        if current is None or job.attempt > current.attempt:
            latest[job.stage] = job
    stages = [
        AnalysisJobStageOut(
            stage=stage,
            status=latest[stage].status,
            error_type=latest[stage].error_type,
        )
        for stage in STAGE_ORDER
        if stage in latest
    ]
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
        status=job_status,
        current_stage=current_stage.stage if current_stage is not None else None,
        stages=stages,
        error_type=failed.error_type if failed else None,
        created_at=min(job.created_at for job in jobs),
        updated_at=max(job.updated_at for job in jobs),
    )
