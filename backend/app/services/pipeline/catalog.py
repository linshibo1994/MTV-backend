"""主体目录与流水线运行的关联状态。"""

from collections import defaultdict
from datetime import datetime
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.pipeline_job import PipelineJob
from app.models.subject import Subject
from app.services.pipeline import STAGE_ORDER

CatalogState = Literal["current", "processing", "failed", "superseded", "untracked"]


def _latest_stage_jobs(jobs: list[PipelineJob]) -> dict[str, PipelineJob]:
    latest: dict[str, PipelineJob] = {}
    for job in jobs:
        current = latest.get(job.stage)
        if current is None or job.attempt > current.attempt:
            latest[job.stage] = job
    return latest


def _run_state(latest: dict[str, PipelineJob]) -> tuple[CatalogState, str | None, str | None]:
    statuses = {stage: latest[stage].status for stage in STAGE_ORDER if stage in latest}
    if statuses and all(status == "skipped" for status in statuses.values()):
        return "superseded", None, None
    core_stages = ("split", "extract", "merge")
    failed_core = next(
        (stage for stage in core_stages if statuses.get(stage) == "failed"), None
    )
    if failed_core is not None:
        return "failed", failed_core, latest[failed_core].error_type
    if all(statuses.get(stage) == "done" for stage in core_stages):
        merge_output = getattr(latest["merge"], "output_json", None)
        merge_published = isinstance(merge_output, dict) and merge_output.get(
            "subjects_published"
        ) is True
        leaf_stages = tuple(stage for stage in STAGE_ORDER if stage not in core_stages)
        if all(statuses.get(stage) == "skipped" for stage in leaf_stages):
            return "superseded", None, None
        # 部署前完成的运行没有发布标记，但两个叶子都完成意味着旧 finalizer 已发布。
        legacy_completed = all(statuses.get(stage) == "done" for stage in leaf_stages)
        failed_leaf = next(
            (
                stage
                for stage in leaf_stages
                if statuses.get(stage) == "failed"
            ),
            None,
        )
        if merge_published or legacy_completed:
            return (
                "current",
                failed_leaf,
                latest[failed_leaf].error_type if failed_leaf is not None else None,
            )
        if failed_leaf is not None:
            return "failed", failed_leaf, latest[failed_leaf].error_type
        if any(
            statuses.get(stage) in {"pending", "retry", "running", "ready"}
            for stage in leaf_stages
        ):
            return "processing", None, None
    failed = next((stage for stage in STAGE_ORDER if statuses.get(stage) == "failed"), None)
    if failed is not None:
        return "failed", failed, latest[failed].error_type
    if any(status in {"pending", "retry", "running", "ready"} for status in statuses.values()):
        return "processing", None, None
    return "untracked", None, None


async def get_subject_catalog_state(db: AsyncSession, script_id: str) -> dict[str, object]:
    """返回主体是否来自最新流水线，避免历史 subjects 被误认为本轮产物。"""
    jobs = list(
        (
            await db.execute(
                select(PipelineJob).where(PipelineJob.script_id == script_id)
            )
        )
        .scalars()
        .all()
    )
    subject_count = int(
        (
            await db.execute(
                select(func.count(Subject.id)).where(
                    Subject.script_id == script_id,
                    Subject.is_active.is_(True),
                )
            )
        ).scalar()
        or 0
    )
    if not jobs:
        return {
            "state": "untracked",
            "is_current": False,
            "latest_run_id": None,
            "published_run_id": None,
            "failure_stage": None,
            "error_type": None,
            "stale_subject_count": 0,
        }

    by_run: dict[str, list[PipelineJob]] = defaultdict(list)
    for job in jobs:
        by_run[job.run_id].append(job)
    latest_run_id, latest_jobs = max(
        by_run.items(),
        key=lambda item: max(job.created_at for job in item[1]),
    )
    latest_state, failure_stage, error_type = _run_state(_latest_stage_jobs(latest_jobs))

    published_runs: list[tuple[str, datetime]] = []
    for run_id, run_jobs in by_run.items():
        state, _, _ = _run_state(_latest_stage_jobs(run_jobs))
        if state == "current":
            published_runs.append((run_id, max(job.updated_at for job in run_jobs)))
    published_run_id = max(published_runs, key=lambda item: item[1])[0] if published_runs else None
    is_current = latest_state == "current"
    return {
        "state": latest_state,
        "is_current": is_current,
        "latest_run_id": latest_run_id,
        "published_run_id": published_run_id,
        "failure_stage": failure_stage,
        "error_type": error_type,
        "stale_subject_count": 0 if is_current else subject_count,
    }
