"""生成任务数据访问层 — CRUD。

- create_job: 创建任务记录
- get_job: 按 job_id 查询单任务
- list_jobs: 按 material_id 过滤列表
- update_job_status: 更新状态/进度/输出等字段
- update_progress: 轻量更新进度
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import case, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_job import GenerationJob


async def create_job(
    db: AsyncSession,
    *,
    job_id: str,
    material_id: str | None,
    script_id: str | None,
    owner_user_id: str | None = None,
    subject_id: str | None = None,
    idempotency_key: str | None = None,
    request_hash: str | None = None,
    input_version: int = 1,
    attempt: int = 1,
    job_type: str = "generate_images",
    input_json: dict | None = None,
    episode_id: str | None = None,
    scene_no: int | None = None,
) -> GenerationJob:
    """创建生成任务记录，初始 status=starting, progress=0。"""
    job = GenerationJob(
        job_id=job_id,
        material_id=material_id,
        script_id=script_id,
        owner_user_id=owner_user_id,
        subject_id=subject_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        input_version=input_version,
        attempt=attempt,
        episode_id=episode_id,
        scene_no=scene_no,
        job_type=job_type,
        status="starting",
        progress=0,
        input_json=input_json,
    )
    db.add(job)
    await db.flush()
    return job


async def get_job(db: AsyncSession, job_id: str) -> GenerationJob | None:
    """按 job_id 查询单个生成任务。"""
    result = await db.execute(
        select(GenerationJob).where(GenerationJob.job_id == job_id)
    )
    return result.scalar_one_or_none()


async def list_jobs(
    db: AsyncSession,
    *,
    material_id: str | None = None,
    limit: int = 30,
    offset: int = 0,
) -> list[GenerationJob]:
    """按 material_id 过滤的生成任务列表，按创建时间倒序。"""
    stmt = select(GenerationJob)
    if material_id:
        stmt = stmt.where(GenerationJob.material_id == material_id)
    stmt = stmt.order_by(GenerationJob.created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def update_job_status(
    db: AsyncSession,
    job_id: str,
    *,
    status: str | None = None,
    progress: int | None = None,
    progress_stage: str | None = None,
    progress_message: str | None = None,
    error_message: str | None = None,
    output_json: dict | None = None,
    input_json: dict | None = None,
    metrics: dict | None = None,
) -> None:
    """更新生成任务状态及关联字段（仅更新非 None 字段）。"""
    values: dict[str, Any] = {}
    if status is not None:
        values["status"] = status
    if progress is not None:
        values["progress"] = case(
            (GenerationJob.progress > progress, GenerationJob.progress),
            else_=progress,
        )
    if progress_stage is not None:
        values["progress_stage"] = progress_stage
    if progress_message is not None:
        values["progress_message"] = progress_message
    if error_message is not None:
        values["error_message"] = error_message
    if output_json is not None:
        values["output_json"] = output_json
    if input_json is not None:
        values["input_json"] = input_json
    if metrics is not None:
        values["metrics"] = metrics
    if not values:
        return
    await db.execute(
        update(GenerationJob).where(GenerationJob.job_id == job_id).values(**values)
    )


async def update_progress(
    db: AsyncSession,
    job_id: str,
    *,
    progress: int,
    stage: str | None = None,
    message: str | None = None,
) -> None:
    """轻量更新进度（progress + 可选 stage/message）。"""
    values: dict[str, Any] = {
        "progress": case(
            (GenerationJob.progress > progress, GenerationJob.progress),
            else_=progress,
        )
    }
    if stage is not None:
        values["progress_stage"] = stage
    if message is not None:
        values["progress_message"] = message
    await db.execute(
        update(GenerationJob).where(GenerationJob.job_id == job_id).values(**values)
    )
