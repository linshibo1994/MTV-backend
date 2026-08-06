"""生成任务 API 路由 — POST/GET /generation-jobs。

端点：
- POST /generation-jobs          创建生图任务（绑定 material_id + subject_id + model），触发积分 reserve -> 异步 worker
- POST /generation-jobs/batch     一键设计 N：批量为未设计主体生成图（06 §3.5，消耗积分）
- GET  /generation-jobs/<job_id>  状态轮询（校验调用者对该任务所属 material/script 的访问权）
- GET  /generation-jobs           按 material_id 过滤列表（仅返回当前用户有权访问的画布项目下的任务）

每个 api 文件定义 router = APIRouter()，不 include 到主路由（Phase C 收口）。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ForbiddenError, NotFoundError
from app.core.permissions import Action, check_script_access
from app.crud.generation import get_job, list_jobs
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.script import Script
from app.models.user import User
from app.schemas.generation import (
    BatchCreateJobIn,
    BatchJobOut,
    CreateJobIn,
    GenerationJobOut,
    JobListOut,
)
from app.services.generation import create_batch_generation_jobs, create_generation_job
from app.workers.generation import cancel_generation_job

router = APIRouter()


def _sanitize_input_json(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    """裁剪 input_json 出参，仅保留 02 §4.7.1 / 04 §1.13 契约字段。

    契约规定 input_json 仅含 prompt、style_id 与 resolved_prompts.final。实现层为 worker
    settle/release 额外写入了 reservation_id（内部冻结流水 ID）以及 subject_id/
    subject_name/model/size/aspect/count，这些内部字段不应对调用方暴露（reservation_id
    属内部审计关联键，可被用于拼接积分查询枚举）。此处仅在出参时过滤，存储层保持不变，
    worker 仍可读取完整 input_json。
    """
    if not raw:
        return None
    sanitized: dict[str, Any] = {}
    if "prompt" in raw:
        sanitized["prompt"] = raw["prompt"]
    if "style_id" in raw and (raw["style_id"] is None or isinstance(raw["style_id"], str)):
        sanitized["style_id"] = raw["style_id"]
    resolved_final = (raw.get("resolved_prompts") or {}).get("final")
    if isinstance(resolved_final, str):
        sanitized["resolved_prompts"] = {"final": resolved_final}
    return sanitized or None


def _sanitize_metrics(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not raw:
        return None
    allowed: dict[str, Any] = {
        key: raw[key]
        for key in ("actual_points", "count")
        if key in raw
    }
    return allowed or None


_PUBLIC_ERROR_MESSAGES = {
    "provider_or_storage_error": "生成失败，请稍后重试",
    "provider_configuration_error": "生成服务配置异常，请联系管理员",
    "financial_cleanup": "生成清算中，请稍后重试",
    "invalid_job_input": "生成任务参数无效",
    "superseded_input": "剧本内容已更新，请重新发起生成",
}


def _public_error_message(job: GenerationJob) -> str | None:
    if not job.error_type and not job.error_message:
        return None
    return _PUBLIC_ERROR_MESSAGES.get(
        job.error_type or job.error_message or "",
        "生成失败，请稍后重试",
    )


def _to_out(job: GenerationJob) -> GenerationJobOut:
    """ORM -> 出参 schema 转换。"""
    return GenerationJobOut(
        job_id=job.job_id,
        material_id=job.material_id,
        script_id=job.script_id,
        subject_id=job.subject_id,
        episode_id=job.episode_id,
        scene_no=job.scene_no,
        job_type=job.job_type,
        status=job.status,
        progress=job.progress,
        progress_stage=job.progress_stage,
        progress_message=job.progress_message,
        error_message=_public_error_message(job),
        # 出参仅暴露契约字段，过滤 reservation_id 等内部键（02 §4.7.1）
        input_json=_sanitize_input_json(job.input_json),
        output_json=job.output_json,
        metrics=_sanitize_metrics(job.metrics),
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


async def _verify_job_access(
    db: AsyncSession, user: User, job: GenerationJob, *, action: Action = Action.READ
) -> None:
    """校验当前用户对该任务所属 material/script 的访问权（08 §4.1 资源级授权）。

    通过 material -> script -> check_script_access(action=Action.READ) 链校验。
    无 material/script 归属或无访问权时拒绝（403/401），避免凭 job_id 轮询他人任务。
    """
    if not job.material_id:
        raise ForbiddenError("任务无关联画布项目")
    material = (
        await db.execute(
            select(Materials)
            .where(Materials.id == job.material_id)
            .options(noload(Materials.nodes), noload(Materials.edges))
        )
    ).scalar_one_or_none()
    if material is None:
        raise ForbiddenError("画布项目不存在")
    script_id = job.script_id or material.script_id
    script = (
        await db.execute(
            select(Script)
            .where(Script.id == script_id)
            .options(
                noload(Script.episodes),
                noload(Script.subjects),
                noload(Script.parent_subjects),
                noload(Script.materials),
            )
        )
    ).scalar_one_or_none()
    if script is None:
        raise ForbiddenError("剧本不存在")
    await check_script_access(db, user, script, action=action)


@router.post("/generation-jobs", response_model=GenerationJobOut, status_code=201)
async def create_job_endpoint(
    payload: CreateJobIn,
    idempotency_key: str = Header(..., alias="Idempotency-Key", max_length=128),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> GenerationJobOut:
    """创建生图任务：校验归属 -> 积分预扣 -> 投递异步 worker。"""
    job = await create_generation_job(
        db, user=user, payload=payload, idempotency_key=idempotency_key
    )
    return _to_out(job)


@router.post(
    "/generation-jobs/batch",
    response_model=BatchJobOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_batch_jobs_endpoint(
    payload: BatchCreateJobIn,
    idempotency_key: str = Header(..., alias="Idempotency-Key", max_length=128),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> BatchJobOut:
    """一键设计 N：批量为未设计主体生成图（消耗积分），06 §3.5。"""
    created, skipped, failed = await create_batch_generation_jobs(
        db, user=user, payload=payload, idempotency_key=idempotency_key
    )
    return BatchJobOut(
        created=[_to_out(j) for j in created], skipped=skipped, failed=failed
    )


@router.get("/generation-jobs/{job_id}", response_model=GenerationJobOut)
async def get_job_endpoint(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> GenerationJobOut:
    """获取单个生成任务（前端轮询 status/progress）。

    校验调用者对该任务所属 material/script 的访问权（08 AC-G03 资源级授权）。
    """
    job = await get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    await _verify_job_access(db, user, job)
    return _to_out(job)


@router.post("/generation-jobs/{job_id}/cancel", response_model=GenerationJobOut)
async def cancel_job_endpoint(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> GenerationJobOut:
    """取消生图任务；终态重复取消返回 409。"""
    job = await get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    await _verify_job_access(db, user, job, action=Action.RUN_AI)
    await cancel_generation_job(db, job_id, actor_user_id=user.user_id)
    await db.commit()
    fresh = await get_job(db, job_id)
    if fresh is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return _to_out(fresh)


@router.get("/generation-jobs", response_model=JobListOut)
async def list_jobs_endpoint(
    material_id: str | None = Query(None, description="按画布项目过滤"),
    limit: int = Query(30, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> JobListOut:
    """生成任务列表（按 material_id 过滤）。

    仅返回当前用户有权访问的画布项目下的任务：material_id 缺省时返回空列表
    （强制按画布项目范围过滤，避免跨用户泄漏，08 AC-G03）。
    """
    if not material_id:
        # 强制按画布项目范围过滤，未指定则不返回任何任务
        return JobListOut(items=[])
    # 校验对该画布项目所属剧本的访问权
    material = (
        await db.execute(select(Materials).where(Materials.id == material_id))
    ).scalar_one_or_none()
    if material is None:
        raise NotFoundError("画布项目不存在")
    script = (
        await db.execute(select(Script).where(Script.id == material.script_id))
    ).scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    await check_script_access(db, user, script, action=Action.READ)

    jobs = await list_jobs(db, material_id=material_id, limit=limit, offset=offset)
    return JobListOut(items=[_to_out(j) for j in jobs])
