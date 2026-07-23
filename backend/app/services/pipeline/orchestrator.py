"""流水线编排器 — trigger_pipeline 投递 Celery DAG 任务链。

DAG: split → extract → merge → { enrich ‖ parents }
- 读 pipeline_status、写 pipeline_stage（重置为 pending，split 标记 running）
- 一次性预扣积分（reserve），reservation_id 经链路贯穿至终态阶段 settle/release
- split 阶段 detail 写入 input_version，作为 worker 输入版本过期判定的权威信号
- 投递 Celery 链：chain(split, extract, merge, group(enrich, parents))
  enrich 与 parents 仅依赖 merge，并行执行，复刻 DAG 可观测状态（AC-SU03）
- PipelineJob 预建 attempt=1；显式 Celery 重试复制新历史 attempt
"""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, InsufficientCreditsError
from app.core.ids import new_id
from app.models.credit import CreditAccount
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.script import Script
from app.models.subject import Subject
from app.services.credits import release as credits_release
from app.services.credits import reserve
from app.services.pipeline import (
    STAGE_LABELS,
    STAGE_ORDER,
    get_stage,
    set_stage,
)
from app.services.task_dispatch import enqueue_task

PIPELINE_DAG_TASK_TYPE = "pipeline.dag"


def _database_dialect_name(db: Any) -> str | None:
    """从真实会话或同步测试 adapter/proxy 解析结构化 dialect。"""
    candidates = (db, getattr(db, "session", None), getattr(db, "sync_session", None))
    for candidate in candidates:
        if candidate is None:
            continue
        bind = getattr(candidate, "bind", None)
        if bind is None:
            get_bind = getattr(candidate, "get_bind", None)
            if callable(get_bind):
                bind = get_bind()
        dialect_name = getattr(getattr(bind, "dialect", None), "name", None)
        if isinstance(dialect_name, str):
            return dialect_name
    return None


async def acquire_pipeline_advisory_lock(db: Any, script_id: str) -> bool:
    """仅 PostgreSQL 取得同剧本流水线事务锁；其他数据库安全跳过。"""
    if _database_dialect_name(db) != "postgresql":
        return False
    await db.execute(
        select(
            func.pg_advisory_xact_lock(
                func.hashtext("pipeline_trigger"),
                func.hashtext(script_id),
            )
        )
    )
    return True


async def _is_running(db: AsyncSession, script_id: str) -> bool:
    """任一阶段处于 running 视为流水线运行中。"""
    from app.models.pipeline_stage import PipelineStage

    result = await db.execute(
        select(PipelineStage.id)
        .where(
            PipelineStage.script_id == script_id,
            PipelineStage.status == "running",
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def _lock_pipeline_parents(
    db: AsyncSession,
    script_id: str,
    episode_id: str,
) -> tuple[Script, Episode]:
    """按 Script -> advisory -> Episode 锁定，与所有创建入口保持同序。"""
    script = (
        await db.execute(
            select(Script)
            .where(Script.id == script_id)
            .with_for_update(read=True, key_share=True)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if script is None:
        raise ConflictError("剧本不存在")
    await acquire_pipeline_advisory_lock(db, script_id)
    episode = (
        await db.execute(
            select(Episode)
            .where(
                Episode.episode_id == episode_id,
                Episode.script_id == script_id,
            )
            .with_for_update(read=True, key_share=True)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if episode is None:
        raise ConflictError("分集不存在")
    return script, episode


def build_pipeline_outbox_payload(
    *,
    script_id: str,
    episode_id: str,
    input_version: int,
    input_client_save_seq: int,
    reservation_id: str,
    estimated: int,
) -> dict:
    """T3 暴露给共享 outbox publisher 的稳定载荷合约。"""
    return {
        "script_id": script_id,
        "episode_id": episode_id,
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
        "reservation_id": reservation_id,
        "estimated": estimated,
    }


async def _supersede_active_pipeline_runs(db: AsyncSession, script_id: str) -> int:
    """取消旧流水线 run，使新版本保存后一定能可靠调度最新输入。"""
    active_statuses = {"pending", "retry", "running", "ready"}
    result = await db.execute(
        select(PipelineJob).where(
            PipelineJob.script_id == script_id,
            PipelineJob.status.in_(active_statuses),
        )
    )
    jobs = list(result.scalars().all())
    if not jobs:
        return 0

    now = datetime.now(UTC)
    run_ids = {job.run_id for job in jobs}
    for job in jobs:
        job.status = "skipped"
        job.error_type = "superseded"
        job.error_message = "已被新版本输入取代"
        job.retryable = False
        job.lease_owner = None
        job.lease_token = None
        job.lease_until = None
        job.finished_at = job.finished_at or now

    reservations = (
        (
            await db.execute(
                select(CreditReservation).where(
                    CreditReservation.source_type == "pipeline",
                    CreditReservation.source_id.in_(run_ids),
                    CreditReservation.status == "reserved",
                )
            )
        )
        .scalars()
        .all()
    )
    for reservation in reservations:
        await credits_release(
            db,
            reservation.id,
            "superseded by newer input",
            commit=False,
        )
    await db.flush()
    return len(jobs)


async def _estimate_cost(db: AsyncSession, script_id: str) -> int:
    """预估本次流水线积分消耗（reserve 的 estimated）。

    extract: 当前集数 × price_extract_per_episode
    merge:   price_merge（固定）
    enrich:  当前主体数 × price_enrich_per_subject（首次运行主体为 0，由 settle 按实际结算补足）
    +1 保底，确保 estimated > 0（reserve 拒绝 0）。
    """
    ep_count = (
        await db.execute(
            select(func.count(Episode.episode_id)).where(Episode.script_id == script_id)
        )
    ).scalar() or 0
    subj_count = (
        await db.execute(
            select(func.count(Subject.id)).where(
                Subject.script_id == script_id,
                Subject.is_active.is_(True),
            )
        )
    ).scalar() or 0
    from app.core.config import settings

    estimated = (
        int(ep_count) * settings.price_extract_per_episode
        + settings.price_merge
        + int(subj_count) * settings.price_enrich_per_subject
        + 1
    )
    return max(estimated, 1)


async def trigger_pipeline(
    db: AsyncSession, script_id: str, episode_id: str
) -> dict:
    """触发剧本处理流水线。

    在 API 层（async session，持有用户上下文）调用：
      1. 并发保护：若任一阶段 running → 拒绝重复触发（08§2.2 / 防 chain 竞态）
      2. 全部 5 阶段重置为 pending；split 标记 running 并写入 input_version
      3. 预扣积分（reserve，source_type='pipeline'）；不足抛 InsufficientCreditsError
      4. 投递 Celery DAG：chain(split, extract, merge, group(enrich, parents))
    首次 PipelineJob 在本事务预建；显式 Celery 重试由 worker 创建新 attempt。
    返回投递信息。
    """
    # 1. 并发保护：对同一剧本的触发串行化，避免 _is_running 检查与阶段重置之间产生竞态
    #    两个并发 PUT 可同时通过 _is_running=False 检查，各自 reserve（source_id=不同 run_id
    #    不触发幂等拒绝）并投递两条 Celery chain，流水线并发跑同一剧本。此处用事务级咨询锁
    #    （pg_advisory_xact_lock，commit/rollback 后自动释放）将 check→reset→reserve→commit
    #    串行化：第二个调用阻塞至第一个提交（split=running）后回检即被拒绝（08§2.2）。
    script, episode = await _lock_pipeline_parents(db, script_id, episode_id)
    await _supersede_active_pipeline_runs(db, script_id)

    for stage in STAGE_ORDER:
        row = await get_stage(db, script_id, stage)
        # 新轮次不能遗留上一轮的进度、展示明细或结构化错误。
        row.status = "pending"
        row.detail = None
        row.current = None
        row.total = None
        row.error = None
        await db.flush()

    # 使用父锁后的权威分集版本绑定本轮输入。
    input_version = episode.episode_version or 1
    input_client_save_seq = episode.client_save_seq or 0

    # split 阶段 detail 写入 input_version —— worker 据此判定旧链路是否被取代
    await set_stage(
        db,
        script_id,
        "split",
        status="running",
        detail={
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )

    # 2. 预扣积分：按剧本所有者账户 reserve（commit=False，与 split 阶段写入合并为单事务，
    #    避免 reserve 内部 commit 后任务拿不到 reservation_id 的孤立冻结）
    account = (
        await db.execute(
            select(CreditAccount).where(
                CreditAccount.owner_type == "user",
                CreditAccount.owner_id == script.owner_user_id,
            )
        )
    ).scalar_one_or_none()
    if account is None:
        raise InsufficientCreditsError("用户无积分账户")

    estimated = await _estimate_cost(db, script_id)
    run_id = new_id()
    reservation_id = await reserve(
        db,
        account_id=account.id,
        source_type="pipeline",
        source_id=run_id,
        estimated=estimated,
        commit=False,
    )
    # 同一事务持久化可靠 outbox；commit 后由 dispatcher 构造 Celery canvas。
    outbox = await enqueue_task(
        db,
        task_type=PIPELINE_DAG_TASK_TYPE,
        job_id=run_id,
        attempt=input_version,
        payload=build_pipeline_outbox_payload(
            script_id=script_id,
            episode_id=episode_id,
            input_version=input_version,
            input_client_save_seq=input_client_save_seq,
            reservation_id=reservation_id,
            estimated=estimated,
        ),
    )
    for stage in STAGE_ORDER:
        db.add(
            PipelineJob(
                id=new_id(),
                script_id=script_id,
                episode_id=episode_id,
                stage=stage,
                dispatch_id=outbox.id,
                run_id=run_id,
                status="pending",
                input_version=input_version,
                input_client_save_seq=input_client_save_seq,
                attempt=1,
            )
        )
    await db.flush()
    # 单次提交：split 阶段 running + 积分冻结 + outbox 原子落库。
    await db.commit()

    return {
        "script_id": script_id,
        "episode_id": episode_id,
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
        "reservation_id": reservation_id,
        "estimated": estimated,
        "dispatch_id": outbox.id,
        "run_id": run_id,
    }


async def get_pipeline_status(db: AsyncSession, script_id: str) -> dict:
    """用单次轻量列查询读取 5 阶段状态并计算聚合状态。

    skipped 视为非 done（被取代的链路不应让新链路误判 all_done）；
    但新链路触发会重置阶段为 pending→done，故旧链路的 skipped 不会残留。
    """
    from app.models.pipeline_stage import PipelineStage

    rows = (
        await db.execute(
            select(
                PipelineStage.stage,
                PipelineStage.status,
                PipelineStage.detail,
                PipelineStage.current,
                PipelineStage.total,
                PipelineStage.error,
            ).where(PipelineStage.script_id == script_id)
        )
    ).all()
    by_stage = {row.stage: row for row in rows}
    stages: list[dict] = []
    statuses: list[str] = []
    for stage in STAGE_ORDER:
        row = by_stage.get(stage)
        stage_status = row.status if row is not None else "pending"
        stages.append(
            {
                "stage": stage,
                "label": STAGE_LABELS.get(stage, stage),
                "status": stage_status,
                "detail": row.detail if row is not None else None,
                "current": row.current if row is not None else None,
                "total": row.total if row is not None else None,
                "error": row.error if row is not None else None,
            }
        )
        statuses.append(stage_status)

    return {
        "stages": stages,
        "running": "running" in statuses,
        "failed": "failed" in statuses,
        "all_done": bool(statuses) and all(s == "done" for s in statuses),
    }
