"""剧本处理流水线 Celery worker — split→extract→merge→(enrich‖parents)。

DAG 结构（08 文档 §2.2）：
  split → extract → merge → { enrich ‖ parents }
  enrich 与 parents 均仅依赖 merge，可并行/乱序完成，复刻
  "parents=done 且 enrich=pending" 的可观测状态（AC-SU03）。

每阶段任务（_run_stage_async）：
  1. 用 run_id/stage/attempt 行锁领取 PipelineJob；已完成阶段透传正常上下文。
  2. 按真实 Episode 校验 input_version，运行期间周期续租。
  3. service 写入、stage 终态与 token/input_version CAS 同事务提交。
  4. 瞬时重试创建新 attempt；lease 过期重领复用原 attempt。
  5. enrich/parents 先标记 ready，由唯一 chord finalizer 结算并原子置 done。

积分三段式（08 文档 §2.4 / TASK T2.2）：
  - reserve 在 orchestrator.trigger_pipeline（API 层有用户账户）一次性预扣，
    reservation_id 经 Celery 链路贯穿至终态阶段。
  - 任一阶段最终失败 → 先提交结构化 failure，再在独立事务 release。
  - 两个叶子均 ready → finalizer 按实际消耗 settle；失败不得留下 done job。
  - superseded 链路 → skipped 与 release 在同一事务提交。

async + Celery 混用方案（PLAN 风险 11）：所有同步任务入口提交到 worker 进程级
持久事件循环，避免 AsyncEngine 池内连接跨事件循环复用。
"""

import asyncio
import inspect
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.orm import noload

from app.core.config import settings
from app.core.database import async_session_factory
from app.core.ids import new_id
from app.core.job_state import PipelineFailure
from app.crud import asset as asset_crud
from app.crud.script import replace_parent_subjects, replace_subjects
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.script import Script
from app.services.credits import release as credits_release
from app.services.credits import settle as credits_settle
from app.services.pipeline.orchestrator import acquire_pipeline_advisory_lock
from app.workers.async_runtime import run_worker_async
from app.workers.celery_app import app as celery_app

logger = logging.getLogger(__name__)

# 阶段 → 服务函数的导入名（延迟导入避免循环依赖）
_STAGE_SERVICE = {
    "split": ("app.services.pipeline.split", "run_split"),
    "extract": ("app.services.pipeline.extract", "run_extract"),
    "merge": ("app.services.pipeline.merge", "run_merge"),
    "enrich": ("app.services.pipeline.enrich", "run_enrich"),
    "parents": ("app.services.pipeline.parents", "run_parents"),
}

# 叶子阶段由唯一 finalizer 在两者均 ready 后统一结算。
_TERMINAL_STAGES = {"enrich", "parents"}
_AUTORETRY_FOR = (TimeoutError, ConnectionError, OSError)
_LEASE_SECONDS = 300
_HEARTBEAT_SECONDS = 60
_MAX_ATTEMPTS = 3


class PipelineStageBusy(OSError):
    """同阶段正由其他有效 lease 执行，当前投递必须重试而非报告成功。"""


@dataclass(frozen=True)
class PipelineJobClaim:
    job_id: str
    lease_token: str
    script_id: str
    episode_id: str | None
    stage: str
    dispatch_id: str
    run_id: str
    input_version: int
    input_client_save_seq: int | None
    attempt: int


def _is_expired(value: datetime | None, now: datetime) -> bool:
    if value is None:
        return False
    comparable_now = now if value.tzinfo is not None else now.replace(tzinfo=None)
    return value <= comparable_now


# --------------------------------------------------------------------------- #
# DB 内辅助（均接受已存在的 AsyncSession，避免多余事件循环/会话）
# --------------------------------------------------------------------------- #
async def _mark_stage(
    db,
    script_id: str,
    episode_id: str | None,
    stage: str,
    status: str,
    input_version: int,
    input_client_save_seq: int | None = None,
    detail: dict | None = None,
    error: PipelineFailure | None = None,
) -> bool:
    """仅在输入版本仍是当前版本时写回阶段，防止晚到任务覆盖新轮次。"""
    from app.services.pipeline import input_episode_is_current

    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return False
    from app.services.pipeline import get_stage

    row = await get_stage(db, script_id, stage)
    row.status = status
    if detail is not None:
        row.detail = {
            **detail,
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        }
    elif status == "running":
        previous_detail = row.detail or {}
        same_input = (
            previous_detail.get("input_version") == input_version
            and previous_detail.get("input_client_save_seq") == input_client_save_seq
        )
        row.detail = {
            **(previous_detail if same_input else {}),
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        }
    if error is not None:
        row.error = error.to_dict()
    await db.flush()
    return True


async def _reset_owned_running_stage(
    db, script_id: str, stage: str, input_version: int
) -> bool:
    """仅清理由当前输入版本标记的 running stage，避免覆盖新 run。"""
    from app.services.pipeline import get_stage

    row = await get_stage(db, script_id, stage)
    detail = row.detail or {}
    if row.status != "running" or detail.get("input_version") != input_version:
        return False
    row.status = "pending"
    # extract/enrich 的明细是可恢复检查点；同一输入版本重试时保留，避免重复外呼。
    if stage not in {"extract", "enrich"}:
        row.detail = None
        row.current = None
        row.total = None
    row.error = None
    await db.flush()
    return True


async def _call_mark_stage(
    db,
    script_id: str,
    episode_id: str | None,
    stage: str,
    status: str,
    input_version: int,
    input_client_save_seq: int | None,
    *,
    detail: dict | None = None,
    error: PipelineFailure | None = None,
) -> bool:
    """调用可被旧测试替身 monkeypatch 的 _mark_stage，同时生产路径带 seq。"""
    parameters = inspect.signature(_mark_stage).parameters
    if "input_client_save_seq" in parameters:
        return await _mark_stage(
            db,
            script_id,
            episode_id,
            stage,
            status,
            input_version,
            input_client_save_seq,
            detail=detail,
            error=error,
        )
    return await _mark_stage(db, script_id, episode_id, stage, status, input_version)


def _pipeline_claim_conditions(
    run_id: str,
    stage: str,
    dispatch_id: str | None,
    now: datetime,
) -> list[Any]:
    conditions = [PipelineJob.run_id == run_id, PipelineJob.stage == stage]
    if dispatch_id is not None:
        conditions.append(PipelineJob.dispatch_id == dispatch_id)
    conditions.append(
        or_(
            PipelineJob.status.in_(["pending", "retry"]),
            (PipelineJob.status == "running")
            & (PipelineJob.lease_until.is_not(None))
            & (PipelineJob.lease_until <= now),
        )
    )
    return conditions


def pipeline_claim_query(
    run_id: str,
    stage: str,
    dispatch_id: str | None,
    now: datetime,
):
    """构造 PG 行锁领取查询；SQLite 会保留同一确定性选择路径。"""
    return (
        select(PipelineJob)
        .where(
            *_pipeline_claim_conditions(run_id, stage, dispatch_id, now),
        )
        .options(noload(PipelineJob.script))
        .order_by(PipelineJob.attempt.desc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )


def pipeline_claim_parent_query(
    run_id: str,
    stage: str,
    dispatch_id: str | None,
    now: datetime,
):
    """领取前只读父标识；真正状态与 attempt 仍在父锁后复检。"""
    return (
        select(PipelineJob.script_id, PipelineJob.episode_id)
        .where(*_pipeline_claim_conditions(run_id, stage, dispatch_id, now))
        .order_by(PipelineJob.attempt.desc())
        .limit(1)
    )


async def _lock_pipeline_retry_parents(
    db,
    script_id: str,
    episode_id: str | None,
) -> bool:
    """按 Script -> advisory -> Episode 取得父锁，与所有创建入口同序。"""
    locked_script_id = (
        await db.execute(
            select(Script.id)
            .where(Script.id == script_id)
            .with_for_update(read=True, key_share=True)
        )
    ).scalar_one_or_none()
    if locked_script_id is None:
        return False
    await acquire_pipeline_advisory_lock(db, script_id)
    if episode_id is None:
        return True
    locked_episode_id = (
        await db.execute(
            select(Episode.episode_id)
            .where(
                Episode.episode_id == episode_id,
                Episode.script_id == script_id,
            )
            .with_for_update(read=True, key_share=True)
        )
    ).scalar_one_or_none()
    return locked_episode_id is not None


async def claim_pipeline_job(
    db,
    run_id: str,
    stage: str,
    owner: str,
    *,
    dispatch_id: str | None = None,
    lease_seconds: int = _LEASE_SECONDS,
) -> PipelineJobClaim | None:
    """锁定一个可领取 attempt；过期 lease 重领不创建或修改 attempt。"""
    now = datetime.now(UTC)
    token = new_id()
    parent_snapshot = (
        await db.execute(
            pipeline_claim_parent_query(run_id, stage, dispatch_id, now)
        )
    ).one_or_none()
    if parent_snapshot is None or not await _lock_pipeline_retry_parents(
        db,
        parent_snapshot.script_id,
        parent_snapshot.episode_id,
    ):
        return None
    candidate = (
        await db.execute(pipeline_claim_query(run_id, stage, dispatch_id, now))
    ).scalar_one_or_none()
    if candidate is None:
        return None
    if (
        candidate.script_id != parent_snapshot.script_id
        or candidate.episode_id != parent_snapshot.episode_id
    ):
        return None
    if (
        candidate.status == "running"
        and _is_expired(candidate.lease_until, now)
    ):
        candidate.status = "failed"
        candidate.error_type = candidate.error_type or "lease_expired"
        candidate.error_message = candidate.error_message or "任务租约已过期"
        candidate.retryable = candidate.attempt < _MAX_ATTEMPTS
        candidate.lease_owner = None
        candidate.lease_token = None
        candidate.lease_until = None
        candidate.finished_at = candidate.finished_at or now
        if candidate.attempt >= _MAX_ATTEMPTS:
            await db.flush()
            return None
        candidate = await _insert_pipeline_retry_attempt(db, candidate)
    result = await db.execute(
        update(PipelineJob)
        .where(
            PipelineJob.id == candidate.id,
            PipelineJob.status == candidate.status,
            PipelineJob.lease_token == candidate.lease_token,
        )
        .values(
            status="running",
            lease_owner=owner,
            lease_token=token,
            lease_until=now + timedelta(seconds=lease_seconds),
            heartbeat_at=now,
            started_at=candidate.started_at or now,
        )
        .returning(PipelineJob.id.label("job_id"), PipelineJob.lease_token)
    )
    row = result.one_or_none()
    await db.flush()
    if row is None:
        return None
    return PipelineJobClaim(
        job_id=row.job_id,
        lease_token=row.lease_token,
        script_id=candidate.script_id,
        episode_id=candidate.episode_id,
        stage=candidate.stage,
        dispatch_id=candidate.dispatch_id,
        run_id=candidate.run_id,
        input_version=candidate.input_version,
        input_client_save_seq=candidate.input_client_save_seq,
        attempt=candidate.attempt,
    )


async def _insert_pipeline_retry_attempt(db, source) -> PipelineJob:
    retry = PipelineJob(
        id=new_id(),
        script_id=source.script_id,
        episode_id=source.episode_id,
        stage=source.stage,
        dispatch_id=source.dispatch_id,
        run_id=source.run_id,
        status="pending",
        input_version=source.input_version,
        input_client_save_seq=getattr(source, "input_client_save_seq", None),
        attempt=source.attempt + 1,
    )
    db.add(retry)
    await db.flush()
    return retry


async def create_pipeline_retry_attempt(db, source) -> PipelineJob:
    """按父先锁序复制新 attempt；旧行保持终态和错误历史不变。"""
    if not await _lock_pipeline_retry_parents(
        db,
        source.script_id,
        source.episode_id,
    ):
        raise PipelineStageBusy("流水线父资源已删除")
    source_id = getattr(source, "job_id", None) or source.id
    locked_source = (
        await db.execute(
            select(PipelineJob)
            .where(PipelineJob.id == source_id)
            .options(noload(PipelineJob.script))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if locked_source is None:
        raise PipelineStageBusy("流水线原始 attempt 已删除")
    return await _insert_pipeline_retry_attempt(db, locked_source)


async def get_pipeline_job_status(db, run_id: str, stage: str) -> str | None:
    """读取当前阶段最新 attempt 状态，用于区分完成透传与执行中竞争。"""
    result = await db.execute(
        select(PipelineJob.status)
        .where(PipelineJob.run_id == run_id, PipelineJob.stage == stage)
        .order_by(PipelineJob.attempt.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def heartbeat_pipeline_job(
    db, job_id: str, lease_token: str, *, lease_seconds: int = _LEASE_SECONDS
) -> bool:
    """仅当前 token 可续租。"""
    now = datetime.now(UTC)
    result = await db.execute(
        update(PipelineJob)
        .where(
            PipelineJob.id == job_id,
            PipelineJob.status == "running",
            PipelineJob.lease_token == lease_token,
        )
        .values(
            heartbeat_at=now,
            lease_until=now + timedelta(seconds=lease_seconds),
        )
    )
    await db.flush()
    return result.rowcount == 1


async def finish_pipeline_job(
    db,
    job_id: str,
    lease_token: str,
    status: str,
    failure: PipelineFailure | None = None,
    output_json: dict | None = None,
) -> bool:
    """终态或 retry 写回使用 status+token 的数据库 CAS。"""
    if status not in {"ready", "done", "failed", "retry", "skipped"}:
        raise ValueError("无效 PipelineJob 完成状态")
    values: dict[str, object] = {
        "status": status,
        "lease_owner": None,
        "lease_token": None,
        "lease_until": None,
    }
    if status in {"ready", "done", "failed", "skipped"}:
        values["finished_at"] = datetime.now(UTC)
    if failure is not None:
        values.update(
            error_type=failure.error_type,
            error_message=failure.error_message,
            retryable=failure.retryable,
        )
    if output_json is not None:
        values["output_json"] = output_json
    result = await db.execute(
        update(PipelineJob)
        .where(
            PipelineJob.id == job_id,
            PipelineJob.status == "running",
            PipelineJob.lease_token == lease_token,
        )
        .values(**values)
    )
    await db.flush()
    return result.rowcount == 1


# --------------------------------------------------------------------------- #
# 积分结算/释放（幂等包装）
# --------------------------------------------------------------------------- #
async def _compute_actual(db, script_id: str) -> int:
    """按已落库的阶段明细计算实际消耗积分。

    - extract: 集数 × price_extract_per_episode
    - merge:   price_merge（固定）
    - enrich:  主体数 × price_enrich_per_subject
    - split / parents: 文档未定义定价，按 0 计
    """
    from app.services.pipeline import get_stage

    extract_stage = await get_stage(db, script_id, "extract")
    enrich_stage = await get_stage(db, script_id, "enrich")
    # extract/enrich 的计数由 run_extract/run_enrich 通过 set_stage(..., total=...)
    # 写入 PipelineStage.total 整数列；detail 结构为 {"episodes":[...]} /
    # {"count":..., "prompts":{...}}，并不含 total 键，故必须读 total 列，
    # 否则 extract 成本会漏算为 0。
    ep_total = int((extract_stage.total or 0) if extract_stage else 0)
    subj_count = int((enrich_stage.total or 0) if enrich_stage else 0)
    actual = (
        ep_total * settings.price_extract_per_episode
        + settings.price_merge  # merge 按固定价计（未写 total 列）
        + subj_count * settings.price_enrich_per_subject
    )
    return max(actual, 0)


async def _settle_pipeline(
    db, script_id: str, reservation_id: str, estimated: int
) -> None:
    """终态成功：按实际消耗结算，允许超过预估并由积分服务校验余额。"""
    if not reservation_id:
        return
    actual = await _compute_actual(db, script_id)
    await credits_settle(
        db,
        reservation_id=reservation_id,
        actual=actual,
        commit=False,
    )


async def _release_pipeline(
    db, reservation_id: str, reason: str
) -> None:
    """失败/取消/被取代：释放冻结积分，设施错误必须向上传播。"""
    if not reservation_id:
        return
    await credits_release(
        db,
        reservation_id=reservation_id,
        reason=reason,
        commit=False,
    )


async def finalize_pipeline_jobs(
    db,
    *,
    run_id: str,
    script_id: str,
    reservation_id: str | None,
    estimated: int,
) -> bool:
    """按统一锁序结算，并在同一事务发布父分组。"""

    async def load_leaves(*, lock: bool) -> list[PipelineJob]:
        query = (
            select(PipelineJob)
            .where(
                PipelineJob.run_id == run_id,
                PipelineJob.stage.in_(_TERMINAL_STAGES),
                PipelineJob.status.in_(["ready", "done", "skipped"]),
            )
            .options(noload(PipelineJob.script))
            .order_by(PipelineJob.stage.asc(), PipelineJob.attempt.asc())
        )
        if lock:
            query = query.with_for_update()
        return list((await db.execute(query)).scalars().all())

    def validate_leaves(
        rows: list[PipelineJob],
    ) -> tuple[dict[str, PipelineJob], tuple[str | None, int, int | None]]:
        latest = {row.stage: row for row in rows}
        if set(latest) != _TERMINAL_STAGES:
            raise RuntimeError("pipeline leaves are not ready")
        if all(row.status == "skipped" for row in latest.values()):
            return latest, (None, 0, None)
        if all(row.status == "done" for row in latest.values()):
            return latest, (None, 0, None)
        if any(row.status != "ready" for row in latest.values()):
            raise RuntimeError("pipeline leaves have inconsistent terminal state")
        leaf_inputs = {
            (row.episode_id, row.input_version, row.input_client_save_seq)
            for row in latest.values()
        }
        if len(leaf_inputs) != 1:
            raise RuntimeError("pipeline leaves have inconsistent input version")
        return latest, leaf_inputs.pop()

    # 第一次只读取元数据，不持有 PipelineJob 锁；随后先锁 Episode，和保存路径保持一致。
    preview_latest, preview_input = validate_leaves(await load_leaves(lock=False))
    if all(row.status in {"done", "skipped"} for row in preview_latest.values()):
        return False
    episode_id, input_version, input_client_save_seq = preview_input
    if episode_id is None or not await _input_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        raise RuntimeError("pipeline input is no longer current")

    latest, locked_input = validate_leaves(await load_leaves(lock=True))
    if locked_input != preview_input:
        raise RuntimeError("pipeline leaves changed during finalization")

    merge_job = (
        (
            await db.execute(
                select(PipelineJob)
                .where(
                    PipelineJob.run_id == run_id,
                    PipelineJob.stage == "merge",
                    PipelineJob.status == "done",
                )
                .options(noload(PipelineJob.script))
                .order_by(PipelineJob.attempt.desc())
                .limit(1)
                .with_for_update()
            )
        )
        .scalar_one_or_none()
    )
    merge_output = merge_job.output_json if merge_job is not None else None
    enrich_output = latest["enrich"].output_json or {}
    parents_output = latest["parents"].output_json or {}
    if merge_job is None or not isinstance(merge_output, dict):
        raise RuntimeError("pipeline merge staging output is missing")
    if "prompts" not in enrich_output or "parent_subjects" not in parents_output:
        raise RuntimeError("pipeline leaf staging output is missing")

    if reservation_id:
        await _settle_pipeline(db, script_id, reservation_id, estimated)

    # 结算、父分组发布和 job/stage 终态共用当前事务；任一步失败将整体回滚。
    if merge_output.get("subjects_published") is not True:
        subjects = merge_output.get("subjects")
        if not isinstance(subjects, list):
            raise RuntimeError("pipeline merge staging output is missing subjects")
        # 兼容部署前已完成 merge 的运行：它们仅在 finalizer 中发布主体。
        await asset_crud.persist_subject_suggestions(
            db,
            script_id,
            subjects,
            source_run_id=run_id,
        )
        await replace_subjects(db, script_id, subjects)
    await replace_parent_subjects(
        db, script_id, parents_output.get("parent_subjects") or []
    )
    for stage in ("enrich", "parents"):
        row = latest[stage]
        stage_output = enrich_output if stage == "enrich" else parents_output
        public_detail = {
            key: value
            for key, value in stage_output.items()
            if key != "parent_subjects"
        }
        if not await _call_mark_stage(
            db,
            script_id,
            episode_id,
            stage,
            "done",
            input_version,
            input_client_save_seq,
            detail=public_detail,
        ):
            raise RuntimeError("pipeline input changed during settlement")
        row.status = "done"
        row.finished_at = row.finished_at or datetime.now(UTC)
    await db.flush()
    return True


# --------------------------------------------------------------------------- #
# 阶段执行主体（在 worker 持久事件循环内完成）
# --------------------------------------------------------------------------- #
async def _heartbeat_loop(job_id: str, lease_token: str, stop: asyncio.Event) -> None:
    """阶段运行期间周期续租；停止事件可立即结束等待。"""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=_HEARTBEAT_SECONDS)
        except TimeoutError:
            async with async_session_factory() as db:
                renewed = await heartbeat_pipeline_job(db, job_id, lease_token)
                await db.commit()
            if not renewed:
                return


async def _input_is_current(
    db,
    script_id: str,
    episode_id: str,
    input_version: int,
    input_client_save_seq: int | None = None,
    *,
    lock: bool = False,
) -> bool:
    from app.services.pipeline import input_episode_is_current

    return await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=lock,
    )


async def _run_stage_async(
    stage: str,
    script_id: str | dict,
    episode_id: str,
    service_args: tuple,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
    retry_exhausted: bool = False,
    input_client_save_seq: int | None = None,
) -> str | dict:
    """通用阶段异步执行器。

    流程见模块文档字符串。成功返回 script_id（作为链路下一任务的入参）。
    """
    if isinstance(script_id, dict):
        context_script_id = script_id.get("script_id")
        if not isinstance(context_script_id, str) or not context_script_id:
            raise ValueError("流水线上下文缺少 script_id")
        script_id = context_script_id
    is_terminal = stage in _TERMINAL_STAGES

    # 1. 条件领取 job；仅已完成阶段透传，有效租约竞争必须重试。
    claim_status: str | None = None
    async with async_session_factory() as db:
        claim = await claim_pipeline_job(
            db,
            run_id,
            stage,
            owner=f"pipeline-worker:{stage}",
            dispatch_id=dispatch_id,
        )
        if claim is None:
            claim_status = await get_pipeline_job_status(db, run_id, stage)
        await db.commit()
    if claim is None:
        if claim_status in {"done", "ready", "skipped"}:
            return script_id
        raise PipelineStageBusy("流水线阶段正在执行或等待重试")
    input_client_save_seq = getattr(claim, "input_client_save_seq", input_client_save_seq)

    # 2. 领取后核验真实 Episode；旧输入仅将本 run 标为 skipped。
    async with async_session_factory() as db:
        if not await _input_is_current(
            db, script_id, episode_id, input_version, input_client_save_seq
        ):
            finished = await finish_pipeline_job(
                db, claim.job_id, claim.lease_token, "skipped"
            )
            if not finished:
                await db.rollback()
                raise PipelineStageBusy("流水线阶段租约已被接管")
            await _reset_owned_running_stage(db, script_id, stage, input_version)
            if reservation_id:
                await _release_pipeline(
                    db, reservation_id, "superseded by newer input"
                )
            await db.commit()
            return script_id
        await _call_mark_stage(
            db,
            script_id,
            episode_id,
            stage,
            "running",
            input_version,
            input_client_save_seq,
        )
        await db.commit()

    # 3. 执行阶段服务；后台 heartbeat 保持当前 token 的租约。
    service = _resolve_service(stage)
    stop_heartbeat = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(claim.job_id, claim.lease_token, stop_heartbeat)
    )
    try:
        async with async_session_factory() as db:
            kwargs: dict[str, object] = {
                "input_version": input_version,
                "input_client_save_seq": input_client_save_seq,
                "provider_attempt": claim.attempt,
                "commit": False,
            }
            # split 只消费触发分集的整段原文；extract 则必须遍历当前剧本的所有
            # 分集。若把本次触发的 episode_id 传给 extract，上传后拆出的第二集及
            # 后续分集永远不会进入本轮主体目录。
            if stage not in {"split", "extract"}:
                kwargs["episode_id"] = episode_id
            if stage in {"merge", "enrich", "parents"}:
                kwargs["run_id"] = run_id
            try:
                service_result = await service(db, script_id, *service_args, **kwargs)
                if not await _input_is_current(
                    db,
                    script_id,
                    episode_id,
                    input_version,
                    input_client_save_seq,
                    lock=True,
                ):
                    await db.rollback()
                    async with async_session_factory() as stale_db:
                        finished = await finish_pipeline_job(
                            stale_db, claim.job_id, claim.lease_token, "skipped"
                        )
                        if not finished:
                            await stale_db.rollback()
                            raise PipelineStageBusy("流水线阶段租约已被接管")
                        await _reset_owned_running_stage(
                            stale_db, script_id, stage, input_version
                        )
                        if reservation_id:
                            await _release_pipeline(
                                stale_db,
                                reservation_id,
                                "superseded by newer input",
                            )
                        await stale_db.commit()
                    return script_id
                job_status = "ready" if is_terminal else "done"
                finished = await finish_pipeline_job(
                    db,
                    claim.job_id,
                    claim.lease_token,
                    job_status,
                    output_json=(
                        service_result if isinstance(service_result, dict) else None
                    ),
                )
                if not finished:
                    await db.rollback()
                    return script_id
            except Exception:
                await db.rollback()
                raise
            await db.commit()
    except Exception as exc:  # noqa: BLE001
        # 失败：先 CAS job，只有当前 token 才能写 stage 终态并释放积分。
        async with async_session_factory() as db:
            failure = PipelineFailure.from_exception(exc)
            logger.error(
                "pipeline 阶段失败: stage=%s script_id=%s error_type=%s",
                stage,
                script_id,
                failure.error_type,
            )
            will_retry = (
                isinstance(exc, _AUTORETRY_FOR)
                and failure.retryable
                and claim.attempt < _MAX_ATTEMPTS
            )
            if will_retry and not await _lock_pipeline_retry_parents(
                db,
                claim.script_id,
                claim.episode_id,
            ):
                will_retry = False
            job_status = "failed"
            finished = await finish_pipeline_job(
                db, claim.job_id, claim.lease_token, job_status, failure
            )
            if finished:
                if will_retry:
                    await _insert_pipeline_retry_attempt(db, claim)
                    await _reset_owned_running_stage(
                        db, script_id, stage, input_version
                    )
                else:
                    await _call_mark_stage(
                        db,
                        script_id,
                        episode_id,
                        stage,
                        "failed",
                        input_version,
                        input_client_save_seq,
                        detail={"error": failure.error_message},
                        error=failure,
                    )
            await db.commit()
        if finished and not will_retry and reservation_id:
            async with async_session_factory() as release_db:
                await _release_pipeline(
                    release_db, reservation_id, failure.error_message
                )
                await release_db.commit()
        raise
    finally:
        stop_heartbeat.set()
        await heartbeat_task

    return script_id


async def _finalize_pipeline_async(
    script_id: str,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    run_id: str,
    input_client_save_seq: int | None = None,
) -> str:
    try:
        async with async_session_factory() as db:
            await finalize_pipeline_jobs(
                db,
                run_id=run_id,
                script_id=script_id,
                reservation_id=reservation_id,
                estimated=estimated,
            )
            await db.commit()
    except Exception as exc:
        failure = PipelineFailure.from_exception(exc)
        async with async_session_factory() as failure_db:
            result = await failure_db.execute(
                select(PipelineJob)
                .where(
                    PipelineJob.run_id == run_id,
                    PipelineJob.stage.in_(_TERMINAL_STAGES),
                    PipelineJob.status == "ready",
                )
                .options(noload(PipelineJob.script))
                .with_for_update()
            )
            for row in result.scalars().all():
                row.status = "failed"
                row.error_type = failure.error_type
                row.error_message = failure.error_message
                row.retryable = failure.retryable
                row.finished_at = datetime.now(UTC)
            for leaf in _TERMINAL_STAGES:
                await _call_mark_stage(
                    failure_db,
                    script_id,
                    episode_id,
                    leaf,
                    "failed",
                    input_version,
                    input_client_save_seq,
                    detail={"error": failure.error_message},
                    error=failure,
                )
            await failure_db.commit()
        if reservation_id:
            async with async_session_factory() as release_db:
                await _release_pipeline(
                    release_db, reservation_id, failure.error_message
                )
                await release_db.commit()
        raise
    return script_id


def _resolve_service(stage: str):
    """延迟导入阶段服务函数，避免循环依赖。"""
    import importlib

    module_name, func_name = _STAGE_SERVICE[stage]
    return getattr(importlib.import_module(module_name), func_name)


def _run_stage(
    stage: str,
    script_id: str | dict,
    episode_id: str,
    service_args: tuple,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
    retry_exhausted: bool = False,
    input_client_save_seq: int | None = None,
) -> str | dict:
    """同步入口：在 worker 持久事件循环中执行 async 主体。"""
    return run_worker_async(
        _run_stage_async(
            stage,
            script_id,
            episode_id,
            service_args,
            input_version,
            reservation_id,
            estimated,
            dispatch_id,
            run_id,
            retry_exhausted,
            input_client_save_seq=input_client_save_seq,
        )
    )


# --------------------------------------------------------------------------- #
# 任务定义 — DAG: split → extract → merge → { enrich ‖ parents }
# 每个任务返回 script_id，作为下一任务的首个位置参数；
# input_version / reservation_id / estimated 经 .s() 绑定贯穿链路。
# enrich 与 parents 用 chord header 并行，唯一 callback 负责结算。
# --------------------------------------------------------------------------- #
@celery_app.task(
    name="pipeline.split",
    bind=True,
    autoretry_for=_AUTORETRY_FOR,
    retry_backoff=True,
    max_retries=2,
)
def split_task(
    self,
    script_id: str | dict,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
) -> str | dict:
    """split 阶段：整段剧本文本 → 多集文本。input_version 绑定输入版本。"""
    return _run_stage(
        "split",
        script_id,
        episode_id,
        (episode_id,),
        input_version,
        reservation_id,
        estimated,
        dispatch_id,
        run_id,
        self.request.retries >= self.max_retries,
    )


@celery_app.task(
    name="pipeline.extract",
    bind=True,
    autoretry_for=_AUTORETRY_FOR,
    retry_backoff=True,
    max_retries=2,
)
def extract_task(
    self,
    script_id: str | dict,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
) -> str | dict:
    """extract 阶段：每集 → 主体列表。"""
    return _run_stage(
        "extract",
        script_id,
        episode_id,
        (),
        input_version,
        reservation_id,
        estimated,
        dispatch_id,
        run_id,
        self.request.retries >= self.max_retries,
    )


@celery_app.task(
    name="pipeline.merge",
    bind=True,
    autoretry_for=_AUTORETRY_FOR,
    retry_backoff=True,
    max_retries=2,
)
def merge_task(
    self,
    script_id: str | dict,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
) -> str | dict:
    """merge 阶段：跨集合并去重。"""
    return _run_stage(
        "merge",
        script_id,
        episode_id,
        (),
        input_version,
        reservation_id,
        estimated,
        dispatch_id,
        run_id,
        self.request.retries >= self.max_retries,
    )


@celery_app.task(
    name="pipeline.enrich",
    bind=True,
    autoretry_for=_AUTORETRY_FOR,
    retry_backoff=True,
    max_retries=2,
)
def enrich_task(
    self,
    script_id: str | dict,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
) -> str | dict:
    """enrich 阶段：设计线索增强（12 段落摄影级 prompt）。终态：结算积分。"""
    return _run_stage(
        "enrich",
        script_id,
        episode_id,
        (),
        input_version,
        reservation_id,
        estimated,
        dispatch_id,
        run_id,
        self.request.retries >= self.max_retries,
    )


@celery_app.task(
    name="pipeline.parents",
    bind=True,
    autoretry_for=_AUTORETRY_FOR,
    retry_backoff=True,
    max_retries=2,
)
def parents_task(
    self,
    script_id: str | dict,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
) -> str | dict:
    """parents 阶段：归纳父主体 + child_count。终态：结算积分。"""
    return _run_stage(
        "parents",
        script_id,
        episode_id,
        (),
        input_version,
        reservation_id,
        estimated,
        dispatch_id,
        run_id,
        self.request.retries >= self.max_retries,
    )


@celery_app.task(name="pipeline.finalize", bind=True, max_retries=0)
def finalize_task(
    self,
    leaf_results: list[str],
    script_id: str,
    episode_id: str,
    input_version: int,
    reservation_id: str | None,
    estimated: int,
    dispatch_id: str,
    run_id: str,
) -> str:
    """chord 唯一回调；只有两个叶子任务均成功返回后才执行。"""
    return run_worker_async(
        _finalize_pipeline_async(
            script_id,
            episode_id,
            input_version,
            reservation_id,
            estimated,
            run_id,
        )
    )
