"""剧本处理流水线服务包 — split / extract / merge / enrich / parents。

阶段顺序: split → extract → merge → enrich → parents
每阶段:
  1. 标记 pipeline_stage 为 running
  2. 构造 LLM prompt（严格遵循 06 文档第 12 节契约）
  3. 调 app.providers.llm.complete_json
  4. 落库 subjects/parent_subjects 或写 pipeline_stage.detail
  5. 更新 pipeline_stage 状态为 done / failed
"""

from sqlalchemy import case, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core import database as database_core
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.pipeline_stage import PipelineStage

# 阶段顺序与中文名
STAGE_ORDER: list[str] = ["split", "extract", "merge", "enrich", "parents"]
STAGE_LABELS: dict[str, str] = {
    "split": "剧集拆分",
    "extract": "提取主体",
    "merge": "合并主体",
    "enrich": "设计线索增强",
    "parents": "归纳父主体",
}

__all__ = [
    "commit_stage_progress",
    "get_stage",
    "get_run_stage_output",
    "input_episode_is_current",
    "set_stage",
    "STAGE_ORDER",
    "STAGE_LABELS",
]


async def commit_stage_progress(
    script_id: str,
    stage: str,
    *,
    status: str | None = None,
    detail: dict | None = None,
    current: int | None = None,
    total: int | None = None,
) -> bool:
    """用独立短事务发布可轮询进度，不提交或占用调用方的业务事务。

    ``current``/``total`` 只允许单调递增，避免并发任务完成顺序不同导致前端
    进度倒退。流水线触发时已预建五个阶段；若阶段不存在则安全忽略检查点，
    后续权威阶段提交仍会通过 :func:`set_stage` 创建记录。
    """
    criteria = [
        PipelineStage.script_id == script_id,
        PipelineStage.stage == stage,
    ]
    values: dict[str, object] = {"updated_at": func.now()}
    if status is not None:
        values["status"] = status
    if detail is not None:
        values["detail"] = detail
    if current is not None:
        values["current"] = case(
            (
                (PipelineStage.current.is_(None)) | (PipelineStage.current < current),
                current,
            ),
            else_=PipelineStage.current,
        )
    if total is not None:
        values["total"] = case(
            (
                (PipelineStage.total.is_(None)) | (PipelineStage.total < total),
                total,
            ),
            else_=PipelineStage.total,
        )
    # 进度检查点只能更新 worker 已领取的当前 running 阶段。输入版本直接从
    # 调用方即将持久化的 detail 取值，封住旧 run 迟到写覆盖新 run 的窗口。
    if status == "running" or current is not None or total is not None:
        criteria.append(PipelineStage.status == "running")
    if detail is not None:
        expected_input_version = detail.get("input_version")
        if isinstance(expected_input_version, int):
            criteria.append(
                PipelineStage.detail["input_version"].as_integer()
                == expected_input_version
            )
        expected_save_seq = detail.get("input_client_save_seq")
        if isinstance(expected_save_seq, int):
            criteria.append(
                PipelineStage.detail["input_client_save_seq"].as_integer()
                == expected_save_seq
            )

    async with database_core.async_session_factory() as progress_db:
        result = await progress_db.execute(
            update(PipelineStage)
            .where(*criteria)
            .values(**values)
        )
        updated = bool(getattr(result, "rowcount", 0))
        await progress_db.commit()
    return updated


async def get_run_stage_output(
    db: AsyncSession, run_id: str, stage: str
) -> dict | None:
    """读取同一 run 某阶段最新成功 attempt 的暂存输出。"""
    result = await db.execute(
        select(PipelineJob.output_json)
        .where(
            PipelineJob.run_id == run_id,
            PipelineJob.stage == stage,
            PipelineJob.status.in_(["done", "ready"]),
            PipelineJob.output_json.is_not(None),
        )
        .order_by(PipelineJob.attempt.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def input_episode_is_current(
    db: AsyncSession,
    script_id: str,
    episode_id: str | None,
    input_version: int | None,
    input_client_save_seq: int | None = None,
    *,
    lock: bool = False,
) -> bool:
    """从真实 Episode 行校验输入版本；写回前可加行锁封闭竞态窗口。"""
    if episode_id is None or input_version is None:
        return True
    episode = await db.get(
        Episode,
        episode_id,
        populate_existing=True,
        with_for_update=lock,
    )
    return bool(
        episode is not None
        and episode.script_id == script_id
        and episode.episode_version == input_version
        and (
            input_client_save_seq is None
            or episode.client_save_seq == input_client_save_seq
        )
    )


async def get_stage(db: AsyncSession, script_id: str, stage: str) -> PipelineStage:
    """获取某阶段状态行，不存在则创建（status=pending）。"""
    result = await db.execute(
        select(PipelineStage)
        .where(
            PipelineStage.script_id == script_id,
            PipelineStage.stage == stage,
        )
        .options(noload(PipelineStage.script))
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = PipelineStage(
            script_id=script_id,
            stage=stage,
            status="pending",
        )
        db.add(row)
        await db.flush()
    return row


async def set_stage(
    db: AsyncSession,
    script_id: str,
    stage: str,
    status: str | None = None,
    detail: dict | None = None,
    current: int | None = None,
    total: int | None = None,
) -> PipelineStage:
    """更新某阶段状态（字段为 None 时保持原值）。"""
    row = await get_stage(db, script_id, stage)
    if status is not None:
        row.status = status
    if detail is not None:
        row.detail = detail
    if current is not None:
        row.current = current
    if total is not None:
        row.total = total
    await db.flush()
    return row
