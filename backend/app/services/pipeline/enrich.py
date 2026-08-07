"""enrich 阶段 — 并发生成、可复用且可降级的视觉主体 prompt。"""

import asyncio
import logging
import math
from typing import TypedDict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.pipeline_stage import PipelineStage
from app.models.subject import Subject
from app.providers import get_llm_provider
from app.schemas.materials import (
    CHARACTER_SHEET_MARKER,
    SUBJECT_DESIGN_PROMPT_MARKER,
    SUBJECT_DESIGN_PROMPT_TEMPLATE_VERSION,
    build_subject_prompt,
    ensure_subject_prompt_constraints,
)
from app.schemas.pipeline_contracts import (
    EnrichOutput,
    complete_json,
    normalize_enrich_prompt,
    should_enrich_subject,
)
from app.services.pipeline import (
    commit_stage_progress,
    get_run_stage_output,
    input_episode_is_current,
    set_stage,
)

logger = logging.getLogger(__name__)

class _ResolvedSubject(TypedDict):
    subject_id: str
    target_enrich_version: int
    entry: dict[str, object]


class CurrentDesignPrompt(TypedDict):
    prompt: str
    source: str
    version: int
    template_version: int

# ---- 扩展 prompt 提示词骨架（06 文档第 12.3 节）----
_SYSTEM = (
    "你是一名电影美术设定与生图提示词专家。请把主体事实整理成可直接用于视觉资产库的"
    "专业中文提示词，优先保证事实保真、跨视图一致和后续多图复用。成像媒介不得写死；"
    "主体事实未指定画风时，使用可被调用方风格安全覆盖的中性表述。"
)

_MIN_ENRICH_PROMPT_LENGTH = 300
_MIN_ENRICH_SECTION_COUNT = 3

_TYPE_REQUIREMENTS = {
    "character": (
        "角色版须覆盖身份事实、面容与视线、发型头饰、服装配色分层、真实体态、道具物理关系、"
        "时代考据、跨视图一致、白底布光、画面纯净和成像质感。必须原样保留标记"
        f"“{CHARACTER_SHEET_MARKER}”，并明确单张组合图中左侧主视觉、右侧正面/标准侧面/"
        "背面三幅完整全身视图。瞬时剧情动作与声音特征不得误当成标准设定姿态。"
    ),
    "scene": (
        "场景版须覆盖空间身份、前中后景布局、固定设施、材质痕迹、时代地域、物理尺度、"
        "光色连续性、16:9 主视角、画面纯净和镜头参数；默认不出现人物，不使用人物三视图。"
    ),
    "prop": (
        "道具版须覆盖形制尺度、部件结构、材质配色、使用痕迹、功能关系、时代考据、"
        "主视图加正侧背与细节视图、物理支撑、白底布光和产品摄影参数；默认不出现手和人物。"
    ),
}

_USER_TEMPLATE = """主体信息:
- 名称: {subject_name}
- 类型: {subject_type}
- 描述: {subject_description}

固定参考模板 prompt（请在此模板上下文基础上扩展，保持主体一致性与背景稳定）:
---
{base_prompt}
---

当前类型要求:
{type_requirements}

请输出 700～1200 个中文字符的高信息密度生图 prompt。可以使用清晰的编号段落，但不要为了
凑字重复内容。严格区分“主体事实”“合理补全”和“禁止项”：不得把原描述未提供的具体年龄、
族裔、颜色、伤疤、品牌或历史事件写成确定事实；原描述中的互斥选项必须选择一个稳定方案，
不得在同一张参考图中并列多个版本。必须原样保留标记“{design_prompt_marker}”。不要在 JSON 外添加说明。

输出格式: JSON，字段 final 为完整扩展 prompt 文本。
输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "resolved_prompts": {{ "final": "<完整扩展 prompt 文本>" }}
}}
"""


def build_base_prompt(subject: Subject) -> str:
    """构造分型详细 prompt，供 enrich 输入与失败降级共同使用。"""
    return build_subject_prompt(
        subject.name,
        subject.type,
        subject.description or "",
    )


def _prompt_meets_quality_floor(prompt: str) -> bool:
    """同时校验信息量与结构，避免极短分段文本冒充完整设定提示词。"""
    normalized = prompt.strip()
    return (
        len(normalized) >= _MIN_ENRICH_PROMPT_LENGTH
        and normalized.count("【") >= _MIN_ENRICH_SECTION_COUNT
    )


def _reusable_prompt(
    entry: object,
    *,
    enrich_version: int,
) -> str | None:
    """历史 prompt 只按主体 ID（由外层字典保证）和 enrich_version 复用。"""
    if not isinstance(entry, dict):
        return None
    # 基础模板只是供应商临时失败时的可用降级结果，不能永久阻止后续重试生成。
    if entry.get("source") == "base_fallback":
        return None
    if entry.get("prompt_template_version") != SUBJECT_DESIGN_PROMPT_TEMPLATE_VERSION:
        return None
    raw_version = entry.get("enrich_version")
    if not isinstance(raw_version, (int, str)):
        return None
    try:
        entry_version = int(raw_version)
    except (TypeError, ValueError):
        return None
    resolved = entry.get("resolved_prompts")
    final = resolved.get("final") if isinstance(resolved, dict) else None
    if entry_version != enrich_version or not isinstance(final, str) or not final.strip():
        return None
    normalized = normalize_enrich_prompt(final)
    return normalized if _prompt_meets_quality_floor(normalized) else None


def _merge_prompt_history(
    target: dict[str, dict],
    payload: object,
) -> None:
    """将较新的历史输出合入缓存，已有主体不被更老结果覆盖。"""
    if not isinstance(payload, dict):
        return
    prompts = payload.get("prompts")
    if not isinstance(prompts, dict):
        return
    for subject_id, entry in prompts.items():
        if isinstance(subject_id, str) and isinstance(entry, dict):
            target.setdefault(subject_id, entry)


async def _load_prompt_history(db: AsyncSession, script_id: str) -> dict[str, dict]:
    """读取公共阶段快照及最近成功 run 的私有输出，避免重复调用模型。"""
    history: dict[str, dict] = {}

    stage_result = await db.execute(
        select(PipelineStage.detail).where(
            PipelineStage.script_id == script_id,
            PipelineStage.stage == "enrich",
        )
    )
    stage_detail = stage_result.scalar_one_or_none()

    jobs_result = await db.execute(
        select(PipelineJob.output_json)
        .where(
            PipelineJob.script_id == script_id,
            PipelineJob.stage == "enrich",
            PipelineJob.status.in_(["done", "ready"]),
            PipelineJob.output_json.is_not(None),
        )
        .order_by(PipelineJob.created_at.desc())
        .limit(5)
    )
    for payload in jobs_result.scalars().all():
        _merge_prompt_history(history, payload)
    _merge_prompt_history(history, stage_detail)
    return history


def _episode_snapshot(
    subject: Subject,
    episode_snapshot_by_no: dict[int, dict[str, object]],
) -> dict[str, dict[str, object]]:
    episode_nos = sorted(
        int(no)
        for no in (subject.episode_nos or [])
        if int(no) in episode_snapshot_by_no
    )
    return {str(no): episode_snapshot_by_no[no] for no in episode_nos}


def _entry_version(value: object) -> int | None:
    if not isinstance(value, (int, str)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalized_episode_snapshot(value: object) -> dict[str, dict[str, object]] | None:
    if not isinstance(value, dict):
        return None
    normalized: dict[str, dict[str, object]] = {}
    for raw_no, raw_snapshot in value.items():
        if not isinstance(raw_snapshot, dict):
            return None
        try:
            episode_no = str(int(raw_no))
        except (TypeError, ValueError):
            return None
        normalized[episode_no] = raw_snapshot
    return normalized


async def load_current_design_prompts(
    db: AsyncSession,
    script_id: str,
    subjects: list[Subject],
    episode_snapshot_by_no: dict[int, dict[str, object]],
) -> dict[str, CurrentDesignPrompt]:
    """批量读取当前版本可用于画布的 enrich 提示词，避免主体列表产生 N+1。"""
    if not subjects:
        return {}
    detail = (
        await db.execute(
            select(PipelineStage.detail).where(
                PipelineStage.script_id == script_id,
                PipelineStage.stage == "enrich",
                PipelineStage.status == "done",
            )
        )
    ).scalar_one_or_none()
    prompt_entries = detail.get("prompts") if isinstance(detail, dict) else None
    if not isinstance(prompt_entries, dict):
        return {}

    resolved: dict[str, CurrentDesignPrompt] = {}
    for subject in subjects:
        if not should_enrich_subject(subject.type):
            continue
        entry = prompt_entries.get(subject.id)
        if not isinstance(entry, dict):
            continue
        subject_version = _entry_version(entry.get("subject_version"))
        enrich_version = _entry_version(entry.get("enrich_version"))
        if subject_version != int(subject.subject_version or 1):
            continue
        if enrich_version != int(subject.enrich_version or 1):
            continue
        stored_snapshot = _normalized_episode_snapshot(entry.get("episode_snapshot"))
        if stored_snapshot != _episode_snapshot(subject, episode_snapshot_by_no):
            continue
        resolved_payload = entry.get("resolved_prompts")
        final = resolved_payload.get("final") if isinstance(resolved_payload, dict) else None
        if not isinstance(final, str) or not final.strip():
            continue
        source = entry.get("source")
        template_version = _entry_version(entry.get("prompt_template_version")) or 1
        if template_version != SUBJECT_DESIGN_PROMPT_TEMPLATE_VERSION:
            continue
        try:
            prompt = ensure_subject_prompt_constraints(
                normalize_enrich_prompt(final),
                subject.type,
            )
        except ValueError:
            continue
        if not _prompt_meets_quality_floor(prompt):
            continue
        resolved[subject.id] = {
            "prompt": prompt,
            "source": source if isinstance(source, str) and source else "generated",
            "version": enrich_version,
            "template_version": template_version,
        }
    return resolved


async def run_enrich(
    db: AsyncSession,
    script_id: str,
    *,
    episode_id: str | None = None,
    input_version: int | None = None,
    input_client_save_seq: int | None = None,
    provider=None,
    provider_attempt: int | None = None,
    run_id: str | None = None,
    commit: bool = False,
    max_concurrency: int | None = None,
) -> int | dict:
    """并发增强视觉主体，返回已处理数量或 run 私有阶段产物。

    每个未命中缓存的主体最多调用模型一次。单项失败时写入可直接使用的基础
    prompt，不让失败扩散为阶段重试。``max_concurrency`` 可由调用方按供应商
    限流能力调整，默认 6。
    """
    # 阶段事务仍由 worker 统一提交；两个参数仅保留兼容既有调用。Enrich 单项
    # 最多外呼一次，不再把供应商格式异常升级为持久 attempt 重试。
    del commit, provider_attempt
    concurrency = max_concurrency or settings.pipeline_enrich_concurrency
    if concurrency < 1:
        raise ValueError("max_concurrency 必须大于 0")
    if not await input_episode_is_current(
        db, script_id, episode_id, input_version, input_client_save_seq
    ):
        return 0

    if run_id is None:
        subjects_result = await db.execute(
            select(Subject).where(
                Subject.script_id == script_id,
                Subject.is_active.is_(True),
            )
        )
        subjects = list(subjects_result.scalars().all())
    else:
        merge_output = await get_run_stage_output(db, run_id, "merge") or {}
        if not merge_output:
            raise ValueError("当前流水线 run 缺少 merge 暂存输出")
        subjects = [
            Subject(
                id=item["id"],
                script_id=script_id,
                name=item["name"],
                type=item["type"],
                description=item.get("description"),
                episode_nos=item.get("episode_nos") or [],
                subject_version=int(item.get("subject_version") or 1),
                enrich_version=int(item.get("enrich_version") or 1),
                is_active=True,
            )
            for item in merge_output.get("subjects", [])
        ]
    visual_subjects = [subject for subject in subjects if should_enrich_subject(subject.type)]
    total = len(visual_subjects)

    provider = provider or get_llm_provider()
    episode_rows = list(
        (
            await db.execute(
                select(Episode).where(Episode.script_id == script_id)
            )
        )
        .scalars()
        .all()
    )
    episode_rows = [episode for episode in episode_rows if hasattr(episode, "episode_no")]
    episode_snapshot_by_no = {
        int(episode.episode_no): {
            "episode_id": episode.episode_id,
            "episode_version": int(episode.episode_version or 1),
            "client_save_seq": int(episode.client_save_seq or 0),
        }
        for episode in episode_rows
    }

    prompt_history = await _load_prompt_history(db, script_id)

    # Worker 的 run 模式在进入服务前没有业务写；结束只读事务，避免等待模型的
    # 一两分钟内长期占用数据库连接。非 run 兼容旧的“由调用方统一提交”契约。
    if run_id is not None:
        await db.commit()

    semaphore = asyncio.Semaphore(concurrency)
    progress_lock = asyncio.Lock()
    provider_call_count = 0
    completed_count = 0
    completed_reused_count = 0
    completed_fallback_count = 0
    completed_prompts: dict[str, dict[str, object]] = {}
    checkpoint_step = max(1, math.ceil(total / 10))

    async def resolve_prompt(subject: Subject) -> _ResolvedSubject:
        nonlocal provider_call_count
        base_prompt = build_base_prompt(subject)
        current_enrich_version = int(subject.enrich_version or 1)
        cached = prompt_history.get(subject.id)
        reusable_prompt = _reusable_prompt(
            cached,
            enrich_version=current_enrich_version,
        )
        if reusable_prompt is not None:
            final_prompt = reusable_prompt
            source = "reused"
            fallback_error_type = None
            target_enrich_version = current_enrich_version
        else:
            target_enrich_version = (
                current_enrich_version
                if run_id is not None
                else current_enrich_version + 1
            )
            try:
                async with semaphore:
                    provider_call_count += 1
                    contract = await complete_json(
                        provider,
                        _SYSTEM,
                        _USER_TEMPLATE.format(
                            subject_name=subject.name,
                            subject_type=subject.type,
                            subject_description=subject.description or "",
                            base_prompt=base_prompt,
                            design_prompt_marker=SUBJECT_DESIGN_PROMPT_MARKER,
                            type_requirements=_TYPE_REQUIREMENTS.get(
                                subject.type,
                                "保持主体事实清晰、特征稳定和画面纯净。",
                            ),
                        ),
                        EnrichOutput,
                        max_attempts=1,
                    )
                final_prompt = contract.resolved_prompts.final
                if not _prompt_meets_quality_floor(final_prompt):
                    raise ValueError("enrich prompt 信息量不足")
                source = "generated"
                fallback_error_type = None
            except Exception as exc:  # noqa: BLE001
                final_prompt = base_prompt
                source = "base_fallback"
                fallback_error_type = getattr(exc, "error_type", type(exc).__name__)
                logger.warning(
                    "enrich 主体降级为基础 prompt: script_id=%s subject_id=%s error_type=%s",
                    script_id,
                    subject.id,
                    fallback_error_type,
                )

        final_prompt = ensure_subject_prompt_constraints(final_prompt, subject.type)

        entry: dict[str, object] = {
            "subject_version": int(subject.subject_version or 1),
            "enrich_version": target_enrich_version,
            "prompt_template_version": SUBJECT_DESIGN_PROMPT_TEMPLATE_VERSION,
            "resolved_prompts": {"final": final_prompt},
            "episode_snapshot": _episode_snapshot(subject, episode_snapshot_by_no),
            "source": source,
        }
        if fallback_error_type is not None:
            entry["fallback_error_type"] = fallback_error_type
        return _ResolvedSubject(
            subject_id=subject.id,
            target_enrich_version=target_enrich_version,
            entry=entry,
        )

    async def resolve_and_checkpoint(subject: Subject) -> _ResolvedSubject:
        nonlocal completed_count, completed_fallback_count, completed_reused_count
        result = await resolve_prompt(subject)
        async with progress_lock:
            entry = result["entry"]
            if isinstance(entry, dict):
                completed_count += 1
                completed_reused_count += int(entry.get("source") == "reused")
                completed_fallback_count += int(entry.get("source") == "base_fallback")
                completed_prompts[str(result["subject_id"])] = entry
            checkpoint_current = completed_count
            checkpoint_reused = completed_reused_count
            checkpoint_fallback = completed_fallback_count
            checkpoint_provider_calls = provider_call_count

            # 生产 worker 使用真实 AsyncSession；独立短事务使轮询能立即看到进度，
            # 同时不提交本服务会话中的业务结果。最多发布十个中间检查点，避免主体
            # 数量很大时制造过多小事务；测试替身无需连接外部数据库。
            should_checkpoint = (
                checkpoint_current == total or checkpoint_current % checkpoint_step == 0
            )
            if isinstance(db, AsyncSession) and should_checkpoint:
                try:
                    await commit_stage_progress(
                        script_id,
                        "enrich",
                        status="running",
                        current=checkpoint_current,
                        total=total,
                        detail={
                            "count": total,
                            "completed_count": checkpoint_current,
                            "reused_count": checkpoint_reused,
                            "fallback_count": checkpoint_fallback,
                            "provider_call_count": checkpoint_provider_calls,
                            # 使 worker 重试能复用已经成功生成的主体，而不只是显示计数。
                            "prompts": dict(completed_prompts),
                            "input_version": input_version,
                            "input_client_save_seq": input_client_save_seq,
                        },
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "enrich 进度检查点写入失败: script_id=%s current=%s total=%s",
                        script_id,
                        checkpoint_current,
                        total,
                        exc_info=True,
                    )
        return result

    results = await asyncio.gather(
        *(resolve_and_checkpoint(subject) for subject in visual_subjects)
    )
    result_by_subject_id = {str(item["subject_id"]): item for item in results}

    # 按主体原始顺序组装，保证 JSON 输出稳定，便于缓存、审计与测试。
    prompts: dict[str, dict[str, object]] = {}
    target_enrich_versions: dict[str, int] = {}
    for subject in visual_subjects:
        result = result_by_subject_id[subject.id]
        prompts[subject.id] = result["entry"]
        target_enrich_versions[subject.id] = int(result["target_enrich_version"])

    reused_count = sum(entry.get("source") == "reused" for entry in prompts.values())
    fallback_count = sum(
        entry.get("source") == "base_fallback" for entry in prompts.values()
    )

    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return 0

    if run_id is None:
        for subject in visual_subjects:
            subject.enrich_version = target_enrich_versions[subject.id]

    public_detail: dict[str, object] = {
        "count": total,
        "completed_count": total,
        "reused_count": reused_count,
        "fallback_count": fallback_count,
        "provider_call_count": provider_call_count,
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
    }
    if run_id is None:
        public_detail["prompts"] = prompts
    await set_stage(
        db,
        script_id,
        "enrich",
        status="running",
        current=total,
        total=total,
        detail=public_detail,
    )
    if run_id is not None:
        return {**public_detail, "prompts": prompts}
    return total
