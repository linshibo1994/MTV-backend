"""extract 阶段 — 主体提取（单集剧本文本 → 主体列表）。

严格遵循 06 文档第 12.1 节 LLM-1 提示词骨架与输入输出契约。
每集单独调用 LLM，并以有界并发执行；结果始终按集号稳定排序。
提取结果（每集主体）写入 pipeline_stage.detail 作为 merge 阶段的输入。
"""

import asyncio
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.episode import Episode
from app.models.pipeline_stage import PipelineStage
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import ExtractOutput, complete_json
from app.services.pipeline import (
    commit_stage_progress,
    input_episode_is_current,
    set_stage,
)

logger = logging.getLogger(__name__)

# ---- 提示词骨架（06 文档第 12.1 节）----
_SYSTEM = (
    "你是一名短剧剧本分析师。请从以下剧本文本中提取所有视觉与声音主体, "
    "包括角色(character)、场景(scene)、道具(prop)、声音/配音音色(sound)。"
)

_USER_TEMPLATE = """对每个主体, 生成:
- name: 主体名称 (简洁, 如 "王贵发"、"养鸡场办公室-内-夜")
- type: 类型 (character|scene|prop|sound)
- description: 详细描述 (角色: 外貌/服装/体态/表情; 场景: 空间布局/光线/氛围; 道具: 外观/材质/尺寸; 声音: 音色/语速/情感特征)
- is_child: 是否为子主体 (true)
- episode_nos: 出现的集数列表

要求:
1. description 必须足够详细, 可直接用作后续文生图/配音的基础 prompt
2. 跨集出现的同一主体必须使用相同的 name
3. 场景名格式: "地点-内/外-日/夜" (如 "养鸡场办公室-内-夜")

剧本文本:
---
{script_text}
---

输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "subjects": [
    {{"type": "character|scene|prop|sound", "name": "<主体名>", "description": "<详细描述>", "is_child": true, "episode_nos": [{episode_no}]}}
  ]
}}
"""


def _recover_completed_episodes(
    detail: object,
    episode_snapshots: list[dict[str, Any]],
    *,
    input_version: int | None,
    input_client_save_seq: int | None,
) -> dict[tuple[int, str], dict[str, Any]]:
    """从同一输入版本的检查点恢复已完成分集，避免 worker 重试时重复外呼。"""
    if not isinstance(detail, dict):
        return {}
    if detail.get("input_version") != input_version:
        return {}
    if detail.get("input_client_save_seq") != input_client_save_seq:
        return {}
    checkpoint_episodes = detail.get("episodes")
    if not isinstance(checkpoint_episodes, list):
        return {}
    current_by_key = {
        (int(item["episode_no"]), str(item["episode_id"])): item
        for item in episode_snapshots
    }
    recovered: dict[tuple[int, str], dict[str, Any]] = {}
    for item in checkpoint_episodes:
        if not isinstance(item, dict) or item.get("extract_status") != "succeeded":
            continue
        try:
            key = (int(item["episode_no"]), str(item["episode_id"]))
        except (KeyError, TypeError, ValueError):
            continue
        snapshot = current_by_key.get(key)
        subjects = item.get("subjects")
        if snapshot is None or not isinstance(subjects, list):
            continue
        recovered[key] = {
            "episode_id": snapshot["episode_id"],
            "episode_no": snapshot["episode_no"],
            "title": snapshot["title"],
            "extract_status": "succeeded",
            "subjects": subjects,
        }
    return recovered


class _ExtractProviderAdapter:
    """在严格 ExtractOutput 校验前写入当前阶段可确定的字段。"""

    def __init__(self, provider: Any, episode_no: int) -> None:
        self._provider = provider
        self._episode_no = episode_no

    async def complete_json(self, system: str, user: str) -> object:
        payload = await self._provider.complete_json(system, user)
        if not isinstance(payload, dict):
            return payload
        subjects = payload.get("subjects")
        if not isinstance(subjects, list):
            return payload
        normalized_payload = dict(payload)
        normalized_subjects: list[object] = []
        for subject in subjects:
            if not isinstance(subject, dict):
                normalized_subjects.append(subject)
                continue
            normalized_subject = dict(subject)
            normalized_subject["is_child"] = True
            normalized_subject["episode_nos"] = [self._episode_no]
            normalized_subjects.append(normalized_subject)
        normalized_payload["subjects"] = normalized_subjects
        return normalized_payload


async def run_extract(
    db: AsyncSession,
    script_id: str,
    *,
    episode_id: str | None = None,
    input_version: int | None = None,
    input_client_save_seq: int | None = None,
    provider=None,
    provider_attempt: int | None = None,
    commit: bool = False,
) -> dict:
    """对所有集逐集提取主体，返回 {episodes: [{episode_id, episode_no, subjects}]}。

    中间结果写入 pipeline_stage.detail，供 merge 阶段消费。
    """
    if not await input_episode_is_current(
        db, script_id, episode_id, input_version, input_client_save_seq
    ):
        return {"episodes": []}

    checkpoint_detail: object = None
    if isinstance(db, AsyncSession):
        checkpoint_detail = (
            await db.execute(
                select(PipelineStage.detail).where(
                    PipelineStage.script_id == script_id,
                    PipelineStage.stage == "extract",
                )
            )
        ).scalar_one_or_none()

    query = select(Episode).where(Episode.script_id == script_id)
    if episode_id is not None:
        query = query.where(Episode.episode_id == episode_id)
    episodes_result = await db.execute(query.order_by(Episode.episode_no.asc()))
    episodes = list(episodes_result.scalars().all())
    if episode_id is not None:
        episodes = [episode for episode in episodes if episode.episode_id == episode_id]
    episode_snapshots: list[dict[str, Any]] = [
        {
            "episode_id": episode.episode_id,
            "episode_no": int(episode.episode_no),
            "title": episode.title,
            "script_text": str(episode.script_text or ""),
        }
        for episode in episodes
    ]
    # 模型调用期间不需要数据库；结束只读事务，避免长时间占用连接池。
    if isinstance(db, AsyncSession):
        await db.rollback()
    total = len(episode_snapshots)
    provider = provider or get_llm_provider()
    semaphore = asyncio.Semaphore(settings.pipeline_extract_concurrency)
    progress_lock = asyncio.Lock()
    completed = _recover_completed_episodes(
        checkpoint_detail,
        episode_snapshots,
        input_version=input_version,
        input_client_save_seq=input_client_save_seq,
    )
    publish_progress = isinstance(db, AsyncSession)

    async def extract_episode(episode: dict[str, Any]) -> dict[str, Any]:
        script_text = str(episode["script_text"]).strip()
        if not script_text:
            subjects: list[dict[str, Any]] = []
        else:
            expected_episode_no = int(episode["episode_no"])
            async with semaphore:
                contract = await complete_json(
                    _ExtractProviderAdapter(provider, expected_episode_no),
                    _SYSTEM,
                    _USER_TEMPLATE.format(
                        script_text=script_text,
                        episode_no=expected_episode_no,
                    ),
                    ExtractOutput,
                    durable_attempt=provider_attempt,
                )
            subjects = [subject.model_dump() for subject in contract.subjects]
        result = {
            "episode_id": episode["episode_id"],
            "episode_no": episode["episode_no"],
            "title": episode["title"],
            "extract_status": "succeeded",
            "subjects": subjects,
        }
        if publish_progress:
            async with progress_lock:
                completed[(int(result["episode_no"]), str(result["episode_id"]))] = result
                visible_episodes = [
                    completed[key] for key in sorted(completed)
                ]
                try:
                    await commit_stage_progress(
                        script_id,
                        "extract",
                        status="running",
                        current=len(visible_episodes),
                        total=total,
                        detail={
                            "episodes": visible_episodes,
                            "input_version": input_version,
                            "input_client_save_seq": input_client_save_seq,
                        },
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "extract 进度检查点写入失败: script_id=%s current=%s total=%s",
                        script_id,
                        len(visible_episodes),
                        total,
                        exc_info=True,
                    )
        return result

    pending_snapshots = [
        item
        for item in episode_snapshots
        if (int(item["episode_no"]), str(item["episode_id"])) not in completed
    ]
    newly_completed = await asyncio.gather(
        *(extract_episode(episode) for episode in pending_snapshots)
    )
    for item in newly_completed:
        completed[(int(item["episode_no"]), str(item["episode_id"]))] = item
    per_episode = list(completed.values())
    per_episode.sort(key=lambda item: (int(item["episode_no"]), str(item["episode_id"])))
    # 权威业务事务只写一次完整结果；模型执行期间的可见进度已由独立短事务
    # 检查点发布，避免在主事务中按集反复 flush。
    await set_stage(
        db,
        script_id,
        "extract",
        status="running",
        current=total,
        total=total,
        detail={
            "episodes": per_episode,
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )
    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return {"episodes": []}

    await set_stage(
        db,
        script_id,
        "extract",
        status="done",
        current=total,
        total=total,
        detail={
            "episodes": per_episode,
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )
    return {"episodes": per_episode}
