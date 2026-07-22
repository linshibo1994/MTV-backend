"""split 阶段 — 剧本拆分（整段剧本文本 → 多集文本）。

输入: 触发分集的 script_text（视为整段剧本文本）
输出: 多个 episode（episode_no / title / script_text），按 (script_id, episode_no) upsert
"""

import logging
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ids import new_id
from app.models.episode import Episode
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import SplitOutput, complete_json
from app.services.pipeline import input_episode_is_current, set_stage

logger = logging.getLogger(__name__)

# ---- 提示词骨架（遵循 06 文档第 1 阶段语义）----
_SYSTEM = (
    "你是一名短剧剧本编辑。请将用户提供的整段剧本文本拆分为多集，"
    "每集包含集号、标题与该集对应的剧本文本。保持原文内容完整，"
    "仅按剧情节点切分，不要改写或增删剧情。"
)

_USER_TEMPLATE = """请将以下整段剧本文本拆分为多集，输出 JSON 数组。

要求:
1. episode_no 从 1 开始连续编号
2. title 形如 "第 1 集" 或带剧情小标题
3. script_text 为该集完整剧本文本（不可截断、不可改写）
4. 多集拼接后内容须覆盖原文全部剧情

整段剧本文本:
---
{script_text}
---

输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "episodes": [
    {{"episode_no": 1, "title": "第 1 集", "script_text": "<该集剧本文本>"}}
  ]
}}
"""


async def run_split(
    db: AsyncSession,
    script_id: str,
    episode_id: str,
    *,
    input_version: int | None = None,
    input_client_save_seq: int | None = None,
    provider=None,
    provider_attempt: int | None = None,
    commit: bool = False,
) -> int:
    """执行 split 阶段，返回拆分得到的集数；事务由调用方统一结束。"""
    if not await input_episode_is_current(
        db, script_id, episode_id, input_version, input_client_save_seq
    ):
        return 0
    episode = await db.get(Episode, episode_id)
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
    script_text = (episode.script_text if episode else None) or ""
    existing_result = await db.execute(
        select(Episode)
        .where(Episode.script_id == script_id)
        .order_by(Episode.episode_no.asc())
    )
    existing_episodes = list(existing_result.scalars().all())
    trigger_episode_no = int(episode.episode_no) if episode else None
    split_mode = (
        "single_episode"
        if trigger_episode_no is not None and len(existing_episodes) > 1
        else "full_script"
    )

    # 已经拆分为多集后，保存其中一集只应触发该剧本的重新分析，不能将单集正文
    # 再交给模型按“从第 1 集开始”拆分。否则模型常返回第 1 集，后续按触发集号
    # 过滤时会得到空集，造成状态与实际分集脱节。
    if split_mode == "single_episode":
        if not await input_episode_is_current(
            db,
            script_id,
            episode_id,
            input_version,
            input_client_save_seq,
            lock=True,
        ):
            return 0
        await set_stage(
            db,
            script_id,
            "split",
            status="done",
            detail={
                "count": 1,
                "mode": split_mode,
                "active_episode_nos": [trigger_episode_no],
                "input_version": input_version,
                "input_client_save_seq": input_client_save_seq,
            },
        )
        return 1

    if not script_text.strip():
        if not await input_episode_is_current(
            db,
            script_id,
            episode_id,
            input_version,
            input_client_save_seq,
            lock=True,
        ):
            return 0
        # 无文本，直接标记完成
        await set_stage(
            db,
            script_id,
                "split",
                status="done",
                detail={
                    "count": 0,
                    "mode": split_mode,
                    "active_episode_nos": [],
                    "input_version": input_version,
                    "input_client_save_seq": input_client_save_seq,
                },
            )
        return 0

    provider = provider or get_llm_provider()
    contract = await complete_json(
        provider,
        _SYSTEM,
        _USER_TEMPLATE.format(script_text=script_text),
        SplitOutput,
        durable_attempt=provider_attempt,
    )
    episodes_data = [item.model_dump() for item in contract.episodes]
    # LLM 返回后再次检查用户是否已保存新版文本；过期任务不得覆盖任何分集。
    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return 0

    # 按 (script_id, episode_no) upsert
    # 已有集采用带 WHERE episode_version=? 的原子更新，与 save_episode 乐观锁语义一致，
    # 避免与用户并发编辑同一 episode 产生竞态覆盖；冲突时跳过该集（保留用户最新文本）。
    from sqlalchemy import update

    for item in episodes_data:
        episode_no = int(item.get("episode_no", 0))
        if episode_no <= 0:
            continue
        existing = await db.execute(
            select(Episode).where(
                Episode.script_id == script_id,
                Episode.episode_no == episode_no,
            )
        )
        ep = existing.scalar_one_or_none()
        new_title = str(item.get("title", f"第 {episode_no} 集"))
        new_text = item.get("script_text")
        if ep is None:
            db.add(
                Episode(
                    episode_id=new_id(),
                    script_id=script_id,
                    episode_no=episode_no,
                    title=new_title,
                    script_text=new_text,
                    schema_version=1,
                    episode_version=1,
                    client_save_seq=0,
                )
            )
        else:
            # 原子更新：仅在 episode_version 未变时覆盖，避免覆盖用户并发保存
            cur_version = ep.episode_version or 1
            # 触发分集的版本代表本次用户输入；split 的派生覆盖不能制造一个
            # 新输入版本，否则同一 run 的后续阶段会把自己误判为旧任务。
            stmt = (
                update(Episode)
                .where(
                    Episode.episode_id == ep.episode_id,
                    Episode.script_id == script_id,
                    Episode.episode_version == cur_version,
                )
                .values(
                    script_text=new_text if new_text is not None else ep.script_text,
                    title=new_title,
                    episode_version=cur_version,
                )
            )
            result = cast(CursorResult[Any], await db.execute(stmt))
            if result.rowcount == 0:
                # 版本已变（用户并发保存），跳过该集，保留用户最新文本
                logger.info(
                    "split 跳过集 %s：episode_version 已变（用户并发保存）", episode_no
                )
                continue
    await set_stage(
        db,
        script_id,
        "split",
        status="done",
        detail={
            "count": len(episodes_data),
            "mode": split_mode,
            "active_episode_nos": [
                int(item["episode_no"]) for item in episodes_data if item.get("episode_no")
            ],
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )
    return len(episodes_data)
