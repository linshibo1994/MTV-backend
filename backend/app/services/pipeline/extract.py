"""extract 阶段 — 主体提取（单集剧本文本 → 主体列表）。

严格遵循 06 文档第 12.1 节 LLM-1 提示词骨架与输入输出契约。
每集单独调用 LLM，逐集更新 current/total 进度。
提取结果（每集主体）写入 pipeline_stage.detail 作为 merge 阶段的输入。
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.episode import Episode
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import ExtractOutput, complete_json
from app.services.pipeline import input_episode_is_current, set_stage

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

    query = select(Episode).where(Episode.script_id == script_id)
    if episode_id is not None:
        query = query.where(Episode.episode_id == episode_id)
    episodes_result = await db.execute(query.order_by(Episode.episode_no.asc()))
    episodes = list(episodes_result.scalars().all())
    if episode_id is not None:
        episodes = [episode for episode in episodes if episode.episode_id == episode_id]
    total = len(episodes)
    provider = provider or get_llm_provider()
    per_episode: list[dict] = []
    for idx, episode in enumerate(episodes, start=1):
        script_text = (episode.script_text or "").strip()
        if not script_text:
            per_episode.append(
                {
                    "episode_id": episode.episode_id,
                    "episode_no": episode.episode_no,
                    "title": episode.title,
                    "extract_status": "succeeded",
                    "subjects": [],
                }
            )
        else:
            expected_episode_no = int(episode.episode_no)

            def validate_current_episode(contract: ExtractOutput) -> None:
                for subject in contract.subjects:
                    if set(subject.episode_nos) != {expected_episode_no}:
                        raise ValueError("extract 输出 episode_nos 必须等于当前集")

            contract = await complete_json(
                provider,
                _SYSTEM,
                _USER_TEMPLATE.format(
                    script_text=script_text, episode_no=episode.episode_no
                ),
                ExtractOutput,
                durable_attempt=provider_attempt,
                validator=validate_current_episode,
            )
            subjects = [subject.model_dump() for subject in contract.subjects]
            per_episode.append(
                {
                    "episode_id": episode.episode_id,
                    "episode_no": episode.episode_no,
                    "title": episode.title,
                    "extract_status": "succeeded",
                    "subjects": subjects,
                }
            )
        await set_stage(
            db,
            script_id,
            "extract",
            status="running",
            current=idx,
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
