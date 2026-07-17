"""parents 阶段 — 父主体归纳（子主体集合 → 父主体 + child_count）。

严格遵循 06 文档第 12.4 节 LLM-4 提示词骨架与输入输出契约。
落库到 parent_subjects 表，并回写子主体 is_parent / parent_id。
"""

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.script import replace_parent_subjects
from app.models.subject import Subject
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import ParentsOutput, complete_json
from app.services.pipeline import get_run_stage_output, input_episode_is_current, set_stage

# ---- 提示词骨架（06 文档第 12.4 节）----
_SYSTEM = "你是一名短剧剧本分析师。请将去重后的子主体列表归纳为父主体。"

_USER_TEMPLATE = """以下是去重后的子主体列表, 请将同类子主体 (如同一角色的不同形态/同一场景的不同时段) 归纳为父主体。

归纳规则:
1. 同一角色不同状态/形态 -> 归为一个父主体, child_count = 形态数
2. 同一场景不同时段 (日/夜) -> 可归为一个父主体
3. 父主体 name 取代表性名称, description 合并概括
4. 无关联的独立子主体不归纳, 单独作为父主体 (child_count=1) 或留在 "其余子主体"

子主体列表:
---
{merged_subjects_json}
---

输出 JSON 数组, 每个元素为一个父主体:
{{
  "schema_version": "pipeline.v1",
  "parent_subjects": [
    {{"name": "<父主体名>", "type": "character|background|prop|sound", "description": "<合并概括描述>", "child_count": 2, "child_ids": ["<uuid>", "<uuid>"]}}
  ]
}}
"""


async def run_parents(
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
) -> int | dict:
    """归纳父主体，返回父主体数量。"""
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
                is_active=True,
            )
            for item in merge_output.get("subjects", [])
        ]

    # 构造 LLM 输入：子主体精简信息
    merged_input = [
        {
            "subject_id": s.id,
            "name": s.name,
            "type": s.type,
            "description": s.description or "",
        }
        for s in subjects
    ]

    provider = provider or get_llm_provider()
    contract = await complete_json(
        provider,
        _SYSTEM,
        _USER_TEMPLATE.format(
            merged_subjects_json=json.dumps(merged_input, ensure_ascii=False)
        ),
        ParentsOutput,
        durable_attempt=provider_attempt,
    )
    parents_data = [parent.model_dump() for parent in contract.parent_subjects]

    child_ids = {subject.id for subject in subjects}
    child_types = {subject.id: subject.type for subject in subjects}
    assigned_ids: set[str] = set()
    for parent in parents_data:
        ids = set(parent["child_ids"])
        if not ids <= child_ids or assigned_ids & ids:
            raise ValueError("parent child_ids 必须属于同一剧本且只能归属一个父主体")
        parent_type = "background" if parent.get("type") == "scene" else parent.get("type")
        for child_id in ids:
            child_type = child_types[child_id]
            normalized_child_type = "background" if child_type == "scene" else child_type
            if normalized_child_type != parent_type:
                raise ValueError("父子主体类型必须一致")
        assigned_ids |= ids

    # 归一化类型（背景与场景同义，统一存 background）
    for item in parents_data:
        if item.get("type") == "scene":
            item["type"] = "background"

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
        await replace_parent_subjects(db, script_id, parents_data)

    await set_stage(
        db,
        script_id,
        "parents",
        status="running",
        detail={
            "count": len(parents_data),
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )
    if run_id is not None:
        return {
            "count": len(parents_data),
            "parent_subjects": parents_data,
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        }
    return len(parents_data)
