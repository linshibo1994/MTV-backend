"""merge 阶段 — 跨集主体合并（多集主体列表 → 去重子主体集合）。

严格遵循 06 文档第 12.2 节 LLM-2 提示词骨架与输入输出契约。
读取 extract 阶段写入的 detail，调用 LLM 合并后落库到 subjects 表。
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ids import new_id
from app.crud.script import replace_subjects
from app.models.subject import Subject
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import MergeOutput, complete_json
from app.services.pipeline import (
    get_run_stage_output,
    get_stage,
    input_episode_is_current,
    set_stage,
)

# ---- 提示词骨架（06 文档第 12.2 节）----
_SYSTEM = "你是一名短剧剧本分析师。请将多集提取的主体列表中跨集出现的同一主体合并, episode_nos 取并集。"

_USER_TEMPLATE = """以下是多集提取的主体列表, 请将跨集出现的同一主体 (同名同类型) 合并, episode_nos 取并集。

输入主体列表:
---
{multi_episode_subjects_json}
---

合并规则:
1. name 相同且 type 相同的主体视为同一主体, 合并
2. description 保留最详细的版本, 或合并补充
3. episode_nos 取并集并排序
4. 保留所有不重复的主体

输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "merged_subjects": [
    {{"name": "<主体名>", "type": "character|scene|prop|sound", "description": "<合并后描述>", "episode_nos": [1, 2], "is_child": true}}
  ],
  "total_count": 44
}}
"""


async def _prepare_staged_subjects(
    db: AsyncSession, script_id: str, merged_subjects: list[dict]
) -> list[dict]:
    """为暂存主体分配稳定 ID 和发布时要校验的版本栅栏。"""
    existing = list(
        (
            await db.execute(select(Subject).where(Subject.script_id == script_id))
        )
        .scalars()
        .all()
    )
    existing_by_key = {(subject.type, subject.name): subject for subject in existing}
    staged: list[dict] = []
    for raw in merged_subjects:
        item = dict(raw)
        subject = existing_by_key.get((item["type"], item["name"]))
        if subject is None:
            item.update(
                id=new_id(),
                base_subject_version=None,
                base_enrich_version=None,
                subject_version=1,
                enrich_version=1,
            )
        else:
            changed = (
                subject.description != item.get("description")
                or bool(subject.is_child) != bool(item.get("is_child", True))
                or sorted(subject.episode_nos or [])
                != sorted(item.get("episode_nos") or [])
                or not subject.is_active
            )
            base_subject_version = int(subject.subject_version or 1)
            base_enrich_version = int(subject.enrich_version or 1)
            item.update(
                id=subject.id,
                base_subject_version=base_subject_version,
                base_enrich_version=base_enrich_version,
                subject_version=base_subject_version + int(changed),
                enrich_version=(
                    base_enrich_version + 1
                    if item.get("type") != "sound"
                    else base_enrich_version
                ),
            )
        staged.append(item)
    return staged


async def run_merge(
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
    """执行 merge 阶段，返回合并后的子主体数量。"""
    if not await input_episode_is_current(
        db, script_id, episode_id, input_version, input_client_save_seq
    ):
        return 0
    # 读取 extract 阶段中间结果
    if run_id is not None:
        extract_detail = await get_run_stage_output(db, run_id, "extract") or {}
        if not extract_detail:
            raise ValueError("当前流水线 run 缺少 extract 暂存输出")
    else:
        extract_stage = await get_stage(db, script_id, "extract")
        extract_detail = (extract_stage.detail if extract_stage else None) or {}
    per_episodes = extract_detail.get("episodes", []) or []

    # 汇总所有集的主体作为 LLM 输入
    all_subjects: list[dict] = []
    for ep in per_episodes:
        for subj in ep.get("subjects", []) or []:
            entry = {
                "name": subj.get("name"),
                "type": subj.get("type"),
                "description": subj.get("description"),
                "episode_nos": subj.get("episode_nos") or [ep.get("episode_no")],
            }
            all_subjects.append(entry)

    import json

    required_episode_nos: dict[tuple[str, str], set[int]] = {}
    for subject in all_subjects:
        key = (str(subject["type"]), str(subject["name"]))
        required_episode_nos.setdefault(key, set()).update(subject["episode_nos"])

    def validate_keyset(contract: MergeOutput) -> None:
        output_keys = {
            (subject.type, subject.name) for subject in contract.merged_subjects
        }
        if output_keys != set(required_episode_nos):
            raise ValueError("merge 输出主体集合与输入不一致")
        for subject in contract.merged_subjects:
            key = (subject.type, subject.name)
            if set(subject.episode_nos) != required_episode_nos.get(key, set()):
                raise ValueError("merge 输出 episode_nos 必须与 extract 输入一致")

    provider = provider or get_llm_provider()
    contract = await complete_json(
        provider,
        _SYSTEM,
        _USER_TEMPLATE.format(
            multi_episode_subjects_json=json.dumps(all_subjects, ensure_ascii=False)
        ),
        MergeOutput,
        durable_attempt=provider_attempt,
        validator=validate_keyset,
    )
    merged_subjects = [subject.model_dump() for subject in contract.merged_subjects]
    for subject in merged_subjects:
        key = (subject["type"], subject["name"])
        subject["episode_nos"] = sorted(required_episode_nos.get(key, set()))

    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return 0

    staged_subjects: list[dict] | None = None
    if run_id is None:
        await replace_subjects(db, script_id, merged_subjects)
    else:
        staged_subjects = await _prepare_staged_subjects(db, script_id, merged_subjects)
    await set_stage(
        db,
        script_id,
        "merge",
        status="done",
        detail={
            "count": len(merged_subjects),
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )
    if staged_subjects is not None:
        return {"count": len(staged_subjects), "subjects": staged_subjects}
    return len(merged_subjects)
