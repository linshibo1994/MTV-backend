"""parents 阶段 — 父主体归纳（子主体集合 → 父主体 + child_count）。

默认按明确名称变体进行确定性归组；显式传入 provider 时保留模型归组。
落库到 parent_subjects 表，并回写子主体 is_parent / parent_id。
"""

import json
import re

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.script import replace_parent_subjects
from app.models.subject import Subject
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

_SCENE_VARIANT_SUFFIX = re.compile(
    r"(?:[-—_·/\s]*(?:内|外))?[-—_·/\s]*(?:日|夜|晨|清晨|黎明|黄昏|傍晚)$"
)
_CHARACTER_VARIANT_TERMS = (
    "幼年",
    "童年",
    "少年",
    "青年",
    "中年",
    "老年",
    "年轻",
    "年迈",
    "古装",
    "现代",
    "日常",
    "正装",
    "便装",
    "睡衣",
    "制服",
    "战损",
    "受伤",
    "妆容",
    "形态",
    "状态",
)
_BRACKETED_SUFFIX = re.compile(
    r"(?:（|\(|【|\[)(?P<variant>[^）)】\]]+)(?:）|\)|】|\])$"
)
_DELIMITED_SUFFIX = re.compile(r"[-—_·/]\s*(?P<variant>[^-—_·/]+)$")


def _deterministic_parent_name(subject: Subject) -> str:
    """仅折叠可明确识别的场景时段或角色形态，避免误合并不同实体。"""
    name = subject.name.strip()
    if subject.type == "scene":
        normalized = _SCENE_VARIANT_SUFFIX.sub("", name).strip("-—_·/ ")
        return normalized or name
    if subject.type != "character":
        return name

    for pattern in (_BRACKETED_SUFFIX, _DELIMITED_SUFFIX):
        match = pattern.search(name)
        if match is None:
            continue
        variant = match.group("variant").strip()
        if any(term in variant for term in _CHARACTER_VARIANT_TERMS):
            normalized = name[: match.start()].strip("-—_·/ ")
            return normalized or name
    return name


def _build_deterministic_parents(subjects: list[Subject]) -> list[dict]:
    """构造稳定、同类型且 child_id 唯一的父主体分组。"""
    groups: dict[tuple[str, str], list[Subject]] = {}
    for subject in subjects:
        parent_type = "background" if subject.type == "scene" else subject.type
        parent_name = _deterministic_parent_name(subject)
        groups.setdefault((parent_type, parent_name), []).append(subject)

    parents: list[dict] = []
    for (parent_type, parent_name), children in sorted(groups.items()):
        ordered_children = sorted(children, key=lambda item: (item.name, item.id))
        descriptions = list(
            dict.fromkeys(
                description
                for child in ordered_children
                if (description := (child.description or "").strip())
            )
        )
        parents.append(
            {
                "name": parent_name,
                "type": parent_type,
                "description": "；".join(descriptions) or f"{parent_name}主体集合",
                "child_count": len(ordered_children),
                "child_ids": [child.id for child in ordered_children],
            }
        )
    return parents


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

    if provider is None:
        parents_data = _build_deterministic_parents(subjects)
        grouping_mode = "deterministic"
    else:
        # 构造 LLM 输入：子主体精简信息。快照后释放只读事务，模型等待
        # 期间不占用数据库连接。
        subjects = [
            Subject(
                id=subject.id,
                script_id=script_id,
                name=subject.name,
                type=subject.type,
                description=subject.description,
                episode_nos=list(subject.episode_nos or []),
                is_active=True,
            )
            for subject in subjects
        ]
        merged_input = [
            {
                "subject_id": subject.id,
                "name": subject.name,
                "type": subject.type,
                "description": subject.description or "",
            }
            for subject in subjects
        ]
        if isinstance(db, AsyncSession):
            await db.rollback()
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
        grouping_mode = "model"

    child_ids = {subject.id for subject in subjects}
    child_types = {subject.id: subject.type for subject in subjects}
    assigned_ids: set[str] = set()
    valid_parents: list[dict] = []
    for parent in parents_data:
        ids = set(parent["child_ids"])
        if not ids <= child_ids:
            raise ValueError("parent child_ids 必须属于同一剧本且只能归属一个父主体")
        parent_type = "background" if parent.get("type") == "scene" else parent.get("type")
        if any(
            ("background" if child_types[child_id] == "scene" else child_types[child_id])
            != parent_type
            for child_id in ids
        ):
            # 父主体只是派生分组；模型跨类型归纳时丢弃该组，子主体保持未分组，
            # 不能因此阻断已完成的角色、场景、道具和声音主体发布。
            continue
        if assigned_ids & ids:
            raise ValueError("parent child_ids 必须属于同一剧本且只能归属一个父主体")
        assigned_ids |= ids
        parent["type"] = parent_type
        valid_parents.append(parent)
    parents_data = valid_parents

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
            "grouping_mode": grouping_mode,
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )
    if run_id is not None:
        return {
            "count": len(parents_data),
            "parent_subjects": parents_data,
            "grouping_mode": grouping_mode,
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        }
    return len(parents_data)
