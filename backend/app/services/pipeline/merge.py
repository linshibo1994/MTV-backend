"""merge 阶段 — 跨集主体合并（多集主体列表 → 去重子主体集合）。

读取 extract 阶段写入的 detail，默认使用确定性去重基线；显式传入 provider
时保留 LLM 补漏增强能力。最终结果落库到 subjects 表。
"""

from typing import Any

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.ids import new_id
from app.crud import asset as asset_crud
from app.crud.script import replace_parent_subjects, replace_subjects
from app.models.episode import Episode
from app.models.subject import Subject
from app.models.subject_candidate import SubjectCandidate
from app.schemas.pipeline_contracts import (
    MergeOutput,
    PipelineSubject,
    ProviderContractError,
    SupplementalSubject,
    complete_json,
)
from app.services.pipeline import (
    get_run_stage_output,
    get_stage,
    input_episode_is_current,
    set_stage,
)
from app.services.subject_candidate_evidence import normalize_candidate_evidence

# ---- 提示词骨架（06 文档第 12.2 节）----
_SYSTEM = (
    "你是一名短剧剧本分析师。请将多集提取的主体列表中跨集出现的同一主体合并, "
    "episode_nos 取并集；同时重新核对分集原文，补出首次提取漏掉的可制作主体。"
    "必须逐类检查角色(character)、场景(scene)、道具(prop)、声音/配音音色(sound)，"
    "但原文没有可制作实体的类别可以为空。merge 阶段的产物只能是子主体：is_child "
    "是服务端固定语义，每一项必须输出 JSON 布尔值 true，严禁输出 false、字符串或省略该字段。"
)

_USER_TEMPLATE = """以下是多集提取的主体列表，以及对应的分集原文。请先合并首次提取结果，再基于原文补漏。

输入主体列表:
---
{multi_episode_subjects_json}
---

分集原文:
---
{episodes_json}
---

合并规则:
1. name 相同且 type 相同的主体视为同一主体, 合并
2. description 保留最详细的版本, 或合并补充
3. episode_nos 取并集并排序
4. 保留所有不重复的主体
5. merged_subjects 必须逐项保留输入主体；不得删除、改名或新增输入之外的主体
6. supplemental_subjects 只放原文中存在、但输入主体列表遗漏的角色、场景、道具或声音；每项至少给出一条逐字原文证据 quote，并填写该证据所在 episode_no
7. 不要把每句台词当作独立声音；声音主体应为说话人音色、旁白、环境音、音效或音乐。道具应是画面中可制作、可复用或推动剧情的实体
8. merge 只负责去重子主体，不创建父主体；每个 is_child 必须为 JSON 布尔值 true

输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "merged_subjects": [
    {{"name": "<主体名>", "type": "character|scene|prop|sound", "description": "<合并后描述>", "episode_nos": [1, 2], "is_child": true}}
  ],
  "supplemental_subjects": [
    {{"name": "<遗漏主体名>", "type": "character|scene|prop|sound", "description": "<详细描述>", "episode_nos": [1], "is_child": true, "evidence": [{{"episode_no": 1, "quote": "<原文逐字片段>"}}]}}
  ],
  "total_count": 44
}}
"""


def _normalize_merge_child_flags(payload: object) -> object:
    """只修复供应商把阶段固定值错误写成 ``false`` 的已知漂移。

    ``is_child`` 不是 merge 模型需要推理的业务结果，而是该阶段的服务端不变量。
    适配只接受精确布尔 ``false`` 并改写为 ``true``；缺失、字符串、数字、非对象条目
    及其他结构性错误全部原样交给严格 Pydantic 契约拒绝，避免扩大供应商输出容错面。
    """
    if not isinstance(payload, dict):
        return payload
    normalized_payload = dict(payload)
    changed = False
    for field in ("merged_subjects", "supplemental_subjects"):
        subjects = payload.get(field)
        if not isinstance(subjects, list):
            continue
        normalized_subjects: list[object] = []
        for subject in subjects:
            if isinstance(subject, dict) and subject.get("is_child") is False:
                normalized = dict(subject)
                normalized["is_child"] = True
                normalized_subjects.append(normalized)
                changed = True
            else:
                normalized_subjects.append(subject)
        normalized_payload[field] = normalized_subjects
    return normalized_payload if changed else payload


class _MergeProviderAdapter:
    """在严格 MergeOutput 校验前注入 merge 阶段的固定子主体语义。"""

    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self.supplemental_subjects: list[object] = []

    async def complete_json(self, system: str, user: str) -> object:
        payload = _normalize_merge_child_flags(await self._provider.complete_json(system, user))
        if not isinstance(payload, dict):
            self.supplemental_subjects = []
            return payload

        normalized_payload = dict(payload)
        supplemental_subjects = normalized_payload.pop("supplemental_subjects", [])
        self.supplemental_subjects = (
            supplemental_subjects if isinstance(supplemental_subjects, list) else []
        )
        merged_subjects = normalized_payload.get("merged_subjects")
        if isinstance(merged_subjects, list):
            # total_count 是基础合并产物的派生值，不能被可选补漏项影响。
            normalized_payload["total_count"] = len(merged_subjects)
        return normalized_payload


def _build_extract_baseline(per_episodes: list[object]) -> list[dict]:
    """从已完成 extract 的暂存结果构造可直接发布的确定性主体基线。"""
    subjects_by_key: dict[tuple[str, str], dict] = {}
    for raw_episode in per_episodes:
        if not isinstance(raw_episode, dict):
            raise ValueError("extract 暂存输出包含非法分集")
        episode_no = raw_episode.get("episode_no")
        if type(episode_no) is not int or episode_no < 1:
            raise ValueError("extract 暂存输出缺少合法集号")
        raw_subjects = raw_episode.get("subjects")
        if not isinstance(raw_subjects, list):
            raise ValueError("extract 暂存输出缺少主体列表")
        for raw_subject in raw_subjects:
            if not isinstance(raw_subject, dict):
                raise ValueError("extract 暂存输出包含非法主体")
            subject_payload = dict(raw_subject)
            # 这两个字段由当前阶段确定，不能让过往模型输出阻断已成功的抽取结果。
            subject_payload["is_child"] = True
            subject_payload["episode_nos"] = [episode_no]
            try:
                subject = PipelineSubject.model_validate(subject_payload)
            except ValidationError as exc:
                raise ValueError("extract 暂存输出包含非法主体") from exc
            item = subject.model_dump()
            key = (item["type"], item["name"])
            existing = subjects_by_key.get(key)
            if existing is None:
                subjects_by_key[key] = item
                continue
            existing["episode_nos"] = sorted(
                set(existing["episode_nos"]).union(item["episode_nos"])
            )
            if (len(item["description"]), item["description"]) > (
                len(existing["description"]),
                existing["description"],
            ):
                existing["description"] = item["description"]
    return [subjects_by_key[key] for key in sorted(subjects_by_key)]


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
    existing_by_pipeline_key = {
        (subject.type, subject.pipeline_name): subject
        for subject in existing
        if subject.pipeline_name
    }
    staged: list[dict] = []
    matched_ids: set[str] = set()
    for raw in merged_subjects:
        item = dict(raw)
        subject = existing_by_key.get((item["type"], item["name"]))
        if subject is None:
            # pipeline_name 是首次流水线识别出的名称，人工改名不会改变它；
            # 因此同集存在多个同类型锁主体时也无需按集号猜测身份。
            subject = existing_by_pipeline_key.get((item["type"], item["name"]))
        if subject is None:
            # 仅兼容尚未经过新迁移的测试/瞬时对象；生产数据迁移后均有稳定身份。
            incoming_episodes = set(item.get("episode_nos") or [])
            locked_candidates = [
                candidate
                for candidate in existing
                if candidate.type == item["type"]
                and not candidate.pipeline_name
                and candidate.name_locked
                and incoming_episodes.intersection(candidate.episode_nos or [])
                and candidate.id not in matched_ids
            ]
            if len(locked_candidates) == 1:
                subject = locked_candidates[0]
        if subject is None:
            item.update(
                id=new_id(),
                base_subject_version=None,
                base_enrich_version=None,
                subject_version=1,
                enrich_version=1,
            )
        else:
            matched_ids.add(subject.id)
            suggestions: list[dict] = []
            if subject.name_locked and subject.name != item["name"]:
                suggestions.append(
                    {
                        "field": "name",
                        "current_value": subject.name,
                        "suggested_value": item["name"],
                    }
                )
                item["name"] = subject.name
            if (
                subject.description_locked
                and subject.description != item.get("description")
            ):
                suggestions.append(
                    {
                        "field": "description",
                        "current_value": subject.description,
                        "suggested_value": item.get("description"),
                    }
                )
                item["description"] = subject.description
            changed = (
                subject.description != item.get("description")
                or bool(subject.is_child) != bool(item.get("is_child", True))
                or sorted(subject.episode_nos or [])
                != sorted(item.get("episode_nos") or [])
                or not subject.is_active
            )
            base_subject_version = int(subject.subject_version or 1)
            base_enrich_version = int(subject.enrich_version or 1)
            prompt_changed = (
                subject.name != item.get("name")
                or subject.type != item.get("type")
                or (subject.description or "") != (item.get("description") or "")
            )
            item.update(
                id=subject.id,
                base_subject_version=base_subject_version,
                base_enrich_version=base_enrich_version,
                subject_version=base_subject_version + int(changed),
                enrich_version=(
                    base_enrich_version + 1
                    if item.get("type") != "sound" and prompt_changed
                    else base_enrich_version
                ),
            )
            if suggestions:
                item["field_suggestions"] = suggestions
        staged.append(item)

    # 匹配歧义时也绝不能让人工锁主体被整体替换逻辑删除。
    for subject in existing:
        if subject.id in matched_ids or not (
            subject.name_locked or subject.description_locked
        ):
            continue
        version = int(subject.subject_version or 1)
        enrich_version = int(subject.enrich_version or 1)
        staged.append(
            {
                "id": subject.id,
                "name": subject.name,
                "type": subject.type,
                "description": subject.description,
                "is_child": bool(subject.is_child),
                "episode_nos": list(subject.episode_nos or []),
                "base_subject_version": version,
                "base_enrich_version": enrich_version,
                "subject_version": version,
                "enrich_version": enrich_version,
            }
        )
    return staged


async def _record_new_subject_candidates(
    db: AsyncSession,
    *,
    script_id: str,
    staged_subjects: list[dict],
    source_run_id: str | None,
    auto_link: bool,
) -> list[SubjectCandidate]:
    """幂等记录本轮首次识别的主体；自动模式也保留 linked 审计事实。"""

    created: list[SubjectCandidate] = []
    for item in staged_subjects:
        if item.get("base_subject_version") is not None:
            continue
        subject_id = item.get("id")
        candidate_conditions = [
            SubjectCandidate.script_id == script_id,
            SubjectCandidate.subject_type == item["type"],
            SubjectCandidate.pipeline_name == item["name"],
        ]
        if auto_link:
            candidate_conditions.append(SubjectCandidate.source_run_id == source_run_id)
        else:
            candidate_conditions.append(SubjectCandidate.status == "pending")
        existing = (
            await db.execute(
                select(SubjectCandidate).where(*candidate_conditions)
            )
        ).scalar_one_or_none()
        if existing is not None:
            created.append(existing)
            continue
        candidate = SubjectCandidate(
            id=new_id(),
            script_id=script_id,
            source_run_id=source_run_id,
            subject_type=item["type"],
            name=item["name"],
            pipeline_name=item["name"],
            description=item.get("description"),
            aliases=[],
            episode_nos=item.get("episode_nos") or [],
            evidence=normalize_candidate_evidence(item.get("evidence")),
            status="linked" if auto_link else "pending",
            version=2 if auto_link else 1,
            linked_subject_id=subject_id if auto_link else None,
        )
        db.add(candidate)
        created.append(candidate)
    return created


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

    extract_baseline = _build_extract_baseline(per_episodes)
    merged_subjects = [dict(subject) for subject in extract_baseline]
    merge_mode = "deterministic"
    fallback_reason: str | None = None
    # 默认快速路径不再将全剧本重复发送给模型。测试、诊断或显式增强调用
    # 可传 provider opt-in，保留历史补漏能力与证据校验。
    if provider is not None:
        import json

        all_subjects = [
            {
                "name": subject["name"],
                "type": subject["type"],
                "description": subject["description"],
                "episode_nos": subject["episode_nos"],
            }
            for subject in extract_baseline
        ]
        episode_rows = list(
            (
                await db.execute(
                    select(Episode).where(Episode.script_id == script_id)
                )
            )
            .scalars()
            .all()
        )
        episode_snapshots: list[dict[str, object]] = [
            {
                "episode_no": int(episode.episode_no),
                "script_text": str(episode.script_text or ""),
            }
            for episode in episode_rows
        ]
        episode_text_by_no: dict[int, str] = {
            int(episode.episode_no): str(episode.script_text or "")
            for episode in episode_rows
        }
        if isinstance(db, AsyncSession):
            await db.rollback()

        model_provider = _MergeProviderAdapter(provider)
        merge_mode = "model_enhanced"
        try:
            contract = await complete_json(
                model_provider,
                _SYSTEM,
                _USER_TEMPLATE.format(
                    multi_episode_subjects_json=json.dumps(
                        all_subjects, ensure_ascii=False
                    ),
                    episodes_json=json.dumps(
                        episode_snapshots,
                        ensure_ascii=False,
                    ),
                ),
                MergeOutput,
                durable_attempt=provider_attempt,
            )
        except (ProviderContractError, OSError) as exc:
            # merge 不是基础主体的权威来源：供应商不可用或字段漂移时发布 extract 基线。
            merge_mode = "extract_fallback"
            fallback_reason = (
                "provider_contract"
                if getattr(exc, "error_type", None) == "provider_contract_error"
                else "provider_transport"
            )
        else:
            descriptions_by_key = {
                (subject.type, subject.name): subject.description
                for subject in contract.merged_subjects
            }
            merged_subjects = []
            for subject in extract_baseline:
                item = dict(subject)
                item["description"] = descriptions_by_key.get(
                    (item["type"], item["name"]), item["description"]
                )
                merged_subjects.append(item)

            existing_keys = {
                (subject["type"], subject["name"]) for subject in merged_subjects
            }
            for raw_supplemental in model_provider.supplemental_subjects:
                try:
                    supplemental = SupplementalSubject.model_validate(raw_supplemental)
                except ValidationError:
                    continue
                supplemental_key = (supplemental.type, supplemental.name)
                if supplemental_key in existing_keys:
                    continue
                evidence_episode_nos = {
                    evidence.episode_no for evidence in supplemental.evidence
                }
                if evidence_episode_nos != set(supplemental.episode_nos):
                    continue
                if any(
                    not (script_text := episode_text_by_no.get(evidence.episode_no))
                    or evidence.quote not in script_text
                    for evidence in supplemental.evidence
                ):
                    continue
                merged_subjects.append(
                    supplemental.model_dump(exclude={"evidence"})
                )
                existing_keys.add(supplemental_key)

    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return 0

    staged_subjects = await _prepare_staged_subjects(db, script_id, merged_subjects)
    await asset_crud.persist_subject_suggestions(
        db,
        script_id,
        staged_subjects,
        source_run_id=run_id,
    )
    manual_review = settings.subject_candidate_review_mode == "manual"
    published_subjects = (
        [item for item in staged_subjects if item.get("base_subject_version") is not None]
        if manual_review
        else staged_subjects
    )
    # 先发布正式主体，再写 linked 候选审计。否则同批第二次查询触发
    # autoflush 时，候选可能先于其 linked_subject_id 对应主体落库而违反 FK。
    await replace_subjects(db, script_id, published_subjects)
    if run_id is not None or manual_review:
        await _record_new_subject_candidates(
            db,
            script_id=script_id,
            staged_subjects=staged_subjects,
            source_run_id=run_id,
            auto_link=not manual_review,
        )
    if run_id is not None:
        # 父分组属于派生产物；子主体已更新时不能继续引用上一轮的分组。
        await replace_parent_subjects(db, script_id, [])
    stage_detail = {
        "count": len(staged_subjects),
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
        "merge_mode": merge_mode,
        "subject_review_mode": settings.subject_candidate_review_mode,
        "pending_candidate_count": len(staged_subjects) - len(published_subjects),
    }
    if fallback_reason is not None:
        stage_detail["fallback_reason"] = fallback_reason
    await set_stage(
        db,
        script_id,
        "merge",
        status="done",
        detail=stage_detail,
    )
    if run_id is not None:
        result: dict[str, Any] = {
            "count": len(staged_subjects),
            "subjects": published_subjects,
            "subjects_published": True,
        }
        # 默认 auto_link 模式继续返回历史形状；只有真正进入人工等待态时才暴露数量。
        pending_candidate_count = len(staged_subjects) - len(published_subjects)
        if pending_candidate_count:
            result["pending_candidate_count"] = pending_candidate_count
        return result
    return len(staged_subjects)
