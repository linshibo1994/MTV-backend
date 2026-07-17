"""enrich 阶段 — 设计线索增强（主体简单描述 → 摄影级扩展 prompt）。

严格遵循 06 文档第 12.3 节 LLM-3 提示词骨架与输入输出契约。
扩展后的 resolved_prompts.final 写入 pipeline_stage(stage=enrich).detail，
键为 subject_id，供文生图任务（T6）构造 input_json 时读取。
模型 Subject 无扩展 prompt 字段，故集中存于流水线阶段明细。
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.episode import Episode
from app.models.subject import Subject
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import EnrichOutput, complete_json, should_enrich_subject
from app.services.pipeline import get_run_stage_output, input_episode_is_current, set_stage

# ---- 固定模板 prompt（06 文档第 5 节，4 插槽）----
_BASE_PROMPT_TEMPLATE = (
    "为以下主体设计一张清晰可用于短剧视觉资产库的参考图。\n"
    "主体名称：{subject_name}\n"
    "主体类型：{subject_type_label}\n"
    "主体描述：{subject_description}\n"
    "要求：主体清晰、特征稳定、背景干扰少、适合后续多图参考生成。"
)

_TYPE_LABELS = {
    "character": "角色",
    "scene": "场景",
    "prop": "道具",
    "sound": "声音",
}

# ---- 扩展 prompt 提示词骨架（06 文档第 12.3 节）----
_SYSTEM = "你是一名专业产品/角色摄影指导。请将主体的简单描述扩展为专业摄影级生图 prompt。"

_USER_TEMPLATE = """主体信息:
- 名称: {subject_name}
- 类型: {subject_type}
- 描述: {subject_description}

固定参考模板 prompt（请在此模板上下文基础上扩展，保持主体一致性与背景稳定）:
---
{base_prompt}
---

扩展 prompt 必须包含以下 12 个段落, 每段用小标题标注:

1. 【形制描述】主体的外形、结构、比例精确描述
2. 【材质钉死】明确指定材质 (金属/布料/木质/皮肤等), 禁止模糊
3. 【使用痕迹】描述磨损/污渍/老化等使用痕迹, 增加真实感
4. 【摆放姿态】主体在画面中的摆放角度与姿态
5. 【反悬浮锁】确保主体有物理支撑, 禁止悬浮感
6. 【时代锚定】锁定时代背景 (如 21 世纪初中国城镇), 禁止时代错位
7. 【画面构图】构图方式 (三分法/居中/黄金比例)
8. 【画风统一锁】统一写实摄影风格, 禁止插画/卡通/油画
9. 【三点柔光箱白底布光】三点柔光箱 (主光45度/辅光45度/轮廓光后方) + 纯白底
10. 【反水印锁】禁止生成水印/Logo/签名
11. 【文字渲染降级方案】画面中文字模糊/遮挡/最小化
12. 【写实质感锚定】Phase One 中画幅 + 100mm 微距 + f/8, 写实摄影质感

输出格式: JSON, 字段 final 为完整扩展 prompt 文本, 包含上述 12 段落。
输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "resolved_prompts": {{ "final": "<12 段落扩展后的完整 prompt 文本>" }}
}}
"""


def build_base_prompt(subject: Subject) -> str:
    """构造固定模板 prompt（06 文档第 5 节，enrich 的输入）。"""
    return _BASE_PROMPT_TEMPLATE.format(
        subject_name=subject.name,
        subject_type_label=_TYPE_LABELS.get(subject.type, subject.type),
        subject_description=subject.description or "",
    )


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
) -> int | dict:
    """对所有主体逐个增强，返回已处理数量。

    resolved_prompts.final 集中写入 enrich 阶段 detail.prompts[subject_id]。
    """
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

    prompts: dict[str, dict] = {}
    target_enrich_versions: dict[str, int] = {}
    for subject in visual_subjects:
        base_prompt = build_base_prompt(subject)
        contract = await complete_json(
            provider,
            _SYSTEM,
            _USER_TEMPLATE.format(
                subject_name=subject.name,
                subject_type=subject.type,
                subject_description=subject.description or "",
                base_prompt=base_prompt,
            ),
            EnrichOutput,
            durable_attempt=provider_attempt,
        )
        final_prompt = contract.resolved_prompts.final
        subject_episode_nos = sorted(
            int(no) for no in (subject.episode_nos or []) if int(no) in episode_snapshot_by_no
        )
        target_enrich_version = (
            int(subject.enrich_version or 1)
            if run_id is not None
            else int(subject.enrich_version or 1) + 1
        )
        target_enrich_versions[subject.id] = target_enrich_version
        prompts[subject.id] = {
            "subject_version": int(subject.subject_version or 1),
            "enrich_version": target_enrich_version,
            "resolved_prompts": {"final": final_prompt},
            "episode_snapshot": {
                str(no): episode_snapshot_by_no[no] for no in subject_episode_nos
            },
        }

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
