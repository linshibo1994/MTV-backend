"""LLM-5 分镜生成服务。

严格遵循 06-AI能力与Prompt工程文档.md 第 7 节 (分镜 Shot Prompt 结构) 与第 8 节 (视频生成约束锁)。

输入: 单集剧本 + 主体列表
LLM 产出: 场景 -> 叙事分组建议 -> 结构化小分镜 storyboard.v2
每镜头: 景别、构图、机位、运镜、动作、画面、光影、情绪、声音与叙事目的
服务端再确定性派生旧版 content / description / shot_line，避免多份事实漂移。
每个 draft 组: 多镜头分时段建议 + 叙事目的；后续由合并接口发布为正式视频单元
解析引用主体填充 shot_group.materials {character/background/prop/other}
audio_elements 需配音角色
"""

from __future__ import annotations

import inspect
import json
import logging
from dataclasses import dataclass
from typing import cast

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.episode import Episode
from app.models.scene import Scene
from app.models.shot import Shot
from app.models.shot_group import DRAFT_SHOT_GROUP_UNIT_PREFIX, ShotGroup
from app.models.subject import Subject
from app.models.video_generation_job import VideoGenerationJob
from app.providers.llm import LLMJSONResponseError
from app.schemas.pipeline_contracts import ProviderContractError
from app.schemas.storyboard_contracts import (
    CAMERA_ANGLE_VALUES,
    CAMERA_MOVEMENT_VALUES,
    COMPOSITION_VALUES,
    SHOT_SIZE_VALUES,
    StoryboardContract,
    StoryboardContractValidationError,
    StoryboardV2Contract,
    storyboard_contract_diagnostic,
    validate_storyboard_contract,
    validate_storyboard_v2_contract,
)

logger = logging.getLogger(__name__)

_STORYBOARD_CONTRACT_MAX_ATTEMPTS = 2
_STORYBOARD_REPAIR_PAYLOAD_MAX_CHARS = 120_000


# ---------------------------------------------------------------------------
# 常量 — 严格遵循 06 文档第 7~8 节
# ---------------------------------------------------------------------------

# 视频生成约束锁 (9 条, 固定放 prompt 末尾)
# 来源: 06 文档第 8.2 节
PROMPT_CONSTRAINTS = [
    "人物特征高度一致",
    "角色不脸崩",
    "肢体不畸形",
    "多镜头切镜丝滑平顺",
    "画面质量4K稳定",
    "语音清晰",
    "情感表达贴合角色性格",
    "台词与唇形/动作精准对位",
    "禁止出现任何形式的字幕",
]
CONSTRAINT_LOCK = "【约束】\n" + "\n".join(
    f"{index}. {constraint}" for index, constraint in enumerate(PROMPT_CONSTRAINTS, start=1)
)

# 镜头 description 结构化模板
# 来源: 06 文档第 7.2 节 / 02 文档第 5.4.2 节
DESCRIPTION_TEMPLATE = (
    "在【{background}】中采用【{camera}】拍摄视角，"
    "画面主体【{characters}】的关键动作是【{action}】，"
    "整体表现的画面是【{scene}】，"
    "表达了【{emotion}】，"
    "整体画面的光影效果是【{lighting}】，"
    "整个镜头的叙事目的是【{narrative_purpose}】。"
)

# 单镜头行模板 (用于组合 prompt)
# 来源: 06 文档第 7.4 节
SHOT_LINE_TEMPLATE = (
    "【镜头{n}:{start}s～{end}s】场景是{scene}，"
    "{shot_scale}·{camera}，{camera_movement}，"
    "{lighting}，{action}，{emotion}，{characters}:{dialogue}，"
    "构图是【{composition}】，声音设计是【{sound_design}】，"
    "台词类型是【{dialogue_kind}】"
)


# ---------------------------------------------------------------------------
# LLM Prompt 构造
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = f"""你是一位专业的短剧分镜导演与摄影指导。请把剧本转换为可拍摄、可验证的 storyboard.v2 JSON。

## 层级
Episode -> Scene（连续时空段落）-> Shot Group（3~15 秒叙事单元）-> Shot（单一视觉节拍）。

## 导演规则
1. 场景、地点、日夜、时间或连续叙事段落变化时新建 Scene；离开后再回到同一地点也要新建 Scene。
2. 每个新场景最多用 1~2 个镜头完成定场；每镜只承载一个主要动作或信息变化，避免无叙事作用的装饰镜头。
3. 景别随叙事逐步聚焦或释放，避免连续三镜使用相同景别；运镜必须服务于发现、强调、跟随或情绪变化。
4. 相邻镜头保持人物位置、朝向、视线与动作进度连续；用反应镜、动作桥或声音桥承接情绪和转场。
5. frame_description 只写可见的主体、动作状态与前中后景空间，不写无法拍摄的心理判断。
6. 剧本中的对白、旁白和内心独白必须逐字保留；不得增删或改写。无语言内容时 dialogue 必须为 null。
7. 只可引用输入目录中的短引用；不得输出主体名称、subject_id、asset_entry_id，也不得创造新资产。
8. 不要输出 content、description 或 shot_line；这些派生字段由服务端生成。

## 受控摄影词表
- shot_size: {'/'.join(SHOT_SIZE_VALUES)}
- composition: {'/'.join(COMPOSITION_VALUES)}（每镜 1~3 项）
- camera_angle: {'/'.join(CAMERA_ANGLE_VALUES)}
- camera_movement: {'/'.join(CAMERA_MOVEMENT_VALUES)}

## 结构与时码
- schema_version 必须是 storyboard.v2。
- scene_no 从 1 连续递增；每个 Scene 内 group_no 从 G1 连续递增。
- 每组 1~5 镜，duration_seconds 为 3~15 的整数秒；单镜头组可用于定场、反应或转场，
  叙事需要时优先使用 2~5 镜的连续镜头组。
- 每组 display_no 必须严格为 1, 2, ..., N；首镜 start_seconds=0，相邻区间首尾相接。
- end_seconds 必须大于 start_seconds，末镜 end_seconds 必须等于 duration_seconds；所有时码只能是整数。
- background_ref 只能引用场景；character_refs 只能引用角色；prop_refs 只能引用道具；sound_refs 只能引用声音。
- kind=dialogue/inner_monologue 时 speaker_ref 必须是当前镜头可见角色；kind=voice_over 时 speaker_ref 可为角色、声音主体或 null。
- narrative_purpose 要说明该镜头让观众获得什么信息、感受什么变化，不能只写“推进剧情”。
- sound_design 写可执行的环境音/动作音/声音衔接，无声音时写“无”。

只输出符合 JSON Schema 的 JSON 对象，不要 markdown 或解释文字。
"""


@dataclass(frozen=True)
class _SubjectReference:
    ref: str
    name: str
    subject_type: str
    description: str
    subject_id: str
    asset_entry_id: str | None

    @property
    def has_image_asset(self) -> bool:
        return self.asset_entry_id is not None


@dataclass(frozen=True)
class _ValidatedStoryboard:
    """同时保留兼容落库合约与 v2 引用事实。

    ``v2_contract`` 使资产绑定能一直使用稳定短引用，不会在不同类型同名
    主体之间二次按名称猜测。历史 v1 响应则为 ``None``。
    """

    contract: StoryboardContract
    v2_contract: StoryboardV2Contract | None


def _build_subject_reference_catalog(
    subjects: list[Subject],
    episode_no: int,
) -> dict[str, _SubjectReference]:
    """为当前分集生成稳定短引用，模型永不接触服务端 UUID。"""
    prefixes = {
        "character": "C",
        "scene": "BG",
        "background": "BG",
        "prop": "P",
        "sound": "S",
    }
    counters: dict[str, int] = {}
    catalog: dict[str, _SubjectReference] = {}
    eligible = [
        subject
        for subject in subjects
        if (not (subject.episode_nos or []) or episode_no in (subject.episode_nos or []))
        and getattr(subject, "is_active", True)
        and subject.type in prefixes
    ]
    for subject in sorted(
        eligible,
        key=lambda item: (item.type, item.name, getattr(item, "id", "")),
    ):
        prefix = prefixes[subject.type]
        counters[prefix] = counters.get(prefix, 0) + 1
        ref = f"{prefix}{counters[prefix]:02d}"
        subject_type = "background" if subject.type == "scene" else subject.type
        catalog[ref] = _SubjectReference(
            ref=ref,
            name=subject.name,
            subject_type=subject_type,
            description=(getattr(subject, "description", None) or "无补充描述").strip(),
            subject_id=str(subject.id),
            asset_entry_id=(
                str(subject.image_entry_id)
                if getattr(subject, "image_entry_id", None)
                else None
            ),
        )
    return catalog


def _build_subject_list_block(
    subjects: list[Subject],
    episode_no: int,
    references: dict[str, _SubjectReference] | None = None,
) -> str:
    """构造只含短引用、名称与创作上下文的主体目录。"""
    reference_catalog = references or _build_subject_reference_catalog(subjects, episode_no)
    lines: list[str] = []
    for ref, item in reference_catalog.items():
        asset_state = "已绑定参考图" if item.has_image_asset else "暂无参考图"
        lines.append(
            f"- {ref} | {item.subject_type} | {item.name} | {item.description} | {asset_state}"
        )
    return "\n".join(lines) or "（当前分集没有可引用主体）"


def _build_user_prompt(
    episode: Episode,
    subjects: list[Subject],
    references: dict[str, _SubjectReference] | None = None,
) -> str:
    """构造 v2 用户提示；示例只表达形状，真实引用以目录为准。"""
    reference_catalog = references or _build_subject_reference_catalog(
        subjects,
        episode.episode_no,
    )
    subject_block = _build_subject_list_block(
        subjects,
        episode.episode_no,
        reference_catalog,
    )
    return f"""请为以下分集生成 storyboard.v2 分镜。

## 分集
- 集号：第 {episode.episode_no} 集
- 标题：{episode.title}

## 剧本原文
{episode.script_text or '(无剧本内容)'}

## 当前分集可引用主体目录
格式为：短引用 | 类型 | 名称 | 描述 | 参考图状态。JSON 中只能填写短引用。
{subject_block}

## JSON 形状示例
下面的 BG01/C01/P01/S01 只是形状示例；若目录不存在该引用，不得使用。
{{
  "schema_version": "storyboard.v2",
  "scenes": [
    {{
      "scene_no": 1,
      "background_ref": "BG01",
      "shot_groups": [
        {{
          "group_no": "G1",
          "narrative_purpose": "建立空间关系并引出冲突",
          "duration_seconds": 6,
          "shots": [
            {{
              "display_no": 1,
              "start_seconds": 0,
              "end_seconds": 3,
              "visual": {{
                "shot_size": "全景",
                "composition": ["三分法", "纵深构图"],
                "camera_angle": "平视",
                "camera_movement": "推镜",
                "action": "角色推门进入并在门口停住",
                "frame_description": "前景门框形成框景，中景角色推门进入，背景人物抬头看向门口",
                "lighting": "室内冷色顶光与门外自然光形成层次",
                "emotion": "紧张、试探"
              }},
              "narrative_purpose": "交代人物进入空间并触发对视",
              "character_refs": ["C01"],
              "prop_refs": ["P01"],
              "sound_refs": ["S01"],
              "sound_design": "门轴轻响，室内环境声短暂停顿",
              "dialogue": {{
                "speaker_ref": "C01",
                "text": "逐字保留的原台词",
                "kind": "dialogue"
              }}
            }},
            {{
              "display_no": 2,
              "start_seconds": 3,
              "end_seconds": 6,
              "visual": {{
                "shot_size": "近景",
                "composition": ["单人", "三分法"],
                "camera_angle": "平视",
                "camera_movement": "固定",
                "action": "人物停下手中动作并抬眼回应",
                "frame_description": "人物位于画面右侧，视线越过前景物体看向门口",
                "lighting": "冷色顶光勾勒面部轮廓",
                "emotion": "迟疑"
              }},
              "narrative_purpose": "用反应镜承接前一镜并强化冲突",
              "character_refs": ["C01"],
              "prop_refs": [],
              "sound_refs": [],
              "sound_design": "无",
              "dialogue": null
            }}
          ]
        }}
      ]
    }}
  ]
}}

## 输出前自检
- 是否覆盖剧本全部叙事、台词/旁白且没有新增情节；
- scene_no、group_no、display_no 是否连续；
- 时码是否全部为整数、连续且与组时长一致；
- 所有短引用是否存在且类型正确；
- 每镜是否都有明确景别、构图、机位、运镜、动作、光线、情绪和叙事目的；
- 相邻镜头的动作、视线、空间和情绪是否自然承接。
"""


_UNSUPPORTED_PROVIDER_SCHEMA_KEYS = frozenset(
    {
        "default",
        "format",
        "maxItems",
        "maxLength",
        "maximum",
        "minItems",
        "minLength",
        "minimum",
        "multipleOf",
        "pattern",
        "uniqueItems",
    }
)


def _provider_safe_json_schema(value: object) -> object:
    """把 Pydantic Schema 收敛为主流结构化输出的交集。

    长度、数值边界、正则和数组数量等约束仍由本地 Pydantic 与语义校验执行。
    ``properties`` / ``$defs`` 下的业务字段名必须原样保留，不能当作 Schema 关键字过滤。
    """
    if isinstance(value, list):
        return [_provider_safe_json_schema(item) for item in value]
    if not isinstance(value, dict):
        return value

    sanitized: dict[str, object] = {}
    for key, item in value.items():
        if key in _UNSUPPORTED_PROVIDER_SCHEMA_KEYS:
            continue
        if key == "const":
            sanitized["enum"] = [item]
            continue
        if key in {"properties", "$defs"} and isinstance(item, dict):
            sanitized[key] = {
                name: _provider_safe_json_schema(child) for name, child in item.items()
            }
            continue
        sanitized[key] = _provider_safe_json_schema(item)
    return sanitized


_RESPONSE_SCHEMA: dict[str, object] = StoryboardV2Contract.model_json_schema()
_PROVIDER_RESPONSE_SCHEMA = cast(
    dict[str, object],
    _provider_safe_json_schema(_RESPONSE_SCHEMA),
)


# ---------------------------------------------------------------------------
# 组合 prompt 构造 (06 文档第 7.3 节)
# ---------------------------------------------------------------------------


def _build_group_prompt(
    shot_lines: list[str],
    referenced_names: list[str],
    narrative_purpose: str,
) -> str:
    """构造镜头组组合 prompt。

    结构: 多镜头分时段行 + 【角色】+【叙事目的】+【约束】(9条)
    """
    lines_block = "\n".join(shot_lines)
    characters_block = "、".join(referenced_names)
    return (
        f"{lines_block}\n"
        f"【角色】{characters_block}\n"
        f"【叙事目的】{narrative_purpose}\n"
        f"{CONSTRAINT_LOCK}"
    )


# ---------------------------------------------------------------------------
# 引用主体解析 → materials 分组
# ---------------------------------------------------------------------------


def _classify_materials(
    referenced_names: list[str], subjects: list[Subject]
) -> dict[str, list[dict[str, str]]]:
    """将引用的主体名按类型分组为 materials。

    返回 {character: [], background: [], prop: [], other: []}
    """
    subject_by_name: dict[str, Subject] = {}
    for s in subjects:
        subject_by_name[s.name] = s

    materials: dict[str, list[dict[str, str]]] = {
        "character": [],
        "background": [],
        "prop": [],
        "other": [],
    }
    for name in referenced_names:
        subject = subject_by_name.get(name)
        stype = subject.type if subject is not None else "other"
        item = {"name": name}
        if subject is not None:
            item["subject_id"] = subject.id
            if subject.image_entry_id:
                item["asset_entry_id"] = subject.image_entry_id
        if stype == "character":
            materials["character"].append(item)
        elif stype in ("scene", "background"):
            materials["background"].append(item)
        elif stype == "prop":
            materials["prop"].append(item)
        elif stype != "sound":
            materials["other"].append(item)
    return materials


def _classify_reference_materials(
    referenced_refs: list[str],
    references: dict[str, _SubjectReference],
) -> dict[str, list[dict[str, str]]]:
    """用 v2 短引用组装视觉素材，全程保留稳定主体身份。"""
    materials: dict[str, list[dict[str, str]]] = {
        "character": [],
        "background": [],
        "prop": [],
        "other": [],
    }
    seen: set[str] = set()
    for ref in referenced_refs:
        if ref in seen:
            continue
        seen.add(ref)
        subject = references.get(ref)
        if subject is None or subject.subject_type == "sound":
            continue
        item = {"subject_id": subject.subject_id, "name": subject.name}
        if subject.asset_entry_id is not None:
            item["asset_entry_id"] = subject.asset_entry_id
        bucket = subject.subject_type
        if bucket not in materials:
            bucket = "other"
        materials[bucket].append(item)
    return materials


async def _extract_audio_elements(
    db: AsyncSession, shots_data: list[dict], subjects: list[Subject]
) -> list[dict[str, str]]:
    """从镜头数据中提取需配音角色和声音主体。

    audio_elements 独立于视觉 materials，避免把 sound 当作图片素材。
    """
    subject_by_name = {s.name: s for s in subjects}
    subject_ids = [s.id for s in subjects]
    audio_entry_by_subject_id: dict[str, str] = {}
    if subject_ids:
        audio_result = await db.execute(
            select(AssetEntry).where(
                AssetEntry.subject_id.in_(subject_ids),
                AssetEntry.kind == "audio",
                AssetEntry.is_approved.is_(True),
                AssetEntry.superseded.is_(False),
            )
        )
        for entry in audio_result.scalars().all():
            audio_entry_by_subject_id.setdefault(entry.subject_id, entry.id)

    seen: set[tuple[str, str]] = set()
    result: list[dict[str, str]] = []
    for shot in shots_data:
        candidates: list[tuple[str, str]] = []
        char = shot.get("dialogue_character") or ""
        if char:
            candidates.append((char, "character"))
        for sound_name in shot.get("sounds", []):
            if sound_name:
                candidates.append((sound_name, "sound"))
        for name, item_type in candidates:
            key = (item_type, name)
            if key in seen:
                continue
            subject = subject_by_name.get(name)
            item = {"name": name, "type": item_type}
            if subject is not None:
                item["subject_id"] = subject.id
                asset_entry_id = audio_entry_by_subject_id.get(subject.id)
                if asset_entry_id:
                    item["asset_entry_id"] = asset_entry_id
            seen.add(key)
            result.append(item)
    return result


async def _extract_audio_elements_from_references(
    db: AsyncSession,
    referenced_refs: list[str],
    references: dict[str, _SubjectReference],
) -> list[dict[str, str]]:
    """用 v2 短引用提取配音与声音资产，避免同名主体错绑。"""
    ordered: list[_SubjectReference] = []
    seen: set[str] = set()
    for ref in referenced_refs:
        if ref in seen:
            continue
        seen.add(ref)
        subject = references.get(ref)
        if subject is not None and subject.subject_type in {"character", "sound"}:
            ordered.append(subject)

    subject_ids = [subject.subject_id for subject in ordered]
    audio_entry_by_subject_id: dict[str, str] = {}
    if subject_ids:
        audio_result = await db.execute(
            select(AssetEntry).where(
                AssetEntry.subject_id.in_(subject_ids),
                AssetEntry.kind == "audio",
                AssetEntry.is_approved.is_(True),
                AssetEntry.superseded.is_(False),
            )
        )
        for entry in audio_result.scalars().all():
            audio_entry_by_subject_id.setdefault(entry.subject_id, entry.id)

    result: list[dict[str, str]] = []
    for subject in ordered:
        item = {
            "subject_id": subject.subject_id,
            "name": subject.name,
            "type": subject.subject_type,
        }
        asset_entry_id = audio_entry_by_subject_id.get(subject.subject_id)
        if asset_entry_id is not None:
            item["asset_entry_id"] = asset_entry_id
        result.append(item)
    return result


def _subject_catalog(subjects: list[Subject], episode_no: int) -> dict[str, str]:
    catalog: dict[str, str] = {}
    for subject in subjects:
        episode_nos = subject.episode_nos or []
        if (
            (episode_nos and episode_no not in episode_nos)
            or not getattr(subject, "is_active", True)
        ):
            continue
        stype = "background" if subject.type == "scene" else subject.type
        previous = catalog.get(subject.name)
        # v1 只有名称，无法安全区分跨类型同名主体。标记为歧义后由
        # 旧版契约明确拒绝；新版 v2 始终使用短引用，不受此限制。
        catalog[subject.name] = stype if previous in {None, stype} else "ambiguous"
    return catalog


# ---------------------------------------------------------------------------
# shot_no 生成 (混淆 id 串)
# ---------------------------------------------------------------------------


def _gen_shot_no(display_no: int) -> str:
    """生成混淆 id 串。

    参考 02§5.4 / 04§1.14 / 06§7.5 文档示例: display_no=1 对应 shot_no "2x2x2x2x2x"。
    该串为平台观测到的固定混淆样式 "NxNxNxNxNx" (N 重复 5 段, 每段以 'x' 结尾)。
    取 N = display_no + 1:
    - display_no=1 精确命中文档示例 "2x2x2x2x2x";
    - 不同 display_no 产生不同 shot_no, 满足 video-workbench shot_nos 列表的去重/索引需求
      (shot_id 为唯一主键, shot_no 仅作混淆展示串, 无唯一约束)。
    """
    digit = display_no + 1 if display_no > 0 else 1
    return f"{digit}x" * 5


# ---------------------------------------------------------------------------
# LLM 调用 (按 PLAN 第 5.1 节 Provider 抽象)
# ---------------------------------------------------------------------------


def _safe_template_text(value: str) -> str:
    """防止模型文本中的模板分隔符破坏确定性展示格式。"""
    return value.strip().replace("【", "〔").replace("】", "〕")


def _safe_shot_line_text(value: str) -> str:
    """单行字段使用顿号分隔内部短语，保留模板逗号作为结构边界。"""
    return _safe_template_text(value).replace("，", "、").replace("\n", " ")


def _render_v2_contract(
    contract: StoryboardV2Contract,
    *,
    subject_reference_names: dict[str, str],
) -> StoryboardContract:
    """把 v2 单一事实源确定性渲染为现有 v1 持久化形态。"""
    scenes: list[dict] = []
    for scene in contract.scenes:
        background = subject_reference_names[scene.background_ref]
        groups: list[dict] = []
        for group in scene.shot_groups:
            shots: list[dict] = []
            for shot in group.shots:
                visual = shot.visual
                characters = [subject_reference_names[ref] for ref in shot.character_refs]
                props = [subject_reference_names[ref] for ref in shot.prop_refs]
                sounds = [subject_reference_names[ref] for ref in shot.sound_refs]
                dialogue_character = ""
                dialogue_text = ""
                dialogue_kind = "none"
                if shot.dialogue is not None:
                    if shot.dialogue.speaker_ref is not None:
                        dialogue_character = subject_reference_names[shot.dialogue.speaker_ref]
                    dialogue_text = shot.dialogue.text.strip()
                    dialogue_kind = shot.dialogue.kind
                character_text = "、".join(characters) or "环境与场景"
                action = _safe_template_text(visual.action)
                frame_description = _safe_template_text(visual.frame_description)
                lighting = _safe_template_text(visual.lighting)
                emotion = _safe_template_text(visual.emotion)
                narrative_purpose = _safe_template_text(shot.narrative_purpose)
                description = DESCRIPTION_TEMPLATE.format(
                    background=_safe_template_text(background),
                    camera=visual.camera_angle,
                    characters=_safe_template_text(character_text),
                    action=action,
                    scene=frame_description,
                    emotion=emotion,
                    lighting=lighting,
                    narrative_purpose=narrative_purpose,
                )
                shot_line = SHOT_LINE_TEMPLATE.format(
                    n=shot.display_no,
                    start=shot.start_seconds,
                    end=shot.end_seconds,
                    scene=_safe_shot_line_text(background),
                    shot_scale=visual.shot_size,
                    camera=visual.camera_angle,
                    camera_movement=visual.camera_movement,
                    lighting=_safe_shot_line_text(lighting),
                    action=_safe_shot_line_text(action),
                    emotion=_safe_shot_line_text(emotion),
                    characters=_safe_shot_line_text(character_text),
                    dialogue=dialogue_text,
                    composition="、".join(visual.composition),
                    sound_design=_safe_template_text(shot.sound_design),
                    dialogue_kind=dialogue_kind,
                )
                shots.append(
                    {
                        "display_no": shot.display_no,
                        "content": frame_description,
                        "description": description,
                        "shot_line": shot_line,
                        "start_seconds": shot.start_seconds,
                        "end_seconds": shot.end_seconds,
                        "characters": characters,
                        "dialogue_character": dialogue_character,
                        "props": props,
                        "sounds": sounds,
                    }
                )
            groups.append(
                {
                    "group_no": group.group_no,
                    "narrative_purpose": _safe_template_text(group.narrative_purpose),
                    "duration_seconds": group.duration_seconds,
                    "shots": shots,
                }
            )
        scenes.append(
            {
                "scene_no": scene.scene_no,
                "background": background,
                "shot_groups": groups,
            }
        )
    rendered = {"schema_version": "storyboard.v1", "scenes": scenes}
    try:
        # 类型与引用已在 v2 短引用层校验。此处只校验派生的 v1 结构、
        # 编号、时码和固定模板，避免跨类型同名主体再次按名称误判。
        return StoryboardContract.model_validate(rendered)
    except ValueError as exc:
        raise StoryboardContractValidationError(
            str(exc),
            diagnostic=storyboard_contract_diagnostic("StoryboardContract", exc),
        ) from exc


def _validate_provider_payload_result(
    payload: dict,
    *,
    subject_catalog: dict[str, str],
    subject_reference_names: dict[str, str],
    subject_reference_types: dict[str, str],
) -> _ValidatedStoryboard:
    """按版本验证 Provider 响应，并保留 v2 的稳定引用事实。"""
    schema_version = payload.get("schema_version")
    looks_like_v2 = (
        any(
            isinstance(scene, dict) and "background_ref" in scene
            for scene in payload.get("scenes", [])
        )
        if isinstance(payload.get("scenes"), list)
        else False
    )
    if schema_version == "storyboard.v2" or (schema_version is None and looks_like_v2):
        normalized = dict(payload)
        normalized.setdefault("schema_version", "storyboard.v2")
        v2_contract = validate_storyboard_v2_contract(normalized, subject_reference_types)
        return _ValidatedStoryboard(
            contract=_render_v2_contract(
                v2_contract,
                subject_reference_names=subject_reference_names,
            ),
            v2_contract=v2_contract,
        )
    if schema_version in {None, "storyboard.v1"}:
        return _ValidatedStoryboard(
            contract=validate_storyboard_contract(payload, subject_catalog),
            v2_contract=None,
        )
    exc = ValueError("unsupported storyboard schema_version")
    raise StoryboardContractValidationError(
        str(exc),
        diagnostic=storyboard_contract_diagnostic("StoryboardV2Contract", exc),
    ) from exc


def _validate_provider_payload(
    payload: dict,
    *,
    subject_catalog: dict[str, str],
    subject_reference_names: dict[str, str],
    subject_reference_types: dict[str, str],
) -> StoryboardContract:
    """按版本验证 Provider 响应；无版本的历史 v1 仍保持兼容。"""
    return _validate_provider_payload_result(
        payload,
        subject_catalog=subject_catalog,
        subject_reference_names=subject_reference_names,
        subject_reference_types=subject_reference_types,
    ).contract


def _build_contract_repair_prompt(
    payload: dict,
    diagnostic: dict[str, object],
    references: dict[str, _SubjectReference],
) -> str | None:
    """只携带首答与脱敏字段路径的定向修复提示，不重发原剧本。"""
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(serialized) > _STORYBOARD_REPAIR_PAYLOAD_MAX_CHARS:
        return None
    reference_lines = [
        f"{ref}:{item.subject_type}" for ref, item in references.items()
    ]
    validation = diagnostic.get("validation")
    codes = validation.get("codes", []) if isinstance(validation, dict) else []
    continuity_codes = {
        "scene_no_not_consecutive",
        "group_no_not_consecutive",
        "display_no_not_consecutive",
        "shot_time_not_continuous",
        "shot_end_must_be_greater_than_start",
        "shot_duration_total_mismatch",
    }
    if isinstance(codes, list) and any(code in continuity_codes for code in codes):
        repair_instruction = (
            "本次检测到全局连续性错误。请无条件重建全部元数据："
            "scene_no 按 scenes 数组顺序从 1 编号；每个 Scene 的 group_no 从 G1 编号；"
            "每组 display_no 从 1 编号；每组首镜从 0s 开始，所有镜头首尾相接，"
            "每镜 end_seconds 大于 start_seconds，最后一镜 end_seconds 必须等于 "
            "duration_seconds。保留 visual、动作、叙事文本、台词及主体短引用不变。"
        )
    else:
        repair_instruction = "按脱敏校验摘要逐项修复，不得改写剧情、动作、台词或主体短引用。"
    return (
        "请只修复下面 JSON 的结构、枚举、引用、编号或连续整数时码错误。"
        "保持原剧情、动作和台词内容，不要新增情节，不要输出解释。\n"
        f"修复规则：{repair_instruction}\n"
        f"允许的主体短引用：{json.dumps(reference_lines, ensure_ascii=False)}\n"
        f"脱敏校验摘要：{json.dumps(diagnostic, ensure_ascii=False, sort_keys=True)}\n"
        f"待修复 JSON：{serialized}"
    )


_REPAIR_SYSTEM_PROMPT = (
    "你是 storyboard.v2 JSON 修复器。只修复给定对象以符合 JSON Schema；"
    "不得改写台词或创造资产，只输出修复后的 JSON 对象。"
)


async def _call_llm(system: str, user: str) -> dict:
    """调用共享 LLM Provider；能力允许时下发供应商安全版 Schema。"""
    from app.providers import get_llm_provider

    provider = get_llm_provider()
    complete_json = provider.complete_json
    try:
        parameters = inspect.signature(complete_json).parameters.values()
        supports_schema = any(
            parameter.name == "response_schema"
            or parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
    except (TypeError, ValueError):
        supports_schema = False
    if supports_schema:
        return await complete_json(
            system=system,
            user=user,
            response_schema=_PROVIDER_RESPONSE_SCHEMA,
        )
    return await complete_json(system=system, user=user)


async def _call_llm_with_contract_result(
    system: str,
    user: str,
    subject_catalog: dict[str, str],
    *,
    subject_references: dict[str, _SubjectReference] | None = None,
) -> _ValidatedStoryboard:
    """首答后最多做一次定向修复，避免三次整集盲重试。"""
    references = subject_references or {}
    reference_names = {ref: item.name for ref, item in references.items()}
    reference_types = {ref: item.subject_type for ref, item in references.items()}
    call_system = system
    call_user = user
    last_diagnostic: dict[str, object] | None = None
    for attempt in range(1, _STORYBOARD_CONTRACT_MAX_ATTEMPTS + 1):
        payload: dict | None = None
        try:
            raw_payload = await _call_llm(call_system, call_user)
            if not isinstance(raw_payload, dict):
                content_length = (
                    len(raw_payload)
                    if isinstance(raw_payload, (str, list, tuple))
                    else 0
                )
                raise LLMJSONResponseError(
                    "non_object_json",
                    kind=type(raw_payload).__name__,
                    content_length=content_length,
                )
            payload = raw_payload
            return _validate_provider_payload_result(
                payload,
                subject_catalog=subject_catalog,
                subject_reference_names=reference_names,
                subject_reference_types=reference_types,
            )
        except StoryboardContractValidationError as exc:
            last_diagnostic = exc.diagnostic
        except ValueError as exc:
            last_diagnostic = storyboard_contract_diagnostic(
                "StoryboardV2Contract",
                exc,
            )
        validation = (last_diagnostic or {}).get("validation")
        error_count = validation.get("error_count") if isinstance(validation, dict) else None
        error_codes = validation.get("codes") if isinstance(validation, dict) else None
        error_paths = validation.get("paths") if isinstance(validation, dict) else None
        category = (last_diagnostic or {}).get("category")
        logger.warning(
            "分镜 Provider 响应未通过契约 attempt=%d/%d category=%s error_count=%s "
            "codes=%s paths=%s",
            attempt,
            _STORYBOARD_CONTRACT_MAX_ATTEMPTS,
            category,
            error_count,
            error_codes,
            error_paths,
        )
        if attempt < _STORYBOARD_CONTRACT_MAX_ATTEMPTS and payload is not None:
            repair_user = _build_contract_repair_prompt(payload, last_diagnostic or {}, references)
            if repair_user is not None:
                call_system = _REPAIR_SYSTEM_PROMPT
                call_user = repair_user
                continue
        if attempt < _STORYBOARD_CONTRACT_MAX_ATTEMPTS and payload is None:
            # Provider 未能解析出 JSON 时只再尝试一次原始请求。
            continue
        break
    raise ProviderContractError(
        f"供应商响应在 {_STORYBOARD_CONTRACT_MAX_ATTEMPTS} 次总预算内"
        "不符合 StoryboardV2Contract 合约",
        diagnostic=last_diagnostic,
    ) from None


async def _call_llm_with_contract(
    system: str,
    user: str,
    subject_catalog: dict[str, str],
    *,
    subject_references: dict[str, _SubjectReference] | None = None,
) -> StoryboardContract:
    """兼容既有内部调用，只返回可落库的 v1 形态。"""
    result = await _call_llm_with_contract_result(
        system,
        user,
        subject_catalog,
        subject_references=subject_references,
    )
    return result.contract


# ---------------------------------------------------------------------------
# 主入口: generate_storyboard
# ---------------------------------------------------------------------------


async def generate_storyboard(
    db: AsyncSession,
    script_id: str,
    episode_id: str,
    *,
    expected_episode_version: int | None = None,
    expected_client_save_seq: int | None = None,
) -> dict:
    """LLM-5 分镜生成主流程。

    1. 加载分集剧本 + 主体列表
    2. 构造 LLM prompt (严格 06 文档第 7 节)
    3. 调用 LLM, 产出 场景->叙事分组建议->小分镜
    4. 构造 draft 组合 prompt (含约束锁 9 条)
    5. 解析引用主体填充 materials + audio_elements
    6. 落库 Scene/draft ShotGroup/Shot；正式组由合并接口生成
    """
    # 1. 加载分集
    ep_result = await db.execute(
        select(Episode).where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
    )
    episode = ep_result.scalar_one_or_none()
    if episode is None:
        raise ValueError(f"分集不存在: script_id={script_id}, episode_id={episode_id}")

    if not episode.script_text:
        raise ValueError(f"分集无剧本内容: episode_id={episode_id}")
    if expected_episode_version is not None and episode.episode_version != expected_episode_version:
        raise RuntimeError("storyboard input changed during generation")
    if expected_client_save_seq is not None and episode.client_save_seq != expected_client_save_seq:
        raise RuntimeError("storyboard input changed during generation")

    # 2. 加载该剧本主体列表
    subjects_result = await db.execute(
        select(Subject).where(Subject.script_id == script_id)
    )
    subjects = list(subjects_result.scalars().all())

    # 3. 构造 prompt 与短引用目录；UUID/资产 ID 不进入 Provider 输入输出合约。
    subject_references = _build_subject_reference_catalog(subjects, episode.episode_no)
    system_prompt = _SYSTEM_PROMPT
    user_prompt = _build_user_prompt(episode, subjects, subject_references)

    catalog = _subject_catalog(subjects, episode.episode_no)

    # 4. 调用 LLM；首答失败时最多进行一次不含原剧本的定向修复。
    validated = await _call_llm_with_contract_result(
        system_prompt,
        user_prompt,
        catalog,
        subject_references=subject_references,
    )
    contract = validated.contract
    scenes_data = contract.model_dump()["scenes"]

    fresh_ep_result = await db.execute(
        select(Episode)
        .where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    fresh_episode = fresh_ep_result.scalar_one_or_none()
    if (
        fresh_episode is None
        or (
            expected_episode_version is not None
            and fresh_episode.episode_version != expected_episode_version
        )
        or (
            expected_client_save_seq is not None
            and fresh_episode.client_save_seq != expected_client_save_seq
        )
    ):
        raise RuntimeError("storyboard input changed before commit")

    # 5. 清理旧分镜 (若存在)
    await _cleanup_existing_storyboard(db, episode_id)

    # 6. 落库
    total_scenes = 0
    total_groups = 0
    total_shots = 0

    for scene_index, scene_data in enumerate(scenes_data):
        source_scene = (
            validated.v2_contract.scenes[scene_index]
            if validated.v2_contract is not None
            else None
        )
        scene_no = scene_data["scene_no"]
        background = scene_data["background"]
        scene_key = f"bg:{background}"
        label = f"场 {scene_no} · {background}"

        # 创建 Scene
        source_groups = scene_data.get("shot_groups", [])
        scene = Scene(
            id=new_id(),
            episode_id=episode_id,
            scene_no=scene_no,
            scene_key=scene_key,
            background=background,
            label=label,
            # 此阶段只生成小分镜；正式视频单元由“合并镜头组”操作创建。
            shot_group_count=0,
            pending_shot_count=sum(len(group.get("shots", [])) for group in source_groups),
        )
        db.add(scene)
        total_scenes += 1

        for group_index, group_data in enumerate(source_groups):
            source_group = (
                source_scene.shot_groups[group_index]
                if source_scene is not None
                else None
            )
            group_no = group_data["group_no"]
            narrative_purpose = group_data.get("narrative_purpose", "")
            duration_seconds = group_data.get("duration_seconds", 0)
            shots_data = group_data.get("shots", [])

            # 构造组合 prompt
            shot_lines: list[str] = []
            all_referenced: list[str] = []
            for sd in shots_data:
                line = sd.get("shot_line", "")
                if line:
                    shot_lines.append(line)
                # 收集该镜头引用的全部主体名 (角色/道具/声音),
                # 背景主体在下方单独补入, 确保 materials 各分组均可填充
                # (04§1.15 materials {character/background/prop/other}, T7.4)
                for c in sd.get("characters", []):
                    if c and c not in all_referenced:
                        all_referenced.append(c)
                for p in sd.get("props", []):
                    if p and p not in all_referenced:
                        all_referenced.append(p)
                for s in sd.get("sounds", []):
                    if s and s not in all_referenced:
                        all_referenced.append(s)
            # 背景主体也算引用
            if background and background not in all_referenced:
                all_referenced.insert(0, background)

            group_prompt = _build_group_prompt(
                shot_lines=shot_lines,
                referenced_names=all_referenced,
                narrative_purpose=narrative_purpose,
            )

            if source_group is not None and source_scene is not None:
                material_refs = [source_scene.background_ref]
                audio_refs: list[str] = []
                for source_shot in source_group.shots:
                    material_refs.extend(source_shot.character_refs)
                    material_refs.extend(source_shot.prop_refs)
                    if (
                        source_shot.dialogue is not None
                        and source_shot.dialogue.speaker_ref is not None
                    ):
                        audio_refs.append(source_shot.dialogue.speaker_ref)
                    audio_refs.extend(source_shot.sound_refs)
                materials = _classify_reference_materials(
                    material_refs,
                    subject_references,
                )
                audio_elements = await _extract_audio_elements_from_references(
                    db,
                    audio_refs,
                    subject_references,
                )
            else:
                # 历史 v1 Provider 无短引用，仅在该兼容路径按名称解析。
                materials = _classify_materials(all_referenced, subjects)
                audio_elements = await _extract_audio_elements(db, shots_data, subjects)

            shot_group_id = new_id()
            # Shot 当前必须归属于 ShotGroup，因此用 draft 组保存分镜生成阶段的
            # 叙事顺序与 LLM 分组建议；draft 不是可提交视频生成的正式单元。
            unit_id = f"{DRAFT_SHOT_GROUP_UNIT_PREFIX}{shot_group_id}"
            unit_runtime_key = f"{episode_id}:{scene_no}:draft:{shot_group_id}"

            shot_group = ShotGroup(
                shot_group_id=shot_group_id,
                script_id=script_id,
                episode_id=episode_id,
                scene_no=scene_no,
                group_index=total_groups + 1,
                group_no=group_no,
                prompt=group_prompt,
                # 首尾帧链式衔接 (04§1.15 / 02§5.4.3 / T8.5):
                # 分镜生成阶段尚无帧, 全部置 None; 由 T8 视频生成 worker
                # 按组顺序回填 — 上一组 last_frame -> 下一组 first_frame,
                # 保证相邻镜头组视觉连续。链式衔接逻辑归 T8 实现。
                # 钩子: update_chain_frames(db, episode_id) 见本模块末尾。
                reference_frames={
                    "first_frame": None,
                    "last_frame": None,
                    "reference_frame": None,
                },
                duration_seconds=duration_seconds,
                video_duration_seconds=duration_seconds,
                materials=materials,
                audio_elements={
                    "characters": [item for item in audio_elements if item.get("type") == "character"],
                    "sounds": [item for item in audio_elements if item.get("type") == "sound"],
                },
                description_text=narrative_purpose,
                unit_id=unit_id,
                unit_runtime_key=unit_runtime_key,
            )
            db.add(shot_group)
            total_groups += 1

            # 创建 Shot
            for sd in shots_data:
                shot = Shot(
                    shot_id=new_id(),
                    scene_no=scene_no,
                    shot_group_id=shot_group_id,
                    group_no=group_no,
                    shot_no=_gen_shot_no(sd.get("display_no", 0)),
                    display_no=sd.get("display_no", 0),
                    content=sd.get("content", ""),
                    description=sd.get("description", ""),
                    shot_line=sd.get("shot_line", ""),
                    start_seconds=sd.get("start_seconds", 0),
                    end_seconds=sd.get("end_seconds", 0),
                    characters=list(sd.get("characters", []) or []),
                    props=list(sd.get("props", []) or []),
                    sounds=list(sd.get("sounds", []) or []),
                    dialogue_character=sd.get("dialogue_character", "") or "",
                )
                db.add(shot)
                total_shots += 1

    # 仅 flush 落入事务, 不 commit — 由调用方 (worker) 统一提交,
    # 保证 settle 失败时整条事务 (含已落库的分镜数据) 可由 worker 回滚,
    # 维持 worker 单事务一致性 (见 app/workers/storyboard.py)。
    await db.flush()

    logger.info(
        "分镜生成完成: script_id=%s, episode_id=%s, scenes=%d, groups=%d, shots=%d",
        script_id, episode_id, total_scenes, total_groups, total_shots,
    )

    return {
        "script_id": script_id,
        "episode_id": episode_id,
        "scenes": total_scenes,
        "shot_groups": total_groups,
        "shots": total_shots,
    }


async def _cleanup_existing_storyboard(db: AsyncSession, episode_id: str) -> None:
    """清理已存在的分镜数据 (重新生成前)。

    删除该分集下所有 Scene/ShotGroup/Shot, 以重新生成覆盖。
    若任何视频生成任务仍通过 shot_group_id 外键引用这些镜头组，抛 ConflictError
    提示先删除/等待相关视频任务，避免让数据库外键错误泄漏为 500。
    failed/canceled 任务仍是可审计的持久事实，不能隐式级联删除。
    """
    shot_group_ids = list(
        (
            await db.execute(
                select(ShotGroup.shot_group_id).where(
                    ShotGroup.episode_id == episode_id
                )
            )
        ).scalars().all()
    )
    if not shot_group_ids:
        # 无旧分镜，无需清理
        return

    blocking_result = await db.execute(
        select(VideoGenerationJob.job_id)
        .where(VideoGenerationJob.shot_group_id.in_(shot_group_ids))
        .limit(1)
    )
    if blocking_result.scalar_one_or_none() is not None:
        raise ConflictError(
            "该分集存在进行中或已生成视频的镜头组，"
            "请先删除或等待相关视频任务结束后再重新生成分镜"
        )

    # 先删 Shot (依赖 ShotGroup)
    await db.execute(delete(Shot).where(Shot.shot_group_id.in_(shot_group_ids)))

    # 删 ShotGroup
    await db.execute(
        delete(ShotGroup).where(ShotGroup.episode_id == episode_id)
    )

    # 删 Scene
    await db.execute(
        delete(Scene).where(Scene.episode_id == episode_id)
    )


async def update_chain_frames(
    db: AsyncSession, episode_id: str
) -> int:
    """首尾帧链式衔接钩子 (跨模块, 供 T8 视频生成 worker 调用)。

    按 scene_no/group_no 顺序遍历该分集所有镜头组, 将上一组 reference_frames.last_frame
    回填为下一组 reference_frames.first_frame, 实现相邻镜头组视觉连续 (04§1.15 / 02§5.4.3 / T8.5)。

    本钩子仅做衔接回填, 实际帧内容由 T8 视频生成 worker 在每组生成完成后写入 last_frame。
    返回回填的组数。

    跨模块依赖标注: 链式衔接的具体调用时序由 T8 worker 在每组视频生成成功后触发。
    """
    groups_result = await db.execute(
        select(ShotGroup)
        .where(ShotGroup.episode_id == episode_id)
        .order_by(ShotGroup.scene_no, ShotGroup.group_no)
    )
    groups = groups_result.scalars().all()
    updated = 0
    prev_last_frame: str | None = None
    for g in groups:
        frames = dict(g.reference_frames or {})
        # 上一组 last_frame -> 本组 first_frame
        if prev_last_frame is not None and not frames.get("first_frame"):
            frames["first_frame"] = prev_last_frame
            g.reference_frames = frames
            updated += 1
        # 推进: 当前组的 last_frame 作为下一组的 first_frame
        prev_last_frame = frames.get("last_frame")
    if updated:
        await db.flush()
    return updated


__all__ = [
    "generate_storyboard",
    "update_chain_frames",
    "CONSTRAINT_LOCK",
    "DESCRIPTION_TEMPLATE",
]
