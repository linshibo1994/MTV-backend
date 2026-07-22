"""LLM-5 分镜生成服务。

严格遵循 06-AI能力与Prompt工程文档.md 第 7 节 (分镜 Shot Prompt 结构) 与第 8 节 (视频生成约束锁)。

输入: 单集剧本 + 主体列表
LLM 产出: 场景 -> 叙事分组建议 -> 小分镜
每镜头: content (画面内容) + description (摄影级结构化描述)
每个 draft 组: 多镜头分时段建议 + 叙事目的；后续由合并接口发布为正式视频单元
解析引用主体填充 shot_group.materials {character/background/prop/other}
audio_elements 需配音角色
"""

from __future__ import annotations

import logging
from typing import Any

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
from app.schemas.pipeline_contracts import ProviderContractError
from app.schemas.storyboard_contracts import StoryboardContract, validate_storyboard_contract

logger = logging.getLogger(__name__)

_STORYBOARD_CONTRACT_MAX_ATTEMPTS = 3


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
    "{lighting}，{action}，{emotion}，{characters}:{dialogue}"
)


# ---------------------------------------------------------------------------
# LLM Prompt 构造
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """你是一位专业的短剧分镜导演与摄影指导。你的任务是将剧本拆分为分镜层级结构。

## 层级结构
Episode (集) -> Scene (场景, 按背景主体聚合) -> Shot Group (镜头组, 视频生成单元) -> Shot (镜头, 最小叙事单元)

## 输出要求

### Scene (场景)
- Scene 表示一次连续的时空与叙事段落；背景、日夜、时间或连续叙事段落变化时建立新 Scene
- 剧情离开后再次回到相同背景时也必须建立新的 scene_no，不得把不连续片段聚合到一起
- scene_no: 从 1 开始递增
- background: 背景主体名 (如 "养鸡场办公室-内-夜")

### Shot Group (叙事分组建议)
- 此阶段只负责给连续小分镜提供语义边界；用户点击“合并镜头组”后才发布为视频生成单元
- group_no: G1, G2, G3... (场景内递增)
- narrative_purpose: 该镜头组的叙事目的
- duration_seconds: 该镜头组的总时长 (秒), 为各镜头时长之和

### Shot (镜头)
- 每镜头包含: content (画面内容, 自然语言) + description (摄影级结构化描述)
- display_no: 每个镜头组内必须按 shots 数组顺序从 1 开始连续编号 (1, 2, 3...), 不得跳号或重复; 新镜头组重新从 1 开始
- description 必须严格使用模板: "在【{背景}】中采用【{机位}】拍摄视角，画面主体【{角色}】的关键动作是【{动作}】，整体表现的画面是【{画面}】，表达了【{情绪}】，整体画面的光影效果是【{光影}】，整个镜头的叙事目的是【{叙事目的}】。"
- shot_line: 该镜头在组合 prompt 中的单行, 格式 "【镜头N:起s～止s】场景是{场景}，{景别}·{机位}，{运镜}，{光影}，{画面动作}，{情绪}，{角色名}:{台词}"
- 景别可选: 全景/中景/近景/特写/俯拍特写
- 机位可选: 平视/俯拍/仰拍/侧拍
- 运镜可选: 固定/推轨/拉轨/摇镜/跟拍
- start_seconds / end_seconds: 该镜头在镜头组内的非负整数秒时间区间, 禁止小数
- characters: 出场角色名列表
- dialogue_character: 有台词的角色名 (如有)
- props: 该镜头出现的道具主体名列表 (引用主体列表中的 prop 类型, 无则空数组)
- sounds: 该镜头出现的声音主体名列表 (引用主体列表中的 sound 类型, 如背景音乐/音效主体, 无则空数组)

## 关键规则
1. 引用的主体名必须来自提供的主体列表
2. 一个镜头组必须包含 2~5 个镜头, 总时长 3~15 秒
3. 每组 display_no 必须严格为 1, 2, ..., N, 与 shots 数组顺序一致
4. 每组首镜头 start_seconds 必须为 0; 后续镜头 start_seconds 必须等于前一镜头 end_seconds
5. end_seconds 必须严格大于 start_seconds, 最后一个 end_seconds 必须等于 duration_seconds
6. 所有时间字段只能使用整数秒, 例如 0、3、7; 禁止 2.5 等小数
7. shot_line 中的镜头编号和起止秒数必须与 display_no/start_seconds/end_seconds 完全一致
8. description 的背景场景、角色、光影等需与剧本内容贴合
9. 只输出 JSON, 不要任何额外文本
"""


def _build_subject_list_block(subjects: list[Subject], episode_no: int) -> str:
    """构造主体列表文本块 (按类型分组)。"""
    by_type: dict[str, list[Subject]] = {}
    for s in subjects:
        # 仅保留该集出现的主体
        ep_nos = s.episode_nos or []
        if ep_nos and episode_no not in ep_nos:
            continue
        by_type.setdefault(s.type, []).append(s)

    type_labels = {
        "character": "角色",
        "scene": "场景",
        "background": "场景",
        "prop": "道具",
        "sound": "声音",
    }

    lines: list[str] = []
    for stype, items in by_type.items():
        label = type_labels.get(stype, stype)
        lines.append(f"\n### {label}")
        for s in items:
            desc = s.description or ""
            lines.append(f"- {s.name}: {desc}")
    return "\n".join(lines)


def _build_user_prompt(episode: Episode, subjects: list[Subject]) -> str:
    """构造用户 prompt: 单集剧本 + 主体列表。"""
    subject_block = _build_subject_list_block(subjects, episode.episode_no)
    return f"""请为以下剧本集生成分镜层级结构。

## 分集信息
- 集号: 第 {episode.episode_no} 集
- 标题: {episode.title}

## 剧本内容
{episode.script_text or '(无剧本内容)'}

## 可引用的主体列表
{subject_block}

## 输出前自检
- 每个 Scene 的 scene_no 从 1 连续递增
- 每个 Scene 内的 group_no 从 G1 连续递增
- 每个 Shot Group 内的 display_no 从 1 连续递增, 不跳号、不重复
- start_seconds/end_seconds 只能是非负整数; 首镜头从 0 开始, 相邻区间首尾相接
- 每个镜头必须满足 end_seconds > start_seconds, 末镜头 end_seconds 等于 duration_seconds

## 输出 JSON 格式
{{
  "scenes": [
    {{
      "scene_no": 1,
      "background": "背景主体名",
      "shot_groups": [
        {{
          "group_no": "G1",
          "narrative_purpose": "镜头组叙事目的",
          "duration_seconds": 7,
          "shots": [
            {{
              "display_no": 1,
              "content": "镜头画面内容 (自然语言)",
              "description": "在【背景】中采用【机位】拍摄视角，画面主体【角色】的关键动作是【动作】，整体表现的画面是【画面】，表达了【情绪】，整体画面的光影效果是【光影】，整个镜头的叙事目的是【叙事目的】。",
              "shot_line": "【镜头1:0s～4s】场景是背景，全景·平视机位，固定，光影描述，画面动作，情绪，角色名:台词",
              "start_seconds": 0,
              "end_seconds": 4,
              "characters": ["角色名1", "角色名2"],
              "dialogue_character": "有台词的角色名 (无则为空字符串)",
              "props": ["道具主体名1"],
              "sounds": ["声音主体名1"]
            }},
            {{
              "display_no": 2,
              "content": "下一镜头画面内容 (自然语言)",
              "description": "在【背景】中采用【机位】拍摄视角，画面主体【角色】的关键动作是【动作】，整体表现的画面是【画面】，表达了【情绪】，整体画面的光影效果是【光影】，整个镜头的叙事目的是【叙事目的】。",
              "shot_line": "【镜头2:4s～7s】场景是背景，中景·平视机位，固定，光影描述，画面动作，情绪，角色名:",
              "start_seconds": 4,
              "end_seconds": 7,
              "characters": ["角色名1"],
              "dialogue_character": "",
              "props": [],
              "sounds": []
            }}
          ]
        }}
      ]
    }}
  ]
}}
"""


# LLM 响应 JSON Schema (用于 response_format 引导)
_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "scenes": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "scene_no": {"type": "integer", "minimum": 1},
                    "background": {"type": "string"},
                    "shot_groups": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "group_no": {"type": "string"},
                                "narrative_purpose": {"type": "string"},
                                "duration_seconds": {
                                    "type": "integer",
                                    "minimum": 3,
                                    "maximum": 15,
                                },
                                "shots": {
                                    "type": "array",
                                    "minItems": 2,
                                    "maxItems": 5,
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "display_no": {"type": "integer", "minimum": 1},
                                            "content": {"type": "string"},
                                            "description": {"type": "string"},
                                            "shot_line": {"type": "string"},
                                            "start_seconds": {"type": "integer", "minimum": 0},
                                            "end_seconds": {"type": "integer", "minimum": 0},
                                            "characters": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                            "dialogue_character": {"type": "string"},
                                            "props": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                            "sounds": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                        },
                                        "required": [
                                            "display_no", "content", "description",
                                            "shot_line", "start_seconds", "end_seconds",
                                            "characters", "dialogue_character",
                                            "props", "sounds",
                                        ],
                                    },
                                },
                            },
                            "required": [
                                "group_no", "narrative_purpose", "duration_seconds", "shots",
                            ],
                        },
                    },
                },
                "required": ["scene_no", "background", "shot_groups"],
            },
        },
    },
    "required": ["scenes"],
}


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


def _subject_catalog(subjects: list[Subject], episode_no: int) -> dict[str, str]:
    catalog: dict[str, str] = {}
    for subject in subjects:
        episode_nos = subject.episode_nos or []
        if episode_nos and episode_no not in episode_nos:
            continue
        stype = "background" if subject.type == "scene" else subject.type
        catalog[subject.name] = stype
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


async def _call_llm(system: str, user: str) -> dict:
    """调用 LLM-5 分镜生成, 返回 JSON dict。

    按 PLAN 第 5.1 节 LLMProvider 协议 (complete_json(system, user)) 调用。
    期望的 JSON 形态已通过 _build_user_prompt 中的示例描述给模型，
    OpenAI json_object 模式不消费 schema 参数，故不向 provider 传入 schema。
    缺 AI key 时 provider 抛 RuntimeError，由 worker 层捕获并 release 积分。
    """
    from app.providers import get_llm_provider

    provider = get_llm_provider()
    return await provider.complete_json(system=system, user=user)


async def _call_llm_with_contract(
    system: str,
    user: str,
    subject_catalog: dict[str, str],
) -> StoryboardContract:
    """在单个分镜任务内有限重试 Provider 契约错误。"""
    for attempt in range(1, _STORYBOARD_CONTRACT_MAX_ATTEMPTS + 1):
        payload = await _call_llm(system, user)
        try:
            return validate_storyboard_contract(payload, subject_catalog)
        except ValueError:
            logger.warning(
                "分镜 Provider 第 %d/%d 次响应未通过契约",
                attempt,
                _STORYBOARD_CONTRACT_MAX_ATTEMPTS,
            )
    raise ProviderContractError(
        f"供应商响应在 {_STORYBOARD_CONTRACT_MAX_ATTEMPTS} 次总预算内"
        "不符合 StoryboardContract 合约"
    ) from None


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

    # 3. 构造 prompt
    system_prompt = _SYSTEM_PROMPT
    user_prompt = _build_user_prompt(episode, subjects)

    catalog = _subject_catalog(subjects, episode.episode_no)

    # 4. 调用 LLM；契约失败在同一任务内最多追加两次尝试。
    contract = await _call_llm_with_contract(system_prompt, user_prompt, catalog)
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

    for scene_data in scenes_data:
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

        for group_data in source_groups:
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

            # 解析 materials 分组
            materials = _classify_materials(all_referenced, subjects)

            # 提取 audio_elements (需配音角色)
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
