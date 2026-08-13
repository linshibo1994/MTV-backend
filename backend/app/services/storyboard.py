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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeGuard, cast

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.episode import Episode
from app.models.scene import Scene
from app.models.scene_audition import SceneAudition
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
    StoryboardV2SceneContract,
    StoryboardV2ShotContract,
    StoryboardV2ShotGroupContract,
    storyboard_contract_diagnostic,
    validate_storyboard_contract,
    validate_storyboard_v2_contract,
)
from app.services.boundary_projection import project_boundary
from app.services.optional_query import (
    can_fallback_from_optional_query,
    execute_optional_query,
)
from app.services.prompt_template import (
    PromptTemplateSnapshot,
    render_prompt_template,
    render_prompt_template_with_fallback,
    resolve_prompt_bundle,
)
from app.services.storyboard_provenance import (
    build_storyboard_provenance,
    storyboard_provenance_sha256,
)
from app.services.storyboard_quality import (
    QualityReport,
    QualityWarning,
    ScriptBlock,
    build_script_blocks,
    build_shot_group_quality_summary,
    build_shot_structured_data,
    evaluate_storyboard_quality,
)

logger = logging.getLogger(__name__)

_STORYBOARD_CONTRACT_MAX_ATTEMPTS = 2
_STORYBOARD_REPAIR_PAYLOAD_MAX_CHARS = 120_000

_CHAIN_LAST_FRAME_MISSING = "chain_last_frame_missing"
_CHAIN_LINK_BROKEN = "chain_link_broken"
_CONTINUATION_PREDECESSOR_LAST_FRAME_UNAVAILABLE = (
    "continuation_predecessor_last_frame_unavailable"
)
_OWNED_CHAIN_DIAGNOSTIC_CODES = frozenset(
    {
        _CHAIN_LAST_FRAME_MISSING,
        _CHAIN_LINK_BROKEN,
        _CONTINUATION_PREDECESSOR_LAST_FRAME_UNAVAILABLE,
    }
)


class ChainFrameUpdateResult(int):
    """链帧更新结果；保持历史 ``int`` 返回值兼容并附带脱敏诊断。"""

    updated: int
    diagnostics: list[dict[str, object]]

    def __new__(
        cls,
        updated: int,
        diagnostics: list[dict[str, object]],
    ) -> ChainFrameUpdateResult:
        result = super().__new__(cls, updated)
        result.updated = updated
        result.diagnostics = [dict(item) for item in diagnostics]
        return result

    def model_dump(self) -> dict[str, object]:
        """提供与 API/测试常用模型一致的字典形态。"""

        return {
            "updated": self.updated,
            "diagnostics": [dict(item) for item in self.diagnostics],
        }


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

_STORYBOARD_SYSTEM_TEMPLATE_KEY = "storyboard.system"
_STORYBOARD_USER_TEMPLATE_KEY = "storyboard.user"
_STORYBOARD_DESCRIPTION_TEMPLATE_KEY = "storyboard.description"
_STORYBOARD_SHOT_TEMPLATE_KEY = "storyboard.shot"
_STORYBOARD_GROUP_TEMPLATE_KEY = "storyboard.group"
_STORYBOARD_CONSTRAINT_TEMPLATE_KEY = "storyboard.constraint"


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
9. 每镜用 source_block_refs 指明落实的 Bxx 原文段；无法落实的段只能在根级 omitted_source_blocks 声明并给出原因。
10. 用 start_boundary/end_boundary 记录镜头起止时刻可见的人物位置、朝向、视线、手部、道具与状态；不可见事实留空。
11. character_positions 使用九宫格位置与受控朝向；audience_visibility 明确事实现在展示或暂缓揭示，暂缓时给出触发条件与保护方法。

## 受控摄影词表
- shot_size: {'/'.join(SHOT_SIZE_VALUES)}
- composition: {'/'.join(COMPOSITION_VALUES)}（每镜 1~3 项）
- camera_angle: {'/'.join(CAMERA_ANGLE_VALUES)}
- camera_movement: {'/'.join(CAMERA_MOVEMENT_VALUES)}

## 结构与时码
- schema_version 必须是 storyboard.v2。
- scene_no 从 1 连续递增；每个 Scene 内 group_no 从 G1 连续递增。
- 每组 1~5 镜，duration_seconds 为 3~15 的整数秒。单镜头组仅允许用于定场、单一反应镜或硬切转场三类非主体叙事用途，且全集单镜头组数量不得超过镜头组总数的 1/3；凡承载对白、冲突展开、动作推进或情绪转折的叙事主体单元必须使用 2~5 镜的连续镜头组，不得用 1 镜带过。
- 每组 display_no 必须严格为 1, 2, ..., N；首镜 start_seconds=0，相邻区间首尾相接。
- end_seconds 必须大于 start_seconds，末镜 end_seconds 必须等于 duration_seconds；所有时码只能是整数。
- background_ref 只能引用场景；character_refs 只能引用角色；prop_refs 只能引用道具；sound_refs 只能引用声音。
- kind=dialogue/inner_monologue 时 speaker_ref 必须是当前镜头可见角色；kind=voice_over 时 speaker_ref 可为角色、声音主体或 null。
- narrative_purpose 要说明该镜头让观众获得什么信息、感受什么变化，不能只写“推进剧情”。
- sound_design 写可执行的环境音/动作音/声音衔接，无声音时写“无”。

## 覆盖度（最高优先级，强制，优先于一切结构规则）
1. 必须覆盖剧本从开头到结尾的全部叙事节拍：每一处场景切换、每一次台词交锋、每一个动作与情绪转折都要落到具体镜头，不得合并、跳过或用一句话带过。
2. 不得把整场戏压缩进一个镜头组；除非该场戏本身只有一句定场旁白，否则每场戏至少拆为 2 个镜头组。
3. 剧本中的全部对白、旁白、内心独白必须逐字保留在对应镜头的 dialogue.text 中；遗漏台词视为覆盖失败。
4. 用户提示会给出由剧本字数推导的“本集覆盖度下限”（最少镜头数/组数/场数/总秒数），这是下限不是目标；实际产出应达到或超过该下限，低于下限会被判定为不合格并要求重做。
5. 禁止用“一个全景镜 + 长 duration_seconds”敷衍整场戏；duration_seconds 上限 15 秒，长戏必须拆成多个 3~15 秒的镜头组。
6. 不得为凑数量注水无叙事作用的装饰镜头；每镜只承载一个主要动作或信息变化。
7. 用户提示中的每个 Bxx 必须出现在至少一个镜头的 source_block_refs，或出现在根级 omitted_source_blocks 并说明原因。

只输出符合 JSON Schema 的 JSON 对象，不要 markdown 或解释文字。
"""

_USER_PROMPT_TEMPLATE_FALLBACK = "{{ default_prompt }}"
_DESCRIPTION_TEMPLATE_FALLBACK = (
    "在【{{ background }}】中采用【{{ camera }}】拍摄视角，"
    "画面主体【{{ characters }}】的关键动作是【{{ action }}】，"
    "整体表现的画面是【{{ scene }}】，"
    "表达了【{{ emotion }}】，"
    "整体画面的光影效果是【{{ lighting }}】，"
    "整个镜头的叙事目的是【{{ narrative_purpose }}】。"
)
_SHOT_TEMPLATE_FALLBACK = (
    "【镜头{{ n }}:{{ start }}s～{{ end }}s】场景是{{ scene }}，"
    "{{ shot_scale }}·{{ camera }}，{{ camera_movement }}，"
    "{{ lighting }}，{{ action }}，{{ emotion }}，{{ characters }}:{{ dialogue }}，"
    "构图是【{{ composition }}】，声音设计是【{{ sound_design }}】，"
    "台词类型是【{{ dialogue_kind }}】"
)
_GROUP_TEMPLATE_FALLBACK = (
    "{{ shot_lines }}\n【角色】{{ referenced_names }}\n"
    "【叙事目的】{{ narrative_purpose }}\n"
    "{{ director_commands }}{{ constraint_lock }}"
)


@dataclass(frozen=True)
class _StoryboardPromptContext:
    system_prompt: str
    user_prompt: str
    constraint_lock: str
    templates: Mapping[str, PromptTemplateSnapshot]


@dataclass(frozen=True)
class _StoryboardResolvedInput:
    """一次分镜任务冻结的全部提示词输入与脱敏溯源。"""

    prompts: _StoryboardPromptContext
    script_blocks: list[ScriptBlock]
    provenance: dict[str, object]


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


# 覆盖度软门阈值：边界抖动时不反复触发重生成。0.8 表示实际值达到下限 80%
# 即视为合格，只有明显稀疏（如 1 场 1 组 3 镜）才会触发带剧本的重生成。
_COVERAGE_SLACK = 0.8


@dataclass(frozen=True)
class _CoverageReport:
    """分镜覆盖度评估结果：镜头/组/场/总时长是否足以承载整集剧本。"""

    shot_count: int
    group_count: int
    scene_count: int
    total_duration: int
    min_shots: int
    min_groups: int
    min_scenes: int
    min_duration: int

    @property
    def sufficient(self) -> bool:
        return (
            self.shot_count >= int(self.min_shots * _COVERAGE_SLACK)
            and self.group_count >= int(self.min_groups * _COVERAGE_SLACK)
            and self.total_duration >= int(self.min_duration * _COVERAGE_SLACK)
        )


def _coverage_minimums(script_text: str) -> tuple[int, int, int, int]:
    """由剧本字数推导覆盖度下限（最小镜头/组/场/总秒数）。

    校准自真实「好输出」：1365 字 -> 4 场 9 组 24 镜 108s，约 57 字/镜。
    取 60 字/镜略宽于 57，确保好输出 comfortably pass；每镜保守 4 秒。
    代入 1365 字：min_shots=23 / min_groups=8 / min_scenes=3 / min_duration=92，
    好输出 24/9/4/108 全过，差输出 3/1/1/15 全不过。
    """
    chars = len((script_text or "").strip())
    min_shots = max(6, (chars + 59) // 60)
    min_groups = max(3, (min_shots + 2) // 3)
    min_scenes = max(2, (min_groups + 2) // 3)
    min_duration = min_shots * 4
    return min_shots, min_groups, min_scenes, min_duration


def _check_coverage(
    validated: _ValidatedStoryboard, script_text: str
) -> _CoverageReport:
    """统计已校验分镜的镜头/组/场/总时长，与下限比对。"""
    min_shots, min_groups, min_scenes, min_duration = _coverage_minimums(script_text)
    scene_count = 0
    group_count = 0
    shot_count = 0
    total_duration = 0
    if validated.v2_contract is not None:
        for scene in validated.v2_contract.scenes:
            scene_count += 1
            for group in scene.shot_groups:
                group_count += 1
                shot_count += len(group.shots)
                total_duration += group.duration_seconds
    else:
        contract_scenes = getattr(validated.contract, "scenes", None)
        if contract_scenes is None:
            # 部分调用方（以及历史测试替身）只实现 model_dump；兼容该稳定协议，
            # 同时不放宽正式 Pydantic 合约的校验。
            dumped_contract = validated.contract.model_dump()
            contract_scenes = dumped_contract.get("scenes", [])
        for v1_scene in contract_scenes:
            if isinstance(v1_scene, dict):
                groups = v1_scene.get("shot_groups", [])
            else:
                groups = v1_scene.shot_groups
            scene_count += 1
            for v1_group in groups:
                if isinstance(v1_group, dict):
                    shots = v1_group.get("shots", [])
                    duration = v1_group.get("duration_seconds", 0)
                else:
                    shots = v1_group.shots
                    duration = v1_group.duration_seconds
                group_count += 1
                shot_count += len(shots)
                total_duration += int(duration)
    return _CoverageReport(
        shot_count=shot_count,
        group_count=group_count,
        scene_count=scene_count,
        total_duration=total_duration,
        min_shots=min_shots,
        min_groups=min_groups,
        min_scenes=min_scenes,
        min_duration=min_duration,
    )


def _coverage_emphasis_suffix(report: _CoverageReport) -> str:
    """覆盖度不足时追加到原 prompt 的重做指令（原 prompt 已含剧本，等价带剧本重生成）。"""
    return f"""

## 覆盖度不足，必须重做
你上一次只产出 {report.shot_count} 镜 / {report.group_count} 组 / {report.scene_count} 场 / 总时长 {report.total_duration}s，
远低于本集下限 {report.min_shots} 镜 / {report.min_groups} 组 / {report.min_scenes} 场 / {report.min_duration}s。
请把整集剧本从开头到结尾逐段拆解为更多场景、镜头组与镜头，覆盖每一处场景切换、台词交锋、动作与情绪转折；
叙事主体必须用 2~5 镜连续镜头组，单镜头组仅用于定场/反应/转场；不要用少数镜头概括整集。
重新输出完整的 storyboard.v2 JSON。
"""


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
    *,
    script_blocks: list[ScriptBlock] | None = None,
    director_plan_block: str = "",
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
    script_text = episode.script_text or ""
    source_blocks = build_script_blocks(script_text) if script_blocks is None else script_blocks
    script_block_text = "\n\n".join(
        f"[{block.block_ref}] {block.text}" for block in source_blocks
    ) or "（无剧本内容）"
    script_chars = len(script_text.strip())
    min_shots, min_groups, min_scenes, min_duration = _coverage_minimums(script_text)
    director_plan_section = (
        f"\n## 已确认导演方案\n以下方案已由用户确认，分镜必须落实其六维选择。\n"
        f"{director_plan_block}\n"
        if director_plan_block
        else ""
    )
    return f"""请为以下分集生成 storyboard.v2 分镜。

## 分集
- 集号：第 {episode.episode_no} 集
- 标题：{episode.title}

## 剧本原文落实表
以下 Bxx 是服务端生成的稳定段落编号。每镜必须在 source_block_refs 中列出其落实的段落；
确实无法拍摄或有意省略的段落必须写入根级 omitted_source_blocks，并说明 reason。
{script_block_text}

## 本集覆盖度下限（由剧本 {script_chars} 字推导，必须达到或超过）
- 最少：{min_shots} 镜 / {min_groups} 组 / {min_scenes} 场 / 总时长 {min_duration} 秒。
- 这是“最少”而非目标值；低于以上下限的输出会被判定为覆盖不足并要求重做。
- 请按覆盖全部叙事段落的原则逐段拆镜，不要走最省事的合法路径（如 1 场 1 组 3 镜概括整集）。

## 当前分集可引用主体目录
格式为：短引用 | 类型 | 名称 | 描述 | 参考图状态。JSON 中只能填写短引用。
{subject_block}
{director_plan_section}

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
              "source_block_refs": ["B01"],
              "character_refs": ["C01"],
              "prop_refs": ["P01"],
              "sound_refs": ["S01"],
              "sound_design": "门轴轻响，室内环境声短暂停顿",
              "start_boundary": {{
                "positions": [{{"ref": "C01", "position": "左中", "facing": "面朝右"}}],
                "visible_state": ["角色手握门把"]
              }},
              "end_boundary": {{
                "positions": [{{"ref": "C01", "position": "正中", "facing": "面朝右"}}],
                "visible_state": ["角色已停在门口"]
              }},
              "character_positions": [
                {{"ref": "C01", "position": "正中", "facing": "面朝右"}}
              ],
              "audience_visibility": [],
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
              "source_block_refs": ["B01"],
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
  ],
  "omitted_source_blocks": []
}}

## 输出前自检（逐条核对，任一不满足必须在同一回答内重新规划至达标后再输出最终 JSON）
- 覆盖度数量核对：镜头数 ≥ {min_shots}、镜头组数 ≥ {min_groups}、场数 ≥ {min_scenes}、总时长 ≥ {min_duration} 秒；任一不满足不要输出当前 JSON，请重新规划至全部达标后再输出；
- 叙事覆盖核对：剧本每一处场景切换、每一次台词交锋、每一个动作/情绪转折是否都落到具体镜头；是否有台词被遗漏或合并；
- 结构密度核对：叙事主体镜头组是否都含 2~5 镜；单镜头组是否仅用于定场/反应/转场且不超过组数 1/3；
- 台词保真核对：对白/旁白/内心独白是否逐字保留在 dialogue.text，未增删改写；无语言内容时 dialogue 是否为 null；
- scene_no、group_no、display_no 是否连续；
- 时码是否全部为整数、连续且与组时长一致；
- 所有短引用是否存在且类型正确；
- 每个 Bxx 是否恰好被镜头落实，或在根级 omitted_source_blocks 中说明省略原因；
- 每镜起止边界、角色九宫格位置/朝向与观众可见性是否按可见事实填写；
- 每镜是否都有明确景别、构图、机位、运镜、动作、光线、情绪和叙事目的；
- 相邻镜头的动作、视线、空间和情绪是否自然承接。
"""


async def _selected_scene_audition_block(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    input_version: int,
) -> str:
    """只读取当前输入版本已确认的导演方案，并渲染六维答案。"""
    try:
        result = await execute_optional_query(
            db,
            select(SceneAudition)
            .where(
                SceneAudition.script_id == script_id,
                SceneAudition.episode_id == episode_id,
                SceneAudition.input_version == input_version,
                SceneAudition.status == "selected",
                SceneAudition.selected_plan_id.is_not(None),
            )
            .order_by(SceneAudition.scene_no, SceneAudition.id)
        )
    except Exception as exc:
        if not can_fallback_from_optional_query(exc):
            raise
        # 灰度升级或最小测试数据库尚未包含导演选案表时，保持历史生成路径。
        logger.warning("读取已确认导演方案失败，按未选案继续 episode_id=%s", episode_id)
        return ""
    dimensions = (
        ("audience_knowledge_timing", "观众知情时机"),
        ("alignment_target", "对齐对象"),
        ("performance_space", "表演空间"),
        ("strongest_image", "最强画面"),
        ("landing", "落点"),
        ("tradeoff", "损失"),
    )
    blocks: list[str] = []
    for audition in result.scalars().all():
        selected = next(
            (
                plan
                for plan in audition.plans or []
                if isinstance(plan, dict)
                and str(plan.get("plan_id") or "") == audition.selected_plan_id
            ),
            None,
        )
        if selected is None:
            continue
        plan_name = _safe_template_text(str(selected.get("name") or "已确认方案"))
        lines = [f"### 场 {audition.scene_no} · {plan_name}"]
        for key, label in dimensions:
            raw_value = selected.get(key)
            if isinstance(raw_value, str) and raw_value.strip():
                lines.append(f"- {label}：{_safe_template_text(raw_value)}")
        if len(lines) == len(dimensions) + 1:
            blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _prompt_template_metadata(
    templates: Mapping[str, PromptTemplateSnapshot],
) -> dict[str, dict[str, object]]:
    return {
        key: dict(snapshot.public_metadata())
        for key, snapshot in sorted(templates.items())
    }


async def _resolve_storyboard_prompt_context(
    db: AsyncSession,
    *,
    default_user_prompt: str,
) -> _StoryboardPromptContext:
    fallbacks = {
        _STORYBOARD_SYSTEM_TEMPLATE_KEY: _SYSTEM_PROMPT,
        _STORYBOARD_USER_TEMPLATE_KEY: _USER_PROMPT_TEMPLATE_FALLBACK,
        _STORYBOARD_DESCRIPTION_TEMPLATE_KEY: _DESCRIPTION_TEMPLATE_FALLBACK,
        _STORYBOARD_SHOT_TEMPLATE_KEY: _SHOT_TEMPLATE_FALLBACK,
        _STORYBOARD_GROUP_TEMPLATE_KEY: _GROUP_TEMPLATE_FALLBACK,
        _STORYBOARD_CONSTRAINT_TEMPLATE_KEY: CONSTRAINT_LOCK,
    }
    templates = await resolve_prompt_bundle(db, fallbacks)
    system_prompt, system_snapshot = render_prompt_template_with_fallback(
        templates[_STORYBOARD_SYSTEM_TEMPLATE_KEY],
        {},
        fallback=_SYSTEM_PROMPT,
    )
    user_prompt, user_snapshot = render_prompt_template_with_fallback(
        templates[_STORYBOARD_USER_TEMPLATE_KEY],
        {"default_prompt": default_user_prompt},
        fallback=_USER_PROMPT_TEMPLATE_FALLBACK,
    )
    constraint_lock, constraint_snapshot = render_prompt_template_with_fallback(
        templates[_STORYBOARD_CONSTRAINT_TEMPLATE_KEY],
        {},
        fallback=CONSTRAINT_LOCK,
    )
    actual = dict(templates)
    actual[_STORYBOARD_SYSTEM_TEMPLATE_KEY] = system_snapshot
    actual[_STORYBOARD_USER_TEMPLATE_KEY] = user_snapshot
    actual[_STORYBOARD_CONSTRAINT_TEMPLATE_KEY] = constraint_snapshot
    # 在调用供应商前，用完整变量集合预渲染派生模板。若活动版本不可渲染，
    # 立即回退并冻结实际使用的快照，避免落库中途才发现版本不可用。
    _, description_snapshot = render_prompt_template_with_fallback(
        actual[_STORYBOARD_DESCRIPTION_TEMPLATE_KEY],
        {
            "background": "场景",
            "camera": "平视",
            "characters": "角色",
            "action": "动作",
            "scene": "画面",
            "emotion": "情绪",
            "lighting": "光影",
            "narrative_purpose": "叙事目的",
        },
        fallback=_DESCRIPTION_TEMPLATE_FALLBACK,
    )
    _, shot_snapshot = render_prompt_template_with_fallback(
        actual[_STORYBOARD_SHOT_TEMPLATE_KEY],
        {
            "n": 1,
            "start": 0,
            "end": 3,
            "scene": "场景",
            "shot_scale": "中景",
            "camera": "平视",
            "camera_movement": "固定",
            "lighting": "光影",
            "action": "动作",
            "emotion": "情绪",
            "characters": "角色",
            "dialogue": "台词",
            "composition": "三分法",
            "sound_design": "环境音",
            "dialogue_kind": "dialogue",
        },
        fallback=_SHOT_TEMPLATE_FALLBACK,
    )
    _, group_snapshot = render_prompt_template_with_fallback(
        actual[_STORYBOARD_GROUP_TEMPLATE_KEY],
        {
            "shot_lines": "镜头行",
            "referenced_names": "角色",
            "narrative_purpose": "叙事目的",
            "director_commands": "",
            "constraint_lock": constraint_lock,
        },
        fallback=_GROUP_TEMPLATE_FALLBACK,
    )
    actual[_STORYBOARD_DESCRIPTION_TEMPLATE_KEY] = description_snapshot
    actual[_STORYBOARD_SHOT_TEMPLATE_KEY] = shot_snapshot
    actual[_STORYBOARD_GROUP_TEMPLATE_KEY] = group_snapshot
    return _StoryboardPromptContext(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        constraint_lock=constraint_lock,
        templates=actual,
    )


def _subject_provenance_snapshot(subject: Subject) -> dict[str, object]:
    return {
        "subject_id": str(subject.id),
        "type": subject.type,
        "subject_version": int(getattr(subject, "subject_version", 1) or 1),
        "enrich_version": int(getattr(subject, "enrich_version", 1) or 1),
        "is_active": bool(getattr(subject, "is_active", True)),
        "episode_nos": list(subject.episode_nos or []),
    }


async def _resolve_storyboard_input(
    db: AsyncSession,
    *,
    script_id: str,
    episode: Episode,
    subjects: list[Subject],
    subject_references: Mapping[str, _SubjectReference],
) -> _StoryboardResolvedInput:
    """冻结本次任务的模板 bundle、选案提示与不可逆输入锚点。"""
    script_blocks = build_script_blocks(episode.script_text or "")
    director_plan_block = await _selected_scene_audition_block(
        db,
        script_id=script_id,
        episode_id=episode.episode_id,
        input_version=int(episode.episode_version),
    )
    default_user_prompt = _build_user_prompt(
        episode,
        subjects,
        dict(subject_references),
        script_blocks=script_blocks,
        director_plan_block=director_plan_block,
    )
    prompts = await _resolve_storyboard_prompt_context(
        db,
        default_user_prompt=default_user_prompt,
    )
    subject_by_id = {str(subject.id): subject for subject in subjects}
    catalog = [
        _subject_provenance_snapshot(subject_by_id[reference.subject_id])
        for reference in subject_references.values()
        if reference.subject_id in subject_by_id
    ]
    provenance = build_storyboard_provenance(
        episode_script=episode.script_text or "",
        episode_version=int(episode.episode_version),
        client_save_seq=int(getattr(episode, "client_save_seq", 0) or 0),
        subject_catalog=catalog,
        system_prompt=prompts.system_prompt,
        user_prompt=prompts.user_prompt,
        prompt_templates=_prompt_template_metadata(prompts.templates),
    )
    return _StoryboardResolvedInput(
        prompts=prompts,
        script_blocks=script_blocks,
        provenance=provenance,
    )


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
    *,
    director_commands: list[str] | None = None,
    group_template: PromptTemplateSnapshot | None = None,
    constraint_lock: str = CONSTRAINT_LOCK,
) -> str:
    """构造镜头组组合 prompt。

    storyboard.v2 已从结构事实派生导演指令时只发送压缩摘要；历史 v1
    或结构快照缺失时才追加完整约束锁。模板快照本身保持不变，只切换
    本次渲染的变量值，确保 provenance 仍能准确指向实际解析的模板版本。
    """
    lines_block = "\n".join(shot_lines)
    characters_block = "、".join(referenced_names)
    normalized_director_commands = [
        command.strip() for command in (director_commands or []) if command.strip()
    ]
    director_block = "\n".join(normalized_director_commands)
    if director_block:
        director_block += "\n"
    constraint_block = "" if director_block else constraint_lock
    variables = {
        "shot_lines": lines_block,
        "referenced_names": characters_block,
        "narrative_purpose": narrative_purpose,
        "director_commands": director_block,
        "constraint_lock": constraint_block,
    }
    if group_template is not None:
        return render_prompt_template(group_template, variables)
    return (
        f"{lines_block}\n"
        f"【角色】{characters_block}\n"
        f"【叙事目的】{narrative_purpose}\n"
        f"{director_block}"
        f"{constraint_block}"
    )


def _warnings_for_group(
    report: QualityReport,
    *,
    scene_index: int,
    group_index: int,
) -> list[QualityWarning]:
    """截取组内告警；场景级连续性告警同步附到该场景各组。"""

    scene_path = f"scenes[{scene_index}]"
    group_path = f"{scene_path}.shot_groups[{group_index}]"
    return [
        warning
        for warning in report.warnings
        if warning.path.startswith(group_path)
        or warning.path == scene_path
        or warning.path.startswith(f"{scene_path}.shots[")
    ]


_CONTINUATION_CAMERA_MOVEMENTS = frozenset({"固定", "推镜"})
_CONTINUATION_BREAK_WORDS = ("跑开", "转身", "背对", "离开画面", "出画")


def _strict_continuation_flags(scene: StoryboardV2SceneContract) -> list[bool]:
    """对新生成镜头组做保守的续帧收紧。

    只有 Provider 显式提出续帧，且组内全部为固定/推镜，上组尾镜无明显
    离场或转身，本组首镜人物不超出上组尾镜人物时才启用。
    """

    flags: list[bool] = []
    previous_last: StoryboardV2ShotContract | None = None
    for group in scene.shot_groups:
        first = group.shots[0]
        movements_ok = all(
            shot.visual.camera_movement in _CONTINUATION_CAMERA_MOVEMENTS
            for shot in group.shots
        )
        previous_ok = previous_last is not None and not any(
            word in previous_last.visual.frame_description
            or word in previous_last.visual.action
            for word in _CONTINUATION_BREAK_WORDS
        )
        subjects_ok = previous_last is not None and set(first.character_refs).issubset(
            previous_last.character_refs
        )
        flags.append(bool(group.is_continuation and movements_ok and previous_ok and subjects_ok))
        previous_last = group.shots[-1]
    return flags


def _build_boundary_reference_frames(
    group: StoryboardV2ShotGroupContract | None,
    *,
    subject_names_by_ref: dict[str, str],
) -> dict[str, object]:
    """把组首/尾的结构边界投影到 reference_frames，旧帧键始终保留。"""

    frames: dict[str, object] = {
        "first_frame": None,
        "last_frame": None,
        "reference_frame": None,
    }
    if group is None:
        return frames
    projections: dict[str, object] = {}
    start = project_boundary(
        group.shots[0].start_boundary,
        role="start",
        subject_names=subject_names_by_ref,
    )
    end = project_boundary(
        group.shots[-1].end_boundary,
        role="end",
        subject_names=subject_names_by_ref,
    )
    for role, projection in (("start", start), ("end", end)):
        if projection is not None:
            projections[role] = {
                "prompt_fragment": projection.prompt_fragment,
                "snapshot": projection.snapshot,
            }
    if projections:
        frames["boundary_projections"] = projections
    return frames


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
        item = {"ref": ref, "subject_id": subject.subject_id, "name": subject.name}
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
    description_template: PromptTemplateSnapshot | None = None,
    shot_template: PromptTemplateSnapshot | None = None,
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
                description_variables = {
                    "background": _safe_template_text(background),
                    "camera": visual.camera_angle,
                    "characters": _safe_template_text(character_text),
                    "action": action,
                    "scene": frame_description,
                    "emotion": emotion,
                    "lighting": lighting,
                    "narrative_purpose": narrative_purpose,
                }
                description = (
                    render_prompt_template(description_template, description_variables)
                    if description_template is not None
                    else DESCRIPTION_TEMPLATE.format(**description_variables)
                )
                shot_variables = {
                    "n": shot.display_no,
                    "start": shot.start_seconds,
                    "end": shot.end_seconds,
                    "scene": _safe_shot_line_text(background),
                    "shot_scale": visual.shot_size,
                    "camera": visual.camera_angle,
                    "camera_movement": visual.camera_movement,
                    "lighting": _safe_shot_line_text(lighting),
                    "action": _safe_shot_line_text(action),
                    "emotion": _safe_shot_line_text(emotion),
                    "characters": _safe_shot_line_text(character_text),
                    "dialogue": dialogue_text,
                    "composition": "、".join(visual.composition),
                    "sound_design": _safe_template_text(shot.sound_design),
                    "dialogue_kind": dialogue_kind,
                }
                shot_line = (
                    render_prompt_template(shot_template, shot_variables)
                    if shot_template is not None
                    else SHOT_LINE_TEMPLATE.format(**shot_variables)
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


def _is_int(value: object) -> TypeGuard[int]:
    """严格整数判定；bool 不视作整数，避免把 True/False 误当 1/0。"""
    return isinstance(value, int) and not isinstance(value, bool)


def _try_scale_durations(durations: list[int], target: int) -> list[int] | None:
    """把单镜时长等比例缩放到 target，每镜至少 1 秒。

    返回 None 表示无法在不违反「每镜 ≥1 秒」的前提下缩放（例如镜头数超过
    目标秒数）；调用方应回退到平铺总时长或交给严格校验处理。
    """
    n = len(durations)
    total = sum(durations)
    if n == 0 or total <= 0 or target < n:
        return None
    scaled: list[int] = []
    accumulated = 0
    for index in range(n - 1):
        # 等比例四舍五入，保证每镜不短于 1 秒
        value = max(1, round(durations[index] * target / total))
        scaled.append(value)
        accumulated += value
    last = target - accumulated
    if last < 1:
        # 末镜被挤没：退化为均分，确保每镜 ≥1 且总和精确等于 target
        base, remainder = divmod(target, n)
        return [base + (1 if index < remainder else 0) for index in range(n)]
    scaled.append(last)
    return scaled


def _normalize_group_timecodes(group: dict, shots: list[dict]) -> None:
    """按模型给出的单镜时长从 0 连续平铺时码，并把组时长收敛到 3~15 秒。

    时间码与组时长是可由服务端确定性派生的元数据，模型常在算术上漂移：
    相邻镜头首尾未对齐、组时长越界（>15s 或 <3s）、平铺总时长与声明不一致。
    这些都属于可确定修正的错误，不应交给 LLM 反复重试并最终卡住生成。

    规则：
    - 单镜时长非正时取 1 秒下限，保证每镜 end > start；
    - 目标组时长优先取模型声明值，其次平铺总时长，越界则夹到 3 或 15；
    - 目标不得少于镜头数（每镜至少 1 秒），多于 15 秒时夹到 15；
    - 把单镜时长等比例缩放到目标，确保末镜 end_seconds == duration_seconds。
    仅当所有 start/end 均为整数时才介入；非整数类型错误留给严格校验处理。
    """
    if not all(
        isinstance(shot, dict)
        and _is_int(shot.get("start_seconds"))
        and _is_int(shot.get("end_seconds"))
        for shot in shots
    ):
        return
    n = len(shots)
    if n < 1:
        return
    durations: list[int] = []
    for shot in shots:
        duration = shot["end_seconds"] - shot["start_seconds"]
        if duration < 1:
            duration = 1  # 单镜时长非正，给 1 秒下限再参与平铺
        durations.append(duration)
    tiled_total = sum(durations)
    declared = group.get("duration_seconds")
    if _is_int(declared) and 3 <= declared <= 15:
        preferred = declared
    elif 3 <= tiled_total <= 15:
        preferred = tiled_total
    elif _is_int(declared) and declared > 15:
        preferred = 15
    elif _is_int(declared) and 0 <= declared < 3:
        preferred = 3
    elif tiled_total > 15:
        preferred = 15
    else:
        preferred = 3
    # 每镜至少 1 秒，故组时长不得少于镜头数；镜头数超过 15 时任何合法组时长
    # 都无法满足每镜 ≥1 秒，交给严格校验/修复流程处理（通常先被 max_length 拦截）。
    if n > 15:
        return
    target = min(15, max(preferred, n))
    if target != tiled_total:
        scaled = _try_scale_durations(durations, target)
        if scaled is None:
            # 等比例四舍五入仍破坏下限：均分兜底，确保每镜 ≥1 且总和精确
            base, remainder = divmod(target, n)
            scaled = [base + (1 if index < remainder else 0) for index in range(n)]
        durations = scaled
    cursor = 0
    for index, shot in enumerate(shots):
        shot["start_seconds"] = cursor
        shot["end_seconds"] = cursor + durations[index]
        cursor = shot["end_seconds"]
    group["duration_seconds"] = target


# 文本字段长度上限（与 ShortText/LongText 一致）。供应商 schema 剥除
# maxLength，模型无长度信号；超长时按字符截断，避免反复卡住生成。
_SHORT_TEXT_MAX = 255
_LONG_TEXT_MAX = 1200

# 受控词表的合法集合；模型偶出自造词，非法值在这里收敛到中性默认，
# 不阻断生成（视觉描述文本仍承载创作内容）。
_DEFAULT_SHOT_SIZE = "中景"
_DEFAULT_CAMERA_ANGLE = "平视"
_DEFAULT_CAMERA_MOVEMENT = "固定"
_DEFAULT_COMPOSITION = "三分法"
_SHOT_SIZE_SET = frozenset(SHOT_SIZE_VALUES)
_COMPOSITION_SET = frozenset(COMPOSITION_VALUES)
_CAMERA_ANGLE_SET = frozenset(CAMERA_ANGLE_VALUES)
_CAMERA_MOVEMENT_SET = frozenset(CAMERA_MOVEMENT_VALUES)

# 镜头级字段与视觉字段白名单，用于归位模型错放的字段（镜头字段误入 visual 或反之）。
_VISUAL_FIELDS = frozenset(
    {
        "shot_size",
        "composition",
        "camera_angle",
        "camera_movement",
        "action",
        "frame_description",
        "lighting",
        "emotion",
    }
)
_SHOT_ONLY_FIELDS = frozenset(
    {
        "display_no",
        "start_seconds",
        "end_seconds",
        "narrative_purpose",
        "character_refs",
        "prop_refs",
        "sound_refs",
        "sound_design",
        "dialogue",
        "source_block_refs",
        "start_boundary",
        "end_boundary",
        "character_positions",
        "audience_visibility",
    }
)


def _coerce_enum(value: object, allowed: frozenset, default: str) -> str:
    """枚举值收敛：合法则保留，否则取中性默认。"""
    if isinstance(value, str) and value in allowed:
        return value
    return default


def _truncate_text(value: object, limit: int) -> str | None:
    """把字符串裁剪到上限；非字符串原样返回交给类型校验处理。"""
    if not isinstance(value, str):
        return value  # type: ignore[return-value]
    if len(value) <= limit:
        return value
    return value[:limit]


def _build_name_to_ref(
    subject_reference_names: dict[str, str],
) -> dict[str, str]:
    """构造「无歧义主体名 -> 短引用」映射；同名多引用不参与映射。

    v2 用短引用而非名称，正是为了规避跨类型同名歧义。模型偶尔回退用名称，
    此处仅对全局唯一的名称做反向映射，歧义名称仍交给严格校验/修复流程。
    """
    counts: dict[str, int] = {}
    for name in subject_reference_names.values():
        counts[name] = counts.get(name, 0) + 1
    return {
        name: ref
        for ref, name in subject_reference_names.items()
        if counts[name] == 1
    }


def _normalize_ref_value(
    value: object,
    *,
    name_to_ref: dict[str, str],
    valid_refs: set[str],
) -> object:
    """把单个引用值收敛为合法短引用：原值合法则保留，否则尝试名称->引用。"""
    if isinstance(value, str):
        if value in valid_refs:
            return value
        mapped = name_to_ref.get(value)
        if mapped is not None:
            return mapped
    return value


def _resolve_background_ref(
    value: object,
    *,
    name_to_ref: dict[str, str],
    background_refs: list[str],
) -> str | None:
    """强解析 background_ref：合法背景引用则保留，名称则映射，否则兜底首个背景引用。

    background_ref 是必填且必须为 background 类型；模型常输出 null/中文名/非法引用
    或拼错 BG 编号，导致 string_pattern_mismatch 或 reference_not_allowed。此处保证
    返回一个合法背景引用（无任何背景主体时返回 None，交给校验处理——理论上
    generate_storyboard 已在目录无背景时补建默认场景）。
    """
    if not background_refs:
        return None
    if isinstance(value, str):
        if value in background_refs:
            return value
        mapped = name_to_ref.get(value)
        if isinstance(mapped, str) and mapped in background_refs:
            return mapped
    return background_refs[0]


def _normalize_ref_list(
    items: object,
    *,
    name_to_ref: dict[str, str],
    valid_refs: set[str],
    subject_reference_types: dict[str, str],
    expected_type: str,
) -> list[str]:
    """归一化引用列表：映射名称、按字段期望类型过滤、去重、丢弃仍非法的引用。"""
    if not isinstance(items, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = _normalize_ref_value(item, name_to_ref=name_to_ref, valid_refs=valid_refs)
        if (
            isinstance(normalized, str)
            and normalized in valid_refs
            and subject_reference_types.get(normalized) == expected_type
            and normalized not in seen
        ):
            result.append(normalized)
            seen.add(normalized)
    return result


def _normalize_shot_fields(
    shot: dict,
    *,
    name_to_ref: dict[str, str],
    valid_refs: set[str],
    subject_reference_types: dict[str, str],
) -> None:
    """归位错放字段、去重构图、截断超长文本、收敛主体引用与对白可见性。

    只对已存在的字段操作（v2 形态）；v1 形态镜头没有 visual / character_refs
    等字段，全部跳过，避免向 v1 字典注入 v2 字段触发 extra="forbid"。
    """
    visual = shot.get("visual")
    if not isinstance(visual, dict):
        # visual 整体缺失但视觉字段散落在 shot 层 -> 重建 visual
        stray_visual = [k for k in shot if k in _VISUAL_FIELDS]
        if stray_visual:
            visual = {}
            shot["visual"] = visual
            for key in stray_visual:
                visual[key] = shot.pop(key)
    if isinstance(visual, dict):
        # 视觉字段误放在 shot 层 -> 移入 visual（shot 层重复则丢弃 shot 上的）
        for key in list(shot.keys()):
            if key in _VISUAL_FIELDS:
                if key not in visual:
                    visual[key] = shot[key]
                del shot[key]
        # 镜头级字段误放在 visual -> 移回 shot（shot 已有则丢弃 visual 里的）
        for key in list(visual.keys()):
            if key in _SHOT_ONLY_FIELDS:
                if key not in shot:
                    shot[key] = visual[key]
                del visual[key]
        # 枚举收敛：shot_size/camera_angle/camera_movement 非法则补中性默认
        if "shot_size" in visual:
            visual["shot_size"] = _coerce_enum(
                visual["shot_size"], _SHOT_SIZE_SET, _DEFAULT_SHOT_SIZE
            )
        if "camera_angle" in visual:
            visual["camera_angle"] = _coerce_enum(
                visual["camera_angle"], _CAMERA_ANGLE_SET, _DEFAULT_CAMERA_ANGLE
            )
        if "camera_movement" in visual:
            visual["camera_movement"] = _coerce_enum(
                visual["camera_movement"], _CAMERA_MOVEMENT_SET, _DEFAULT_CAMERA_MOVEMENT
            )
        # 构图：过滤非法枚举、去重、截断到 3；全部非法时补默认
        composition = visual.get("composition")
        if isinstance(composition, list):
            deduped: list[str] = []
            seen_comp: set[str] = set()
            for item in composition:
                if (
                    isinstance(item, str)
                    and item in _COMPOSITION_SET
                    and item not in seen_comp
                ):
                    deduped.append(item)
                    seen_comp.add(item)
                    if len(deduped) >= 3:
                        break
            if not deduped:
                deduped = [_DEFAULT_COMPOSITION]
            visual["composition"] = deduped
        for key, limit in (
            ("action", _LONG_TEXT_MAX),
            ("frame_description", _LONG_TEXT_MAX),
            ("lighting", _SHORT_TEXT_MAX),
            ("emotion", _SHORT_TEXT_MAX),
        ):
            if key in visual:
                visual[key] = _truncate_text(visual[key], limit)
    # 镜头级文本与引用（仅 v2 形态存在这些字段）
    if "narrative_purpose" in shot:
        shot["narrative_purpose"] = _truncate_text(
            shot["narrative_purpose"], _SHORT_TEXT_MAX
        )
    if "sound_design" in shot:
        shot["sound_design"] = _truncate_text(
            shot["sound_design"], _SHORT_TEXT_MAX
        )
    if "character_refs" in shot:
        shot["character_refs"] = _normalize_ref_list(
            shot.get("character_refs"),
            name_to_ref=name_to_ref,
            valid_refs=valid_refs,
            subject_reference_types=subject_reference_types,
            expected_type="character",
        )
    if "prop_refs" in shot:
        shot["prop_refs"] = _normalize_ref_list(
            shot.get("prop_refs"),
            name_to_ref=name_to_ref,
            valid_refs=valid_refs,
            subject_reference_types=subject_reference_types,
            expected_type="prop",
        )
    if "sound_refs" in shot:
        shot["sound_refs"] = _normalize_ref_list(
            shot.get("sound_refs"),
            name_to_ref=name_to_ref,
            valid_refs=valid_refs,
            subject_reference_types=subject_reference_types,
            expected_type="sound",
        )
    dialogue = shot.get("dialogue")
    if isinstance(dialogue, dict):
        if dialogue.get("text") is not None:
            dialogue["text"] = _truncate_text(dialogue["text"], _LONG_TEXT_MAX)
        kind = dialogue.get("kind")
        speaker = dialogue.get("speaker_ref")
        if speaker is not None:
            speaker = _normalize_ref_value(
                speaker, name_to_ref=name_to_ref, valid_refs=valid_refs
            )
            dialogue["speaker_ref"] = speaker
        # dialogue/inner_monologue 要求 speaker 是当前镜头可见角色；模型常漏列或写错，
        # 这里确定性收敛避免 model_validator(dialogue_speaker_must_be_visible) 抛 value_error。
        if kind in {"dialogue", "inner_monologue"}:
            is_character_speaker = (
                isinstance(speaker, str)
                and subject_reference_types.get(speaker) == "character"
            )
            if is_character_speaker:
                # 确保可见：补入 character_refs
                if (
                    "character_refs" in shot
                    and speaker not in shot["character_refs"]
                ):
                    shot["character_refs"].append(speaker)
            else:
                # speaker 缺失或非角色类型：优先从 character_refs 回填一个角色
                crefs = shot.get("character_refs")
                crefs = crefs if isinstance(crefs, list) else []
                fallback = next(
                    (
                        ref
                        for ref in crefs
                        if subject_reference_types.get(ref) == "character"
                    ),
                    None,
                )
                if fallback is not None:
                    dialogue["speaker_ref"] = fallback
                else:
                    # 无可回填角色：降级为 voice_over（允许 None speaker），保留台词文本
                    dialogue["kind"] = "voice_over"
                    # voice_over 允许角色/声音或 None；若当前 speaker 非合法引用则置 None
                    if not (
                        isinstance(speaker, str)
                        and speaker in valid_refs
                        and subject_reference_types.get(speaker)
                        in {"character", "sound"}
                    ):
                        dialogue["speaker_ref"] = None
        else:
            # voice_over：speaker 可为角色/声音或 None；非法引用置 None，
            # 避免 require_reference 类型不匹配抛裸 ValueError 退化为 <root> 诊断。
            if speaker is not None:
                sp_type = (
                    subject_reference_types.get(speaker)
                    if isinstance(speaker, str)
                    else None
                )
                if sp_type not in {"character", "sound"}:
                    dialogue["speaker_ref"] = None


def _normalize_provider_continuity(
    payload: dict,
    *,
    subject_reference_names: dict[str, str],
    subject_reference_types: dict[str, str],
) -> dict:
    """在严格契约校验前，按数组顺序重建编号、时码、引用与字段结构。

    这些都是可由服务端确定性派生/收敛的元数据，与 description / shot_line 同类。
    模型常在算术与结构上漂移：相邻镜头首尾未对齐、组时长越界、主体名当短引用、
    字段错放（如 narrative_purpose 进 visual）、构图超 3 项或重复、文本超长、
    schema_version 拼错。交给 LLM 反复重试既慢又不稳定，最终卡住生成。

    原则：只做可确定的收敛，不掩盖真正的类型/枚举错误——
    - 编号/时码/组时长：按数组顺序重编、从 0 平铺、组时长夹到 3~15；
    - 引用：名称->短引用（仅无歧义名）、去重、丢弃仍非法的可选引用；
    - 构图：去重截断到 3；文本：截断到上限；字段：错放回迁。
    非整数时码、非法枚举（shot_size 等）、必填 background_ref 无法解析等仍交给
    严格校验与修复流程。
    """
    scenes = payload.get("scenes") if isinstance(payload, dict) else None
    if not isinstance(scenes, list):
        return payload
    name_to_ref = _build_name_to_ref(subject_reference_names)
    valid_refs = set(subject_reference_types)
    # v2 形态的背景引用候选（按目录顺序，首个作为兜底）
    background_refs = [
        ref for ref, t in subject_reference_types.items() if t == "background"
    ]
    # 形态是 v2（含 background_ref）但 schema_version 拼错时强制为 storyboard.v2
    is_v2_payload = isinstance(payload, dict) and (
        payload.get("schema_version") == "storyboard.v2"
        or any(
            isinstance(scene, dict) and "background_ref" in scene
            for scene in scenes
        )
    )
    if isinstance(payload, dict) and is_v2_payload:
        payload["schema_version"] = "storyboard.v2"
    for scene_index, scene in enumerate(scenes, start=1):
        if not isinstance(scene, dict):
            continue
        scene["scene_no"] = scene_index
        # background_ref 必填且必须是 background 类型；模型常输出 null/名称/非法引用，
        # 这里强解析：合法则保留、名称则映射、否则兜底为目录首个背景引用，绝不留给校验卡死。
        if is_v2_payload or "background_ref" in scene:
            scene["background_ref"] = _resolve_background_ref(
                scene.get("background_ref"),
                name_to_ref=name_to_ref,
                background_refs=background_refs,
            )
        groups = scene.get("shot_groups")
        if not isinstance(groups, list):
            continue
        for group_index, group in enumerate(groups, start=1):
            if not isinstance(group, dict):
                continue
            group["group_no"] = f"G{group_index}"
            if "narrative_purpose" in group:
                group["narrative_purpose"] = _truncate_text(
                    group["narrative_purpose"], _SHORT_TEXT_MAX
                )
            shots = group.get("shots")
            if not isinstance(shots, list) or not shots:
                continue
            # 统一按数组顺序重编 display_no，并归一化镜头字段
            for shot_index, shot in enumerate(shots, start=1):
                if isinstance(shot, dict):
                    shot["display_no"] = shot_index
                    _normalize_shot_fields(
                        shot,
                        name_to_ref=name_to_ref,
                        valid_refs=valid_refs,
                        subject_reference_types=subject_reference_types,
                    )
            _normalize_group_timecodes(group, shots)
    return payload


def _validate_provider_payload_result(
    payload: dict,
    *,
    subject_catalog: dict[str, str],
    subject_reference_names: dict[str, str],
    subject_reference_types: dict[str, str],
    description_template: PromptTemplateSnapshot | None = None,
    shot_template: PromptTemplateSnapshot | None = None,
) -> _ValidatedStoryboard:
    """按版本验证 Provider 响应，并保留 v2 的稳定引用事实。"""
    _normalize_provider_continuity(
        payload,
        subject_reference_names=subject_reference_names,
        subject_reference_types=subject_reference_types,
    )
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
                description_template=description_template,
                shot_template=shot_template,
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
    description_template: PromptTemplateSnapshot | None = None,
    shot_template: PromptTemplateSnapshot | None = None,
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
                description_template=description_template,
                shot_template=shot_template,
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
            # payload 过大无法内联修复时，回退为用原始 prompt 重新生成一次，
            # 而不是直接放弃——避免长分集因单点违规被永久卡死。
            call_system = system
            call_user = user
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
    source_storyboard_job_id: str | None = None,
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
    # 兜底：若该分集无场景/背景主体，background_ref 无合法引用可用，模型只能自造或写中文名，
    # 必然触发 string_pattern_mismatch / reference_not_allowed。补建一个默认场景主体（type=scene），
    # 让生成不因缺背景而卡死；用户可事后在主体管理替换为真实场景。
    if not any(item.subject_type == "background" for item in subject_references.values()):
        fallback_subject = Subject(
            id=new_id(),
            script_id=script_id,
            type="scene",
            name=f"默认场景-第{episode.episode_no}集",
            description="自动补建的默认场景主体（剧本缺少场景描写时兜底，可替换）",
            episode_nos=[episode.episode_no],
            is_active=True,
        )
        db.add(fallback_subject)
        await db.flush()
        subjects.append(fallback_subject)
        subject_references = _build_subject_reference_catalog(subjects, episode.episode_no)
        logger.warning(
            "分镜生成兜底：分集缺少场景主体，已自动补建默认场景 script_id=%s episode_id=%s",
            script_id, episode_id,
        )
    resolved_input = await _resolve_storyboard_input(
        db,
        script_id=script_id,
        episode=episode,
        subjects=subjects,
        subject_references=subject_references,
    )
    prompt_context = resolved_input.prompts
    system_prompt = prompt_context.system_prompt
    user_prompt = prompt_context.user_prompt
    script_blocks = resolved_input.script_blocks
    provenance = resolved_input.provenance
    provenance_sha256 = storyboard_provenance_sha256(provenance)
    template_metadata = _prompt_template_metadata(prompt_context.templates)

    catalog = _subject_catalog(subjects, episode.episode_no)

    # 4. 调用 LLM；首答失败时最多进行一次不含原剧本的定向修复。
    validated = await _call_llm_with_contract_result(
        system_prompt,
        user_prompt,
        catalog,
        subject_references=subject_references,
        description_template=prompt_context.templates[_STORYBOARD_DESCRIPTION_TEMPLATE_KEY],
        shot_template=prompt_context.templates[_STORYBOARD_SHOT_TEMPLATE_KEY],
    )

    # 4.5 覆盖度软校验：若镜头数远低于剧本规模，用原 prompt（含剧本）+ 覆盖强调
    # 重生成一次。best-effort：重生成后仍不足则取镜头较多者接受，绝不因覆盖度硬失败。
    coverage = _check_coverage(validated, episode.script_text or "")
    if not coverage.sufficient:
        logger.warning(
            "分镜覆盖度不足 shot=%d/%d group=%d/%d dur=%d/%d，重新生成",
            coverage.shot_count, coverage.min_shots,
            coverage.group_count, coverage.min_groups,
            coverage.total_duration, coverage.min_duration,
        )
        emphasis_user = user_prompt + _coverage_emphasis_suffix(coverage)
        try:
            retry_validated = await _call_llm_with_contract_result(
                system_prompt,
                emphasis_user,
                catalog,
                subject_references=subject_references,
                description_template=prompt_context.templates[
                    _STORYBOARD_DESCRIPTION_TEMPLATE_KEY
                ],
                shot_template=prompt_context.templates[_STORYBOARD_SHOT_TEMPLATE_KEY],
            )
        except ProviderContractError:
            logger.warning("覆盖度重生成结构校验失败，回退首答 best-effort")
            retry_validated = None
        if retry_validated is not None:
            retry_coverage = _check_coverage(retry_validated, episode.script_text or "")
            if retry_coverage.sufficient or retry_coverage.shot_count > coverage.shot_count:
                validated = retry_validated
                coverage = retry_coverage
            if not retry_coverage.sufficient:
                logger.warning(
                    "覆盖度重生成后仍不足 shot=%d/%d，best-effort 接受",
                    retry_coverage.shot_count, retry_coverage.min_shots,
                )

    quality_report = (
        evaluate_storyboard_quality(
            validated.v2_contract,
            expected_source_blocks=script_blocks,
        )
        if validated.v2_contract is not None
        else None
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
        continuation_flags = (
            _strict_continuation_flags(source_scene) if source_scene is not None else []
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

            group_quality_summary = (
                build_shot_group_quality_summary(
                    source_group,
                    warnings=_warnings_for_group(
                        quality_report,
                        scene_index=scene_index,
                        group_index=group_index,
                    ),
                )
                if source_group is not None and quality_report is not None
                else {}
            )
            raw_director_commands = group_quality_summary.get("director_commands")
            group_prompt = _build_group_prompt(
                shot_lines=shot_lines,
                referenced_names=all_referenced,
                narrative_purpose=narrative_purpose,
                director_commands=(
                    [
                        str(item.get("command"))
                        for item in raw_director_commands
                        if isinstance(item, dict) and item.get("command")
                    ]
                    if isinstance(raw_director_commands, list)
                    else None
                ),
                group_template=prompt_context.templates[_STORYBOARD_GROUP_TEMPLATE_KEY],
                constraint_lock=prompt_context.constraint_lock,
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

            reference_frames = _build_boundary_reference_frames(
                source_group,
                subject_names_by_ref={
                    ref: item.name for ref, item in subject_references.items()
                },
            )
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
                reference_frames=reference_frames,
                duration_seconds=duration_seconds,
                video_duration_seconds=duration_seconds,
                materials=materials,
                audio_elements={
                    "characters": [item for item in audio_elements if item.get("type") == "character"],
                    "sounds": [item for item in audio_elements if item.get("type") == "sound"],
                },
                description_text=narrative_purpose,
                quality_summary=group_quality_summary,
                prompt_context={
                    "schema": "shot_group.prompt-context.v1",
                    "source_storyboard_job_id": source_storyboard_job_id,
                    "storyboard_provenance_sha256": provenance_sha256,
                    "prompt_templates": template_metadata,
                },
                is_continuation=(
                    continuation_flags[group_index]
                    if group_index < len(continuation_flags)
                    else False
                ),
                unit_id=unit_id,
                unit_runtime_key=unit_runtime_key,
                source_storyboard_job_id=source_storyboard_job_id,
            )
            db.add(shot_group)
            total_groups += 1

            # 创建 Shot
            for shot_index, sd in enumerate(shots_data):
                persisted_source_shot = (
                    source_group.shots[shot_index]
                    if source_group is not None
                    else None
                )
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
                    structured_data=(
                        build_shot_structured_data(
                            persisted_source_shot,
                            subject_ids_by_ref={
                                ref: item.subject_id
                                for ref, item in subject_references.items()
                            },
                            background_ref=(
                                source_scene.background_ref if source_scene is not None else None
                            ),
                        )
                        if persisted_source_shot is not None
                        else None
                    ),
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

    result: dict[str, object] = {
        "script_id": script_id,
        "episode_id": episode_id,
        "scenes": total_scenes,
        "shot_groups": total_groups,
        "shots": total_shots,
    }
    if quality_report is not None:
        result["quality_report"] = quality_report.as_dict()
    if source_storyboard_job_id is not None:
        result["input_provenance"] = provenance
    return result


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


def _chain_frame_location(group: ShotGroup) -> dict[str, object]:
    """返回不含主键、路径或素材键的最小镜头组位置。"""

    raw_group_index = getattr(group, "group_index", None)
    group_index = (
        raw_group_index
        if isinstance(raw_group_index, int) and raw_group_index >= 1
        else None
    )
    raw_group_no = str(getattr(group, "group_no", "") or "").strip()
    group_no = raw_group_no or (
        f"G{group_index}" if group_index is not None else "unknown"
    )
    return {
        "scene_no": int(group.scene_no),
        "group_no": group_no,
        "group_index": group_index,
    }


def _chain_frame_diagnostic(
    code: str,
    group: ShotGroup,
    *,
    predecessor: ShotGroup | None = None,
) -> dict[str, object]:
    """构造可持久化的脱敏诊断，不暴露素材键和内部资源 ID。"""

    diagnostic: dict[str, object] = {
        "code": code,
        "severity": "warning",
        "location": _chain_frame_location(group),
    }
    if predecessor is not None:
        diagnostic["predecessor_location"] = _chain_frame_location(predecessor)
    return diagnostic


def _valid_chain_frame(value: object) -> str | None:
    """只接受非空字符串帧键；历史异常 JSON 不参与链式传递。"""

    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _preserved_chain_diagnostics(frames: Mapping[str, object]) -> list[object]:
    """移除本服务拥有的旧诊断，同时保留未来版本写入的未知诊断。"""

    raw = frames.get("chain_diagnostics")
    if not isinstance(raw, list):
        return []
    preserved: list[object] = []
    for item in raw:
        if isinstance(item, Mapping) and item.get("code") in _OWNED_CHAIN_DIAGNOSTIC_CODES:
            continue
        preserved.append(item)
    return preserved


def _is_direct_chain_predecessor(
    predecessor: ShotGroup | None,
    current: ShotGroup,
) -> bool:
    """仅同场景、相邻 group_index 才构成可回填的直接前驱。"""

    if predecessor is None or int(predecessor.scene_no) != int(current.scene_no):
        return False
    previous_index = getattr(predecessor, "group_index", None)
    current_index = getattr(current, "group_index", None)
    return (
        isinstance(previous_index, int)
        and isinstance(current_index, int)
        and previous_index >= 1
        and current_index == previous_index + 1
    )


async def update_chain_frames(
    db: AsyncSession, episode_id: str
) -> ChainFrameUpdateResult:
    """首尾帧链式衔接钩子 (跨模块, 供 T8 视频生成 worker 调用)。

    按 scene_no/group_no 顺序遍历该分集所有镜头组, 将上一组 reference_frames.last_frame
    回填为下一组 reference_frames.first_frame, 实现相邻镜头组视觉连续 (04§1.15 / 02§5.4.3 / T8.5)。

    本钩子仅做衔接回填, 实际帧内容由 T8 视频生成 worker 在每组生成完成后写入 last_frame。
    返回值仍是 ``int`` 子类以兼容旧调用，并通过 ``diagnostics`` 暴露本次发现的
    脱敏结构化问题；同一诊断也写入对应组的 ``reference_frames.chain_diagnostics``。

    跨模块依赖标注: 链式衔接的具体调用时序由 T8 worker 在每组视频生成成功后触发。
    """
    groups_result = await db.execute(
        select(ShotGroup)
        .where(ShotGroup.episode_id == episode_id)
        .order_by(ShotGroup.scene_no, ShotGroup.group_index, ShotGroup.group_no)
    )
    groups = groups_result.scalars().all()
    updated = 0
    dirty = False
    diagnostics: list[dict[str, object]] = []
    previous: ShotGroup | None = None
    for group in groups:
        original_frames = group.reference_frames
        frames: dict[str, object] = (
            dict(original_frames) if isinstance(original_frames, Mapping) else {}
        )
        group_diagnostics: list[dict[str, object]] = []
        current_first_frame = _valid_chain_frame(frames.get("first_frame"))
        current_last_frame = _valid_chain_frame(frames.get("last_frame"))
        direct_predecessor = (
            previous if _is_direct_chain_predecessor(previous, group) else None
        )
        predecessor_last_frame = (
            _valid_chain_frame((direct_predecessor.reference_frames or {}).get("last_frame"))
            if direct_predecessor is not None
            and isinstance(direct_predecessor.reference_frames, Mapping)
            else None
        )

        if current_last_frame is None:
            group_diagnostics.append(
                _chain_frame_diagnostic(_CHAIN_LAST_FRAME_MISSING, group)
            )

        has_prior_group_in_scene = (
            previous is not None and int(previous.scene_no) == int(group.scene_no)
        )
        chain_source_unavailable = (
            has_prior_group_in_scene
            and (direct_predecessor is None or predecessor_last_frame is None)
        )
        if chain_source_unavailable and current_first_frame is None:
            group_diagnostics.append(
                _chain_frame_diagnostic(
                    _CHAIN_LINK_BROKEN,
                    group,
                    predecessor=previous,
                )
            )

        is_continuation = bool(getattr(group, "is_continuation", False))
        if is_continuation and (
            direct_predecessor is None or predecessor_last_frame is None
        ):
            group_diagnostics.append(
                _chain_frame_diagnostic(
                    _CONTINUATION_PREDECESSOR_LAST_FRAME_UNAVAILABLE,
                    group,
                    predecessor=previous if has_prior_group_in_scene else None,
                )
            )

        if predecessor_last_frame is not None and current_first_frame is None:
            frames["first_frame"] = predecessor_last_frame
            updated += 1

        preserved_diagnostics = _preserved_chain_diagnostics(frames)
        merged_diagnostics = [*preserved_diagnostics, *group_diagnostics]
        if merged_diagnostics:
            frames["chain_diagnostics"] = merged_diagnostics
        else:
            frames.pop("chain_diagnostics", None)

        if frames != original_frames:
            group.reference_frames = frames
            dirty = True
        diagnostics.extend(group_diagnostics)
        for diagnostic in group_diagnostics:
            logger.warning(
                "镜头组链帧诊断 code=%s severity=%s location=%s predecessor_location=%s",
                diagnostic["code"],
                diagnostic["severity"],
                diagnostic["location"],
                diagnostic.get("predecessor_location"),
            )
        previous = group

    if dirty:
        await db.flush()
    return ChainFrameUpdateResult(updated, diagnostics)


__all__ = [
    "generate_storyboard",
    "update_chain_frames",
    "ChainFrameUpdateResult",
    "CONSTRAINT_LOCK",
    "DESCRIPTION_TEMPLATE",
]
