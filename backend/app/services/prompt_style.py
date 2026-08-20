"""提示词风格目录与图像、视频提示词优化服务。"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter
from collections.abc import Sequence
from typing import TypedDict

from app.providers import LLMProvider, get_chat_llm_provider
from app.schemas.materials import SUBJECT_DESIGN_PROMPT_MARKER

logger = logging.getLogger(__name__)


class PromptOptimizeResult(TypedDict):
    optimized_prompt: str
    negative_prompt: str
    style_id: str | None
    style_name: str | None
    original_prompt: str


PromptOptimizeBatchItem = tuple[str, str | None, str]
PROMPT_OPTIMIZE_BATCH_CONCURRENCY = 4


STYLE_CATALOG: list[dict] = [
    {
        "id": "cinematic",
        "name_zh": "电影感",
        "target": ["image", "video"],
        "prompt_template": (
            "cinematic narrative frame, {prompt}, dramatic motivated lighting, layered composition, "
            "shallow depth of field, subtle film grain, 35mm lens character, restrained color grading"
        ),
        "negative_prompt": (
            "cartoon rendering, flat lighting, oversaturated colors, low detail, blurry, "
            "distorted anatomy, watermark, text"
        ),
        "default_prompt": (
            "a story-driven cinematic frame with clear subject separation and dramatic motivated lighting"
        ),
        "best_for": ["剧情叙事", "人物特写", "预告片"],
    },
    {
        "id": "photoreal",
        "name_zh": "写实摄影",
        "target": ["image", "video"],
        "prompt_template": (
            "photorealistic editorial photograph, {prompt}, natural skin and material texture, "
            "physically accurate lighting, balanced exposure, realistic depth of field, high micro-detail"
        ),
        "negative_prompt": (
            "illustration, CGI, plastic skin, excessive retouching, surreal proportions, "
            "low resolution, blur, compression artifacts"
        ),
        "default_prompt": (
            "a natural editorial photograph with accurate materials, balanced light, and realistic depth"
        ),
        "best_for": ["人物肖像", "产品展示", "纪实场景"],
    },
    {
        "id": "anime",
        "name_zh": "日式动漫",
        "target": ["image", "video"],
        "prompt_template": (
            "high-quality Japanese anime key visual, {prompt}, expressive character design, "
            "clean linework, controlled cel shading, cinematic color script, detailed background"
        ),
        "negative_prompt": (
            "photorealistic, live action, muddy colors, rough sketch, broken linework, "
            "malformed hands, low detail, watermark"
        ),
        "default_prompt": (
            "an expressive anime key visual with clean linework, cel shading, and a detailed setting"
        ),
        "best_for": ["青春故事", "幻想冒险", "角色设定"],
    },
    {
        "id": "guo-feng",
        "name_zh": "国风水墨",
        "target": ["image", "video"],
        "prompt_template": (
            "contemporary Chinese ink-wash composition, {prompt}, expressive brush textures, "
            "layered ink diffusion, elegant negative space, mineral color accents, poetic atmospheric depth"
        ),
        "negative_prompt": (
            "neon palette, plastic texture, hard digital edges, crowded composition, western comic style, "
            "low detail, watermark"
        ),
        "default_prompt": (
            "a poetic Chinese ink-wash scene with expressive brushwork, layered mist, and elegant negative space"
        ),
        "best_for": ["古装剧情", "山水意境", "传统文化"],
    },
    {
        "id": "pixar-3d",
        "name_zh": "3D皮克斯",
        "target": ["image", "video"],
        "prompt_template": (
            "polished stylized 3D animated film frame, {prompt}, appealing rounded forms, "
            "expressive facial animation, tactile materials, soft global illumination, vibrant production design"
        ),
        "negative_prompt": (
            "live action, flat 2D drawing, uncanny face, rigid pose, waxy material, noisy render, "
            "low-poly geometry, watermark"
        ),
        "default_prompt": (
            "a warm stylized 3D animation frame with expressive characters and tactile materials"
        ),
        "best_for": ["家庭故事", "儿童内容", "轻喜剧"],
    },
    {
        "id": "cyberpunk",
        "name_zh": "赛博朋克",
        "target": ["image", "video"],
        "prompt_template": (
            "dense cyberpunk world, {prompt}, rain-slick surfaces, practical neon signage, "
            "holographic interfaces, industrial texture, high-contrast rim lighting, atmospheric haze"
        ),
        "negative_prompt": (
            "pastoral daylight, historical costume, clean empty background, flat lighting, "
            "washed-out colors, low detail, blurry, watermark"
        ),
        "default_prompt": (
            "a rain-soaked futuristic city scene with neon reflections, industrial detail, and atmospheric haze"
        ),
        "best_for": ["科幻都市", "动作场面", "未来悬疑"],
    },
    {
        "id": "watercolor",
        "name_zh": "水彩",
        "target": ["image"],
        "prompt_template": (
            "expressive watercolor illustration, {prompt}, translucent pigment washes, "
            "soft wet-on-wet edges, visible cold-press paper grain, selective detail, luminous color layering"
        ),
        "negative_prompt": (
            "photorealistic, 3D render, vector art, heavy black outlines, plastic texture, "
            "muddy pigment, digital noise, watermark"
        ),
        "default_prompt": (
            "a luminous watercolor illustration with translucent washes, soft edges, and visible paper texture"
        ),
        "best_for": ["绘本", "自然景观", "温柔叙事"],
    },
    {
        "id": "comic-book",
        "name_zh": "美漫",
        "target": ["image"],
        "prompt_template": (
            "unmistakable 2D American comic-book illustration, {prompt}, bold confident ink contours, "
            "controlled cross-hatching and halftone texture, flat graphic spot-color planes, "
            "dramatic foreshortening, energetic panel composition, never photorealistic or live-action"
        ),
        "negative_prompt": (
            "photorealistic, live action, camera photograph, realistic skin pores, soft watercolor, "
            "anime proportions, weak linework, flat staging, muddy colors, illegible lettering, watermark"
        ),
        "default_prompt": (
            "an unmistakable 2D American comic-book illustration with bold inks, halftone texture, "
            "dramatic perspective, and controlled spot colors"
        ),
        "best_for": ["英雄动作", "冲突场面", "分镜概念"],
    },
]

_SUBJECT_DESIGN_STYLE_CORES = {
    "cinematic": (
        "cinematic visual-development rendering, restrained color grading, subtle film grain, "
        "evenly readable materials"
    ),
    "photoreal": (
        "photorealistic high-detail rendering, natural skin and material texture, "
        "balanced exposure, high micro-detail"
    ),
    "anime": (
        "high-quality Japanese anime rendering, clean linework, controlled cel shading, "
        "consistent production-design detail"
    ),
    "guo-feng": (
        "contemporary Chinese ink-wash rendering, expressive brush textures, layered ink diffusion, "
        "restrained mineral-color accents"
    ),
    "pixar-3d": (
        "polished stylized 3D animation rendering, appealing forms, tactile materials, "
        "soft global illumination"
    ),
    "cyberpunk": (
        "unmistakable cyberpunk visual-development rendering, dominant cyan-magenta neon color language, "
        "vivid emissive edge lighting, high-contrast luminous accents, futuristic industrial graphic "
        "treatment, atmospheric electronic glow; the cyberpunk aesthetic must remain visually dominant, "
        "not a mostly photorealistic image with token neon accents"
    ),
    "watercolor": (
        "expressive watercolor rendering, translucent pigment washes, visible cold-press paper grain, "
        "selective high-detail rendering"
    ),
    "comic-book": (
        "unmistakable 2D American comic-book illustration, bold confident ink contours, "
        "controlled cross-hatching and halftone texture, flat graphic spot-color planes, "
        "graphic cel-style shadow shapes; the comic-book illustration treatment must remain visually "
        "dominant across every view and must never become photorealistic or live-action"
    ),
}

_SUBJECT_DESIGN_PRESENTATION_DIRECTIONS = {
    "character": (
        "character model sheet with consistent front, standard side, and back views, "
        "readable pose, clean studio background, consistent turnaround rendering"
    ),
    "scene": (
        "full-frame environment concept scene with coherent spatial depth, architectural readability, "
        "and unified environmental lighting"
    ),
    "prop": (
        "product-design reference sheet with consistent multi-view object rendering, isolated product "
        "readability, and grounded physical support"
    ),
}

_SUBJECT_DESIGN_FACT_LOCKS = {
    "character": (
        "The selected rendering medium and visual style are mandatory and override any conflicting medium "
        "or style wording anywhere in this prompt; preserve every required view, pose, composition, clean "
        "background, physical relationship, and all identity, period, clothing, and prop facts."
    ),
    "scene": (
        "The selected rendering medium and visual style are mandatory and override any conflicting medium "
        "or style wording anywhere in this prompt; preserve the full-frame composition, spatial layout, "
        "architecture, materials, location, period, time, weather, and all scene facts."
    ),
    "prop": (
        "The selected rendering medium and visual style are mandatory and override any conflicting medium "
        "or style wording anywhere in this prompt; preserve every required view, product-sheet composition, "
        "physical support, scale, construction, materials, period, and all prop facts."
    ),
}

# 这些词描述场景内容而非渲染媒介，会直接否定主体事实或设定图白底构图。
_SUBJECT_DESIGN_NEGATIVE_CONFLICTS = {
    "pastoral daylight",
    "historical costume",
    "clean empty background",
}

_STYLE_RENDER_CONSTRAINT_MARKER = "【风格渲染约束】"
_STYLE_PRIORITY_CONSTRAINT_MARKER = "【风格优先级】"
_STYLE_RENDER_CONSTRAINT_PATTERN = re.compile(
    rf"{re.escape(_STYLE_RENDER_CONSTRAINT_MARKER)}.*?(?=【[^】\r\n]+】|\Z)",
    re.DOTALL,
)
_STYLE_PRIORITY_CONSTRAINT_PATTERN = re.compile(
    rf"{re.escape(_STYLE_PRIORITY_CONSTRAINT_MARKER)}.*?(?=【[^】\r\n]+】|\Z)",
    re.DOTALL,
)


_IMAGE_OPTIMIZE_SYSTEM = """你是一名专业视觉导演和图像提示词设计师。请把用户输入扩展为可直接用于图像生成的高质量 prompt。
补充主体的稳定外观特征、环境与构图、镜头景别、光线方向与质感、色彩和视觉风格；视觉风格必须服从用户选择的目标风格，不得自行加入冲突的摄影、插画或渲染语汇；不得改变用户明确指定的核心主体、关系或情节。
输出必须是一段 80-150 个英文单词的 prompt。输入中形如 {{var}} 的占位符必须逐字保留，不得翻译、改写、删除或拆分。输入中形如 @数字 的主体引用标记必须逐字保留，不得翻译、改写或删除。
输出格式约束：禁止解释，禁止使用 Markdown 代码块，禁止添加标题或 Prompt: 等前缀，只输出优化后的 prompt 文本。"""

_VIDEO_OPTIMIZE_SYSTEM = """你是一名专业视频导演和视频生成提示词设计师。请把用户输入扩展为可直接用于视频生成的高质量 prompt。
补充主体特征、连续动作、场景反馈、镜头景别、运镜方式、光影与时间变化；重点强化运动信息和运镜，使用简单、直接、可执行的动词，避免互相冲突的动作。
若为图生视频，只描述从当前画面开始的动作和运镜，不要描述转场。输出控制在 80-120 字。输入中形如 {{var}} 的占位符必须逐字保留，不得翻译、改写、删除或拆分。输入中形如 @数字 的主体引用标记必须逐字保留，不得翻译、改写或删除。
输出格式约束：禁止解释，禁止使用 Markdown 代码块，禁止添加标题或 Prompt: 等前缀，只输出优化后的 prompt 文本。"""

_PLACEHOLDER_PATTERN = re.compile(r"\{\{[^{}\r\n]+\}\}")
_META_PREFIX_PATTERN = re.compile(
    r"^(?:prompt|optimized\s+prompt|optimized_prompt|优化后的提示词|优化提示词|提示词)\s*[:：]",
    re.IGNORECASE,
)


def _validate_provider_prompt(content: str, raw_prompt: str) -> str:
    """校验供应商纯文本契约，并确保模板变量不丢失、不重复。"""
    candidate = content.strip()
    if not candidate or "```" in candidate or _META_PREFIX_PATTERN.match(candidate):
        raise ValueError("LLM 返回内容不符合纯文本输出契约")
    if Counter(_PLACEHOLDER_PATTERN.findall(candidate)) != Counter(
        _PLACEHOLDER_PATTERN.findall(raw_prompt)
    ):
        raise ValueError("LLM 返回内容未保持模板变量")
    return candidate


def get_styles(target: str | None = None) -> list[dict]:
    """返回风格目录；指定目标时仅返回支持该目标的风格。"""
    if target is None:
        return [style.copy() for style in STYLE_CATALOG]
    return [style.copy() for style in STYLE_CATALOG if target in style["target"]]


def find_style(style_id: str) -> dict | None:
    """按 ID 查找风格。"""
    return next((style for style in STYLE_CATALOG if style["id"] == style_id), None)


def _unwrap_catalog_style_template(prompt: str, style: dict) -> str | None:
    """按目录模板固定边界精确解包，避免通过风格关键词误删用户事实。"""
    template = str(style["prompt_template"])
    if "{prompt}" not in template:
        suffix = f", {template}"
        if not prompt.endswith(suffix):
            return None
        inner = prompt[: -len(suffix)].strip()
        return inner or None
    prefix, suffix = template.split("{prompt}", 1)
    if not prefix and not suffix:
        return None
    if not prompt.startswith(prefix) or not prompt.endswith(suffix):
        return None
    end = len(prompt) - len(suffix) if suffix else len(prompt)
    inner = prompt[len(prefix) : end].strip()
    return inner or None


def strip_image_style(prompt: str) -> str:
    """移除已知图片风格包装与结构化风格段，保留主体事实和后续硬约束。"""
    normalized = prompt.strip()
    for _ in STYLE_CATALOG:
        matches = [
            inner
            for style in get_styles("image")
            if (inner := _unwrap_catalog_style_template(normalized, style)) is not None
        ]
        if len(matches) != 1:
            break
        normalized = matches[0]
    normalized = _STYLE_PRIORITY_CONSTRAINT_PATTERN.sub("", normalized)
    normalized = _STYLE_RENDER_CONSTRAINT_PATTERN.sub("", normalized)
    return re.sub(r"\n{3,}", "\n\n", normalized).strip()


def _resolve_subject_design_type(subject_type: str | None, prompt: str) -> str:
    """规范化视觉资产类型；旧调用缺少类型时从结构化段落推断。"""
    normalized = (subject_type or "").strip().lower()
    if normalized == "background":
        return "scene"
    if normalized in _SUBJECT_DESIGN_PRESENTATION_DIRECTIONS:
        return normalized
    if "【1 空间身份锚定】" in prompt or "主体类型：场景" in prompt:
        return "scene"
    if "【1 形制与尺度锚定】" in prompt or "主体类型：道具" in prompt:
        return "prop"
    return "character"


def _subject_design_style_core(style: dict) -> str:
    """返回结构化资产可用的风格核心，并兼容目录后续新增风格。"""
    style_id = str(style["id"])
    if direction := _SUBJECT_DESIGN_STYLE_CORES.get(style_id):
        return direction
    return str(style["prompt_template"]).replace("{prompt}", "").strip(" ,")


def apply_style(
    style_id: str,
    prompt: str,
    target: str,
    *,
    subject_type: str | None = None,
) -> tuple[str, str]:
    """把风格模板应用到提示词，返回正向与负向提示词。"""
    style = find_style(style_id)
    if style is None or target not in style["target"]:
        return prompt, ""

    if target == "image" and SUBJECT_DESIGN_PROMPT_MARKER in prompt:
        base_prompt = strip_image_style(prompt)
        resolved_subject_type = _resolve_subject_design_type(subject_type, base_prompt)
        style_core = _subject_design_style_core(style)
        presentation = _SUBJECT_DESIGN_PRESENTATION_DIRECTIONS[resolved_subject_type]
        fact_lock = _SUBJECT_DESIGN_FACT_LOCKS[resolved_subject_type]
        priority = (
            f"{_STYLE_PRIORITY_CONSTRAINT_MARKER}所选渲染风格为最高优先级，必须主导整张图的每个视图："
            f"{style_core}. 不得被本提示词中其他相冲突的媒介或风格描述替代。"
        )
        positive = (
            f"{priority}\n\n{base_prompt}\n\n{_STYLE_RENDER_CONSTRAINT_MARKER}"
            f"{style_core}, {presentation}. {fact_lock}"
        )
        negative_parts = [
            part.strip()
            for part in str(style["negative_prompt"]).split(",")
            if part.strip().lower() not in _SUBJECT_DESIGN_NEGATIVE_CONFLICTS
        ]
        return positive, ", ".join(negative_parts)

    template = str(style["prompt_template"])
    positive = template.replace("{prompt}", prompt) if "{prompt}" in template else f"{prompt}, {template}"
    return positive, str(style["negative_prompt"])


def apply_style_definition(
    style: dict,
    prompt: str,
    target: str,
    *,
    subject_type: str | None = None,
    visual_style: str | None = None,
) -> tuple[str, str]:
    """应用内置或数据库画风定义，并把渲染媒介作为独立维度注入。

    数据库 ArtStyle 通过同一字典契约进入本函数，避免维护
    ``visual_style × style`` 的 16 份重复模板。
    """

    if target not in style.get("target", []):
        return prompt, ""
    style_id = str(style.get("id") or "")
    if find_style(style_id) is not None:
        positive, negative = apply_style(
            style_id,
            prompt,
            target,
            subject_type=subject_type,
        )
    else:
        template = str(style.get("prompt_template") or "{prompt}")
        positive = template.replace("{prompt}", prompt)
        negative = str(style.get("negative_prompt") or "")
    medium = visual_style or style.get("visual_style")
    return apply_visual_style_dimension(positive, str(medium) if medium else None), negative


def apply_visual_style_dimension(prompt: str, visual_style: str | None) -> str:
    """独立注入写实/动画媒介，不复制具体美术风格模板。"""

    medium = visual_style
    medium_prompt = {
        "live_action": "live-action physical production, real camera and practical materials",
        "anime": "animated production medium, designed linework and controlled animation shading",
    }.get(str(medium))
    if medium_prompt and medium_prompt not in prompt:
        return f"{medium_prompt}, {prompt}"
    return prompt


async def optimize_prompt(
    raw_prompt: str,
    style_id: str | None,
    target: str,
    *,
    provider: LLMProvider | None = None,
) -> PromptOptimizeResult:
    """优化原始提示词并合成可选风格；供应商异常时降级使用原文。"""
    if target not in {"image", "video"}:
        raise ValueError("target 必须为 image 或 video")

    style = find_style(style_id) if style_id else None
    if style is not None and target not in style["target"]:
        style = None

    resolved_style_id = str(style["id"]) if style is not None else None
    style_name = str(style["name_zh"]) if style is not None else None
    if target == "image" and SUBJECT_DESIGN_PROMPT_MARKER in raw_prompt:
        # 结构化资产只持久化无风格基础提示词；最终风格由创建任务时按主体类型合成。
        base_prompt = strip_image_style(raw_prompt)
        return {
            "optimized_prompt": base_prompt,
            "negative_prompt": "",
            "style_id": resolved_style_id,
            "style_name": style_name,
            "original_prompt": base_prompt,
        }

    system = _IMAGE_OPTIMIZE_SYSTEM if target == "image" else _VIDEO_OPTIMIZE_SYSTEM
    if style is not None:
        style_direction = str(style["prompt_template"]).replace("{prompt}", "主体与场景描述")
        system = (
            f"{system}\n目标风格：{style['name_zh']}。扩写内容必须符合以下视觉方向："
            f"{style_direction}。禁止加入这些冲突特征：{style['negative_prompt']}。"
        )
    user_message = (
        f"请优化以下{target}生成提示词：\n{raw_prompt}"
        if raw_prompt
        else f"请生成一段高质量{target}生成提示词"
    )
    try:
        llm = provider or get_chat_llm_provider()
        optimized_prompt = _validate_provider_prompt(
            await llm.chat(
                [{"role": "user", "content": user_message}],
                system=system,
                temperature=0.6,
                max_tokens=800,
            ),
            raw_prompt,
        )
    except Exception as exc:  # noqa: BLE001
        optimized_prompt = raw_prompt
        logger.warning(
            "提示词优化降级为原始 prompt: target=%s style_id=%s error_type=%s",
            target,
            style_id,
            getattr(exc, "error_type", type(exc).__name__),
        )

    positive, negative = apply_style(resolved_style_id or "", optimized_prompt, target)
    return {
        "optimized_prompt": positive,
        "negative_prompt": negative,
        "style_id": resolved_style_id,
        "style_name": style_name,
        "original_prompt": raw_prompt,
    }


async def optimize_prompts(
    items: Sequence[PromptOptimizeBatchItem],
    *,
    provider: LLMProvider | None = None,
    concurrency: int = PROMPT_OPTIMIZE_BATCH_CONCURRENCY,
) -> list[PromptOptimizeResult]:
    """批量优化提示词，限制供应商并发并保持输入顺序。

    单项优化继续复用 ``optimize_prompt`` 的供应商降级和风格处理逻辑；
    ``asyncio.gather`` 的结果顺序与输入一致，信号量避免一次批量请求
    对外部 LLM 造成无界并发。
    """
    if not items:
        return []
    if concurrency < 1:
        raise ValueError("concurrency 必须大于 0")

    semaphore = asyncio.Semaphore(concurrency)

    async def _optimize_one(item: PromptOptimizeBatchItem) -> PromptOptimizeResult:
        raw_prompt, style_id, target = item
        async with semaphore:
            return await optimize_prompt(
                raw_prompt,
                style_id,
                target,
                provider=provider,
            )

    return list(await asyncio.gather(*(_optimize_one(item) for item in items)))
