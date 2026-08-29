"""NeoLink 文生图 Provider。

NeoLink 的图片接口兼容 OpenAI ``images.generate``，但 gp-im-2 始终通过
``b64_json`` 返回图片数据。Provider 只接受该内联结果，避免 URL 回退引入 SSRF 风险。
"""

from __future__ import annotations

import base64
import binascii
from typing import cast
from urllib.parse import urlsplit, urlunsplit

from openai import (
    AsyncOpenAI,
    BadRequestError,
    ContentFilterFinishReasonError,
    NotFoundError,
    UnprocessableEntityError,
)

from app.core.config import settings
from app.providers.image_gen import (
    ImageResult,
    ImageSafetyCategory,
    ImageSafetyRejectedError,
    classify_image_safety_code,
    image_gen_model_override_applies_to,
    image_gen_provider_is_selected,
    prompt_with_negative_constraints,
)
from app.providers.registry import ProviderConfigurationError

_SUPPORTED_ASPECTS = ("16:9", "9:16", "1:1", "4:3", "3:4", "3:2", "2:3")
_GP_IM_2_SIZE_BY_TIER: dict[str, dict[str, str]] = {
    "1k": {
        "16:9": "1792x1008",
        "9:16": "1008x1792",
        "1:1": "1024x1024",
        "4:3": "1344x1008",
        "3:4": "1008x1344",
        "3:2": "1536x1024",
        "2:3": "1024x1536",
    },
    "2k": {
        "16:9": "2816x1584",
        "9:16": "1584x2816",
        "1:1": "1536x1536",
        "4:3": "2048x1536",
        "3:4": "1536x2048",
        "3:2": "2304x1536",
        "2:3": "1536x2304",
    },
    "4k": {
        "16:9": "3840x2160",
        "9:16": "2160x3840",
        "1:1": "2160x2160",
        "4:3": "2880x2160",
        "3:4": "2160x2880",
        "3:2": "3216x2144",
        "2:3": "2144x3216",
    },
}


def _normalize_neolink_image_base_url(value: str) -> str:
    """把 NeoLink 文档 /api 基址规范化为 OpenAI SDK 所需的 /api/v1。

    与视频侧保持同一套安全约束，但不跨模块复用函数，避免图片路径耦合视频实现。
    """
    if not isinstance(value, str):
        raise ProviderConfigurationError("NEOLINK_BASE_URL 格式无效")
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except ValueError as exc:
        raise ProviderConfigurationError("NEOLINK_BASE_URL 格式无效") from exc

    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ProviderConfigurationError(
            "NEOLINK_BASE_URL 必须是不含用户信息、查询或片段的 HTTPS 443 地址"
        )

    path = parsed.path.rstrip("/")
    if path == "/api":
        path = "/api/v1"
    elif path != "/api/v1":
        raise ProviderConfigurationError("NEOLINK_BASE_URL 必须以 /api 或 /api/v1 结尾")
    return urlunsplit(("https", parsed.netloc, path, "", ""))


def _normalize_size_tier(size: str) -> str:
    """规范化产品层尺寸档位。"""
    normalized = (size or "").strip().lower()
    if normalized not in _GP_IM_2_SIZE_BY_TIER:
        raise ProviderConfigurationError("NeoLink 图片尺寸仅支持 1K、2K 或 4K")
    return normalized


def _normalize_aspect(aspect: str) -> str:
    """规范化比例；auto 保守回退为横版。"""
    normalized = (aspect or "").strip().lower()
    if normalized == "auto":
        return "16:9"
    if normalized not in _SUPPORTED_ASPECTS:
        supported = "、".join(_SUPPORTED_ASPECTS)
        raise ProviderConfigurationError(f"NeoLink 图片不支持比例 {normalized}；支持：{supported}")
    return normalized


def _validate_gp_im_2_size(size: str) -> None:
    """校验 gp-im-2 的请求尺寸仍处于 NeoLink 已声明的安全范围。"""
    try:
        width_text, height_text = size.split("x", 1)
        width = int(width_text)
        height = int(height_text)
    except (TypeError, ValueError) as exc:
        raise ProviderConfigurationError("NeoLink 图片尺寸映射无效") from exc

    if (
        width <= 0
        or height <= 0
        or width % 16 != 0
        or height % 16 != 0
        or width > height * 3
        or height > width * 3
    ):
        raise ProviderConfigurationError("NeoLink 图片尺寸不满足 gp-im-2 基本约束（16 倍数、1:3~3:1）")
    # 上限校验：长边不超 3840，短边不超 2160（对应文档 3840x2160）
    if max(width, height) > 3840 or min(width, height) > 2160:
        raise ProviderConfigurationError("NeoLink 图片尺寸超出 gp-im-2 已声明上限（3840x2160）")


def _map_size(model: str, size: str, aspect: str) -> str:
    """按模型、尺寸档位和画幅比例映射为 NeoLink 的实际 ``WIDTHxHEIGHT``。"""
    normalized_model = (model or "").strip().lower()
    normalized_size = _normalize_size_tier(size)
    normalized_aspect = _normalize_aspect(aspect)

    if normalized_model.startswith("gpt-image"):
        if normalized_size != "1k":
            raise ProviderConfigurationError(
                f"{model or 'GPT image'} 仅支持 1K 尺寸，不能使用 {normalized_size.upper()}"
            )
        if normalized_aspect == "1:1":
            return "1024x1024"
        width_text, height_text = normalized_aspect.split(":", 1)
        return "1536x1024" if int(width_text) > int(height_text) else "1024x1536"

    if normalized_model.startswith("dall-e"):
        raise ProviderConfigurationError(
            f"{model or 'DALL-E'} 的 NeoLink 图片尺寸能力未声明，无法安全调用"
        )

    try:
        mapped_size = _GP_IM_2_SIZE_BY_TIER[normalized_size][normalized_aspect]
    except KeyError as exc:
        # 比例已在规范化阶段校验；保留防御性分支，避免未来表项变更静默出错。
        raise ProviderConfigurationError("NeoLink 图片尺寸映射未声明") from exc
    _validate_gp_im_2_size(mapped_size)
    return mapped_size


def _error_details(exc: BadRequestError | UnprocessableEntityError) -> dict[str, object]:
    """从 OpenAI SDK 异常中提取 NeoLink 结构化错误对象。"""
    body = cast(dict[str, object], exc.body) if isinstance(exc.body, dict) else {}
    raw_error = body.get("error")
    return cast(dict[str, object], raw_error) if isinstance(raw_error, dict) else body


def _model_not_found_error(model: str) -> ProviderConfigurationError:
    """生成渠道未开通时给出可由运维直接定位的配置错误。"""
    return ProviderConfigurationError(f"{model} 当前 NeoLink API Key 分组未开通该图片模型渠道")


class NeolinkImageGenProvider:
    """通过 NeoLink OpenAI 兼容图片接口生成图片。"""

    async def generate(
        self,
        prompt: str,
        model: str,
        size: str,
        aspect: str,
        count: int = 1,
        negative_prompt: str | None = None,
        explicit_model: bool = False,
    ) -> list[ImageResult]:
        api_key = (
            settings.image_gen_api_key
            if image_gen_provider_is_selected("neolink", configuration=settings)
            and settings.is_explicitly_configured("image_gen_api_key")
            and settings.image_gen_api_key
            else settings.neolink_api_key
        )
        if not api_key.strip():
            raise ProviderConfigurationError(
                "未配置 IMAGE_GEN_API_KEY（或 NEOLINK_API_KEY），无法调用 NeoLink 文生图"
            )

        configured_model = settings.image_gen_model.strip()
        # 请求级显式模型不受 IMAGE_GEN_MODEL 的 env 覆盖影响。
        use_model = (
            configured_model
            if not explicit_model
            and image_gen_model_override_applies_to("neolink", configuration=settings)
            and configured_model
            else (model or "").strip() or "gp-im-2"
        )
        configured_base_url = (
            settings.image_gen_base_url
            if image_gen_provider_is_selected("neolink", configuration=settings)
            and settings.is_explicitly_configured("image_gen_base_url")
            and settings.image_gen_base_url
            else settings.neolink_base_url
        )
        base_url = _normalize_neolink_image_base_url(configured_base_url)
        neolink_size = _map_size(use_model, size, aspect)
        effective_prompt = prompt_with_negative_constraints(prompt, negative_prompt)

        client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        try:
            try:
                resp = await client.images.generate(
                    model=use_model,
                    prompt=effective_prompt,
                    size=neolink_size,
                    n=max(count, 1),
                )
            except ContentFilterFinishReasonError:
                raise ImageSafetyRejectedError(
                    provider="neolink",
                    category=ImageSafetyCategory.SAFETY,
                    code="content_filter",
                ) from None
            except NotFoundError:
                raise _model_not_found_error(use_model) from None
            except (BadRequestError, UnprocessableEntityError) as exc:
                # 优先判定 404 状态码，避免误判为安全拒绝
                if getattr(exc, "status_code", None) == 404:
                    raise _model_not_found_error(use_model) from None

                error = _error_details(exc)
                error_code = error.get("code")
                error_type = error.get("type")
                if (
                    isinstance(error_code, str)
                    and error_code.strip().lower() == "model_not_found"
                ) or (
                    isinstance(error_type, str)
                    and error_type.strip().lower() == "model_not_found"
                ):
                    raise _model_not_found_error(use_model) from None

                classification = classify_image_safety_code(error_code)
                if classification is None:
                    classification = classify_image_safety_code(error_type)
                if classification is None:
                    raise
                category, code = classification
                raise ImageSafetyRejectedError(
                    provider="neolink",
                    category=category,
                    code=code,
                ) from None
        finally:
            await client.close()

        image_data = None if resp is None else resp.data
        if not image_data:
            raise RuntimeError("NeoLink 未返回任何图片数据")

        results: list[ImageResult] = []
        for item in image_data:
            b64_json = getattr(item, "b64_json", None)
            if b64_json:
                try:
                    data = base64.b64decode(b64_json, validate=True)
                except (TypeError, ValueError, binascii.Error):
                    raise RuntimeError("NeoLink 返回的图片 Base64 数据无效") from None
                if not data:
                    raise RuntimeError("NeoLink 返回的图片 Base64 数据无效")
                results.append(ImageResult(data=data, mime_type="image/png"))
                continue
            if getattr(item, "url", None):
                raise RuntimeError("NeoLink 图片 URL 回退已禁用")
            raise RuntimeError("NeoLink 图片返回内容中未找到图片数据 (b64_json/url)")

        if not results:
            raise RuntimeError("NeoLink 未返回任何图片数据")
        return results
