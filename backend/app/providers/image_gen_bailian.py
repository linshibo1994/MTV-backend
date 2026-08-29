"""阿里百炼文生图 Provider。

非 OpenAI 兼容协议，按模型选择原生 DashScope REST 接口：
- qwen-image-2.0 系列走同步多模态生成接口
  POST {base_url}/api/v1/services/aigc/multimodal-generation/generation
- 提交 POST {base_url}/api/v1/services/aigc/text2image/image-synthesis
  请求头必带 X-DashScope-Async: enable（缺则报错 current user api does not support
  synchronous calls）
- 轮询 GET {base_url}/api/v1/tasks/{task_id}，状态 PENDING/RUNNING/SUCCEEDED/FAILED/UNKNOWN
- 成功后 results[].url 为 OSS 临时链接（24h 过期），立即下载为 bytes 返回
- size 格式用星号分隔（1024*1024），非 OpenAI 的 1024x1024

模型：qwen-image-2.0 / wan2.1-t2i-plus / wan2.1-t2i-turbo / wanx-v1 等。
缺 dashscope_api_key 时抛 RuntimeError，由 worker 捕获后 release 积分。
"""

from __future__ import annotations

import asyncio
import logging
import random
from urllib.parse import urlsplit

import httpx

from app.core.config import settings
from app.providers.image_gen import (
    ImageResult,
    ImageSafetyRejectedError,
    classify_image_safety_code,
    image_gen_model_override_applies_to,
    image_gen_provider_is_selected,
)
from app.providers.registry import ProviderConfigurationError

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 2.0
_POLL_TIMEOUT = 300  # 5 分钟
_RETRYABLE_SUBMIT_STATUSES = {429}
_MAX_SUBMIT_ATTEMPTS = 5
_MAX_RETRY_DELAY_SECONDS = 20.0


def _raise_if_safety_rejected(payload: object) -> None:
    """只按百炼结构化 ``code`` 字段识别审核拒绝。"""
    if not isinstance(payload, dict):
        return
    candidates = [payload.get("code")]
    for wrapper_name in ("error", "output"):
        wrapper = payload.get(wrapper_name)
        if isinstance(wrapper, dict):
            candidates.extend((wrapper.get("code"), wrapper.get("finish_reason")))
    for candidate in candidates:
        classification = classify_image_safety_code(candidate)
        if classification is None:
            continue
        category, code = classification
        raise ImageSafetyRejectedError(provider="bailian", category=category, code=code)


def _classify_response_safety(response: object) -> None:
    """响应非 JSON 时保持原错误路径，不把解析失败伪装成审核拒绝。"""
    status_code = getattr(response, "status_code", None)
    if status_code not in {200, 400, 422}:
        return
    json_method = getattr(response, "json", None)
    if not callable(json_method):
        return
    try:
        payload = json_method()
    except (TypeError, ValueError):
        return
    _raise_if_safety_rejected(payload)

# Qwen Image 2.0 支持自定义宽高；以下 2K 值采用百炼官方推荐分辨率。
# 1K 值使用该 API 的常用低分辨率档，均为 16 的倍数并保持相同宽高比。
_QWEN_IMAGE_2_SIZE_BY_TIER: dict[str, dict[str, str]] = {
    "1k": {
        "16:9": "1344*768",
        "9:16": "768*1344",
        "1:1": "1024*1024",
        "4:3": "1184*864",
        "3:4": "864*1184",
        "3:2": "1152*768",
        "2:3": "768*1152",
    },
    "2k": {
        "16:9": "2688*1536",
        "9:16": "1536*2688",
        "1:1": "2048*2048",
        "4:3": "2368*1728",
        "3:4": "1728*2368",
        "3:2": "2304*1536",
        "2:3": "1536*2304",
    },
}

# 旧 qwen-image / qwen-image-plus / qwen-image-max 只支持这五种固定预设。
_LEGACY_QWEN_PRESET_SIZE_BY_ASPECT: dict[str, str] = {
    "16:9": "1664*928",
    "9:16": "928*1664",
    "1:1": "1328*1328",
    "4:3": "1472*1104",
    "3:4": "1104*1472",
}

# 其余旧万相模型没有统一的 2K/4K 能力契约；仅保留已验证的基础尺寸映射，
# 对更高档位显式拒绝，不能再把调用方的 2K/4K 静默降级。
_LEGACY_1K_SIZE_BY_ASPECT: dict[str, str] = {
    "16:9": "1280*720",
    "9:16": "720*1280",
    "1:1": "1024*1024",
    "4:3": "1024*768",
    "3:4": "768*1024",
    "3:2": "1536*1024",
    "2:3": "1024*1536",
}


def _normalize_size_tier(size: str) -> str:
    """规范化画布尺寸档位，拒绝供应商未声明的档位。"""
    normalized = (size or "2k").strip().lower()
    if normalized not in {"1k", "2k", "4k"}:
        raise ProviderConfigurationError("百炼图片尺寸仅支持 1K、2K 或 4K")
    return normalized


def _normalize_aspect(aspect: str) -> str:
    """规范化比例；auto 会在服务层解析，Provider 直接调用时保守回退横版。"""
    normalized = (aspect or "16:9").strip().lower()
    return "16:9" if normalized == "auto" else normalized


def _map_size(model: str, size: str, aspect: str) -> str:
    """按有效模型、尺寸档位和比例选择百炼实际请求的 ``width*height``。

    ``size`` 是产品层的 1K/2K/4K 档位，不允许再被无条件忽略。对于不具备
    对应能力的模型，直接抛出可恢复的配置错误，避免用户把低清预设误认为 2K/4K。
    """
    normalized_model = (model or "").strip().lower()
    normalized_size = _normalize_size_tier(size)
    normalized_aspect = _normalize_aspect(aspect)

    if normalized_model.startswith("qwen-image-2.0"):
        if normalized_size == "4k":
            raise ProviderConfigurationError("Qwen Image 2.0 图片尺寸仅支持 1K 或 2K，不支持 4K")
        try:
            return _QWEN_IMAGE_2_SIZE_BY_TIER[normalized_size][normalized_aspect]
        except KeyError as exc:
            supported = "、".join(_QWEN_IMAGE_2_SIZE_BY_TIER[normalized_size])
            raise ProviderConfigurationError(
                f"Qwen Image 2.0 不支持比例 {normalized_aspect}；支持：{supported}"
            ) from exc

    if normalized_model.startswith(("qwen-image", "qwen-image-plus", "qwen-image-max")):
        if normalized_size != "1k":
            raise ProviderConfigurationError(
                f"{model} 仅支持固定预设尺寸；请使用 1K，或切换至 qwen-image-2.0 使用 2K"
            )
        try:
            return _LEGACY_QWEN_PRESET_SIZE_BY_ASPECT[normalized_aspect]
        except KeyError as exc:
            supported = "、".join(_LEGACY_QWEN_PRESET_SIZE_BY_ASPECT)
            raise ProviderConfigurationError(
                f"{model} 不支持比例 {normalized_aspect}；支持：{supported}"
            ) from exc

    if normalized_size != "1k":
        raise ProviderConfigurationError(
            f"{model or '当前百炼模型'} 的尺寸能力未声明；仅支持 1K，"
            "若需要 2K 请切换至 qwen-image-2.0"
        )
    try:
        return _LEGACY_1K_SIZE_BY_ASPECT[normalized_aspect]
    except KeyError as exc:
        supported = "、".join(_LEGACY_1K_SIZE_BY_ASPECT)
        raise ProviderConfigurationError(
            f"{model or '当前百炼模型'} 不支持比例 {normalized_aspect}；支持：{supported}"
        ) from exc


def _uses_sync_multimodal_api(model: str) -> bool:
    """Qwen Image 2.0 仅支持同步多模态生成接口。"""
    return model.strip().lower().startswith("qwen-image-2.0")


def _should_extend_prompt(prompt: str) -> bool:
    """客户端已确认最终提示词，禁止供应商侧再次扩展。"""
    return False


def _extract_multimodal_urls(payload: dict) -> list[str]:
    """从同步多模态响应中提取图片 URL，并兼容网关的常见包装格式。"""
    output = payload.get("output") or {}
    urls: list[str] = []
    for choice in output.get("choices") or []:
        message = choice.get("message") or {}
        for item in message.get("content") or []:
            if not isinstance(item, dict):
                continue
            candidate = item.get("image") or item.get("image_url") or item.get("url")
            if isinstance(candidate, dict):
                candidate = candidate.get("url")
            if isinstance(candidate, str) and candidate:
                urls.append(candidate)
    return urls


def _retry_delay_seconds(response: httpx.Response, attempt: int) -> float:
    """优先遵循 Retry-After，否则使用带抖动的指数退避。"""
    headers = getattr(response, "headers", {}) or {}
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), _MAX_RETRY_DELAY_SECONDS)
        except (TypeError, ValueError):
            pass
    base = min(2 ** max(attempt - 1, 0), _MAX_RETRY_DELAY_SECONDS)
    return min(base + random.uniform(0.0, base * 0.25), _MAX_RETRY_DELAY_SECONDS)


async def _post_with_transient_retry(
    http: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    body: dict,
) -> httpx.Response:
    """仅重试明确未受理的限流响应，避免非幂等请求因 5xx 重投而重复计费。"""
    response: httpx.Response | None = None
    for attempt in range(1, _MAX_SUBMIT_ATTEMPTS + 1):
        response = await http.post(url, headers=headers, json=body)
        if response.status_code not in _RETRYABLE_SUBMIT_STATUSES:
            return response
        if attempt >= _MAX_SUBMIT_ATTEMPTS:
            return response
        delay = _retry_delay_seconds(response, attempt)
        logger.warning(
            "百炼生图请求被临时拒绝 status=%d attempt=%d/%d，%.2f 秒后重试",
            response.status_code,
            attempt,
            _MAX_SUBMIT_ATTEMPTS,
            delay,
        )
        await asyncio.sleep(delay)
    assert response is not None
    return response


class BailianImageGenProvider:
    """阿里百炼通义万相文生图实现（httpx 直连，零额外依赖）。"""

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
            if image_gen_provider_is_selected("bailian", configuration=settings)
            and settings.is_explicitly_configured("image_gen_api_key")
            and settings.image_gen_api_key
            else settings.dashscope_api_key
        )
        if not api_key:
            raise ProviderConfigurationError(
                "未配置 IMAGE_GEN_API_KEY（或 DASHSCOPE_API_KEY），无法调用通义万相文生图"
            )
        # 仅让当前生效厂商的显式模型覆盖画布任务模型，避免跨厂商串台；
        # 请求级显式模型（explicit_model）不受 env 覆盖影响。
        use_model = (
            settings.image_gen_model
            if not explicit_model
            and image_gen_model_override_applies_to("bailian", configuration=settings)
            and settings.image_gen_model
            else model or "wan2.1-t2i-plus"
        )
        base_url = (
            settings.image_gen_base_url
            if image_gen_provider_is_selected("bailian", configuration=settings)
            and settings.is_explicitly_configured("image_gen_base_url")
            and settings.image_gen_base_url
            else settings.dashscope_base_url
        ).rstrip("/")
        wanx_size = _map_size(use_model, size, aspect)
        logger.info(
            "百炼生图尺寸映射 model=%s requested_size=%s requested_aspect=%s effective_size=%s",
            use_model,
            size,
            aspect,
            wanx_size,
        )
        n = max(count, 1)

        async with httpx.AsyncClient(timeout=60.0) as http:
            if _uses_sync_multimodal_api(use_model):
                urls = await self._generate_sync_multimodal(
                    http,
                    base_url,
                    api_key,
                    prompt,
                    use_model,
                    wanx_size,
                    n,
                    negative_prompt,
                )
            else:
                urls = await self._generate_legacy_async(
                    http,
                    base_url,
                    api_key,
                    prompt,
                    use_model,
                    wanx_size,
                    n,
                    negative_prompt,
                )
            if len(urls) < n:
                logger.warning("百炼返回图片数 %d 少于请求 %d", len(urls), n)
            return [await self._download(url, base_url=base_url) for url in urls]

    async def _generate_sync_multimodal(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        api_key: str,
        prompt: str,
        model: str,
        size: str,
        count: int,
        negative_prompt: str | None,
    ) -> list[str]:
        """调用 Qwen Image 2.0 同步多模态生成接口。"""
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        parameters: dict[str, object] = {
            "size": size,
            "n": count,
            "prompt_extend": _should_extend_prompt(prompt),
            "watermark": False,
        }
        if negative_prompt:
            parameters["negative_prompt"] = negative_prompt
        body = {
            "model": model,
            "input": {"messages": [{"role": "user", "content": [{"text": prompt}]}]},
            "parameters": parameters,
        }
        submit = await _post_with_transient_retry(
            http,
            f"{base_url}/api/v1/services/aigc/multimodal-generation/generation",
            headers=headers,
            body=body,
        )
        _classify_response_safety(submit)
        if submit.status_code != 200:
            raise RuntimeError(f"百炼 Qwen Image 文生图失败 status={submit.status_code}")
        urls = _extract_multimodal_urls(submit.json())
        if not urls:
            raise RuntimeError("百炼 Qwen Image 文生图未返回图片 URL")
        return urls

    async def _generate_legacy_async(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        api_key: str,
        prompt: str,
        model: str,
        size: str,
        count: int,
        negative_prompt: str | None,
    ) -> list[str]:
        """调用通义万相兼容模型的异步任务接口。"""

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        }
        input_payload = {"prompt": prompt}
        if negative_prompt:
            input_payload["negative_prompt"] = negative_prompt
        body = {
            "model": model,
            "input": input_payload,
            "parameters": {
                "size": size,
                "n": count,
                "prompt_extend": _should_extend_prompt(prompt),
                "watermark": False,
            },
        }
        submit = await _post_with_transient_retry(
            http,
            f"{base_url}/api/v1/services/aigc/text2image/image-synthesis",
            headers=headers,
            body=body,
        )
        _classify_response_safety(submit)
        if submit.status_code != 200:
            raise RuntimeError(f"通义万相文生图提交失败 status={submit.status_code}")
        task_id = (submit.json().get("output") or {}).get("task_id")
        if not task_id:
            raise RuntimeError("通义万相文生图提交未返回 task_id")
        return await self._poll(http, base_url, api_key, task_id)

    async def _poll(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        api_key: str,
        task_id: str,
    ) -> list[str]:
        """轮询任务至终态，返回图片 URL 列表。"""
        poll_headers = {"Authorization": f"Bearer {api_key}"}
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        while True:
            resp = await http.get(f"{base_url}/api/v1/tasks/{task_id}", headers=poll_headers)
            if resp.status_code != 200:
                _classify_response_safety(resp)
                raise RuntimeError(f"通义万相轮询失败 status={resp.status_code}")
            payload = resp.json()
            _raise_if_safety_rejected(payload)
            data = payload.get("output") or {}
            status = (data.get("task_status") or "").upper()
            if status == "SUCCEEDED":
                results = data.get("results") or []
                return [r["url"] for r in results if r.get("url")]
            if status in ("FAILED", "UNKNOWN"):
                raise RuntimeError(f"通义万相文生图任务失败 task_id={task_id} status={status}")
            if asyncio.get_event_loop().time() > deadline:
                raise RuntimeError(f"通义万相文生图轮询超时 task_id={task_id}")
            await asyncio.sleep(_POLL_INTERVAL)

    async def _download(self, url: str, *, base_url: str) -> ImageResult:
        """下载图片 URL 字节流。"""
        from app.providers._url_safety import safe_download_bytes

        data = await safe_download_bytes(
            url,
            max_bytes=50 * 1024 * 1024,
            timeout=60.0,
            url_validator=lambda current: self._validate_download_host(current, base_url),
        )
        path = urlsplit(url).path.lower()
        mime_type = "image/jpeg" if path.endswith((".jpg", ".jpeg")) else "image/png"
        return ImageResult(data=data, mime_type=mime_type)

    @staticmethod
    def _validate_download_host(url: str, base_url: str) -> None:
        parsed = urlsplit(url)
        hostname = (parsed.hostname or "").lower()
        provider_host = (urlsplit(base_url).hostname or "").lower()
        if parsed.scheme != "https" or not (
            hostname == provider_host
            or hostname.endswith(".aliyuncs.com")
            or hostname.endswith(".aliyuncs.com.cn")
        ):
            raise RuntimeError("provider_download_url_invalid")
