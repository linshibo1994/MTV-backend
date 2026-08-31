"""阿里百炼文生图 Provider。

非 OpenAI 兼容协议，按模型选择原生 DashScope REST 接口：
- qwen-image-2.0 走同步多模态生成接口
  POST {base_url}/api/v1/services/aigc/multimodal-generation/generation
- qwen-image-3.0 系列默认走官方异步接口
  POST {base_url}/api/v1/services/aigc/image-generation/generation
  请求头必带 X-DashScope-Async: enable；同步多模态接口仅保留为显式兼容路径
- 旧通义万相模型提交 POST {base_url}/api/v1/services/aigc/text2image/image-synthesis
  并必带 X-DashScope-Async: enable
- 轮询 GET {base_url}/api/v1/tasks/{task_id}，状态 PENDING/RUNNING/SUCCEEDED/FAILED/UNKNOWN
- 旧模型成功后 output.results[].url，Qwen Image 3.0 成功后
  output.choices[].message.content[].image，均为 OSS 临时链接（24h 过期）
  并立即下载为 bytes 返回
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

# 官方 Qwen Image 3.0 模型。只有这两个精确 ID 走 3.0 契约，避免把
# 目录外的相似自定义模型误判为新接口。
_QWEN_IMAGE_3_MODELS = frozenset({"qwen-image-3.0", "qwen-image-3.0-pro"})


def _is_qwen_image_3(model: str) -> bool:
    """判断模型是否为官方 Qwen Image 3.0 标识。"""
    return (model or "").strip().lower() in _QWEN_IMAGE_3_MODELS


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

# Qwen Image 3.0 的 size 不是旧版固定预设，而是任意合法的 width*height：
# 总像素范围 512*512 到 2048*2048，比例范围 1:8 到 8:1。产品层使用
# 1K/2K 档位，因此这里选取官方常见尺寸；所有尺寸都满足接口上限。
# 3:2/2:3 与 21:9 的数值参考百炼生态中公开的 Qwen Image 预设。
_QWEN_IMAGE_3_SIZE_BY_TIER: dict[str, dict[str, str]] = {
    "1k": {
        "16:9": "1280*720",
        "9:16": "720*1280",
        "1:1": "1024*1024",
        "4:3": "1152*864",
        "3:4": "864*1152",
        "3:2": "1328*880",
        "2:3": "880*1328",
        "5:4": "1120*896",
        "4:5": "896*1120",
        "21:9": "1512*648",
    },
    "2k": {
        "16:9": "2048*1152",
        "9:16": "1152*2048",
        "1:1": "2048*2048",
        "4:3": "2048*1536",
        "3:4": "1536*2048",
        "3:2": "2048*1365",
        "2:3": "1365*2048",
        "5:4": "2048*1632",
        "4:5": "1632*2048",
        # 21:9 在 2048 长边限制下无法达到 2.25M 像素阈值，但该尺寸仍
        # 满足官方 512^2~2048^2 与 1:8~8:1 的接口约束。
        "21:9": "2048*880",
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

    if normalized_model in _QWEN_IMAGE_3_MODELS:
        if normalized_size == "4k":
            raise ProviderConfigurationError(
                f"{model} 图片尺寸仅支持 1K 或 2K，不支持 4K"
            )
        try:
            return _QWEN_IMAGE_3_SIZE_BY_TIER[normalized_size][normalized_aspect]
        except KeyError as exc:
            supported = "、".join(_QWEN_IMAGE_3_SIZE_BY_TIER[normalized_size])
            raise ProviderConfigurationError(
                f"{model} 不支持比例 {normalized_aspect}；支持：{supported}"
            ) from exc

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
    """Qwen Image 2.0 的默认路径是同步多模态生成接口。

    Qwen Image 3.0 的 HTTP 文档要求异步提交，因此不在默认分支中返回 True；
    仅当调用方显式关闭 ``use_async_for_qwen3`` 时，才使用兼容同步路径。
    """
    normalized_model = model.strip().lower()
    return normalized_model.startswith("qwen-image-2.0")


def _should_extend_prompt(prompt: str) -> bool:
    """客户端已确认最终提示词，禁止供应商侧再次扩展。"""
    return False


def _extract_multimodal_urls(payload: dict) -> list[str]:
    """从同步多模态响应中提取图片 URL，并兼容网关的常见包装格式。"""
    output = payload.get("output") or {}
    if not isinstance(output, dict):
        return []
    urls: list[str] = []
    for choice in output.get("choices") or []:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message") or {}
        if not isinstance(message, dict):
            continue
        for item in message.get("content") or []:
            if not isinstance(item, dict):
                continue
            candidate = item.get("image") or item.get("image_url") or item.get("url")
            if isinstance(candidate, dict):
                candidate = candidate.get("url")
            if isinstance(candidate, str) and candidate:
                urls.append(candidate)
    return urls


def _extract_task_urls(payload: dict) -> list[str]:
    """从异步轮询响应提取图片 URL，兼容旧万相与 Qwen Image 3.0。"""
    output = payload.get("output") or {}
    if not isinstance(output, dict):
        return []
    urls: list[str] = []

    # 旧 text2image/image-synthesis 契约：output.results[].url。
    for result in output.get("results") or []:
        if not isinstance(result, dict):
            continue
        candidate = result.get("url")
        if isinstance(candidate, str) and candidate:
            urls.append(candidate)
    if urls:
        return urls

    # Qwen Image 3.0 image-generation 契约：与同步响应相同的 choices 结构。
    return _extract_multimodal_urls(payload)


def _multimodal_request_body(
    *,
    model: str,
    prompt: str,
    size: str,
    count: int,
    negative_prompt: str | None,
) -> dict:
    """构造 Qwen Image 2/3 共用的 messages 请求体。"""
    parameters: dict[str, object] = {
        "size": size,
        "n": count,
        "prompt_extend": _should_extend_prompt(prompt),
        "watermark": False,
    }
    if negative_prompt:
        parameters["negative_prompt"] = negative_prompt
    return {
        "model": model,
        "input": {"messages": [{"role": "user", "content": [{"text": prompt}]}]},
        "parameters": parameters,
    }


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

    def __init__(self, *, use_async_for_qwen3: bool = True) -> None:
        """初始化 Provider。

        Qwen Image 3.0/3.0 Pro 默认提交到官方异步 endpoint。部分百炼账号或
        网关拒绝同步调用（会返回 ``current user api does not support
        synchronous calls``），因此异步是生产默认值；传入 False 仅用于已确认
        支持同步调用的兼容网关。旧版万相始终沿用其原生异步接口。
        """
        self.use_async_for_qwen3 = use_async_for_qwen3

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
        if _is_qwen_image_3(use_model) and n > 6:
            raise ProviderConfigurationError("Qwen Image 3.0 单次最多生成 6 张图片")

        async with httpx.AsyncClient(timeout=60.0) as http:
            if _is_qwen_image_3(use_model):
                if self.use_async_for_qwen3:
                    urls = await self._generate_qwen3_async(
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
                    # 仅供已确认支持同步调用的兼容网关使用；官方生产默认走异步。
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
            elif _uses_sync_multimodal_api(use_model):
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
        """调用 Qwen Image 2.0（或显式兼容模式下的 3.0）同步接口。"""
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        body = _multimodal_request_body(
            model=model,
            prompt=prompt,
            size=size,
            count=count,
            negative_prompt=negative_prompt,
        )
        submit = await _post_with_transient_retry(
            http,
            f"{base_url}/api/v1/services/aigc/multimodal-generation/generation",
            headers=headers,
            body=body,
        )
        _classify_response_safety(submit)
        if submit.status_code not in {200, 202}:
            raise RuntimeError(f"百炼 Qwen Image 文生图失败 status={submit.status_code}")
        urls = _extract_multimodal_urls(submit.json())
        if not urls:
            raise RuntimeError("百炼 Qwen Image 文生图未返回图片 URL")
        return urls

    async def _generate_qwen3_async(
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
        """调用 Qwen Image 3.0 官方异步接口并轮询任务结果。

        3.0 异步接口与旧通义万相接口的关键差异是 endpoint 和 input 结构：
        必须使用 ``image-generation/generation``，且请求体仍是 messages。
        """
        if not _is_qwen_image_3(model):
            raise ProviderConfigurationError(
                "Qwen Image 3.0 异步接口仅适用于 qwen-image-3.0 或 qwen-image-3.0-pro"
            )
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        }
        body = _multimodal_request_body(
            model=model,
            prompt=prompt,
            size=size,
            count=count,
            negative_prompt=negative_prompt,
        )
        submit = await _post_with_transient_retry(
            http,
            f"{base_url}/api/v1/services/aigc/image-generation/generation",
            headers=headers,
            body=body,
        )
        _classify_response_safety(submit)
        if submit.status_code not in {200, 202}:
            raise RuntimeError(f"百炼 Qwen Image 3.0 异步提交失败 status={submit.status_code}")
        task_id = (submit.json().get("output") or {}).get("task_id")
        if not task_id:
            raise RuntimeError("百炼 Qwen Image 3.0 异步提交未返回 task_id")
        return await self._poll(http, base_url, api_key, task_id)

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
            if not isinstance(data, dict):
                raise RuntimeError(f"百炼图片轮询响应格式无效 task_id={task_id}")
            status = (data.get("task_status") or "").upper()
            if status == "SUCCEEDED":
                urls = _extract_task_urls(payload)
                if not urls:
                    raise RuntimeError(
                        f"百炼图片任务成功但未返回图片 URL task_id={task_id}"
                    )
                return urls
            if status in ("FAILED", "CANCELED", "UNKNOWN"):
                code = data.get("code") or payload.get("code")
                suffix = f" code={code}" if isinstance(code, str) and code else ""
                raise RuntimeError(
                    f"通义万相文生图任务失败 task_id={task_id} status={status}{suffix}"
                )
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
