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
from app.providers.image_gen import ImageResult
from app.providers.registry import ProviderConfigurationError

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 2.0
_POLL_TIMEOUT = 300  # 5 分钟
_RETRYABLE_SUBMIT_STATUSES = {429}
_MAX_SUBMIT_ATTEMPTS = 5
_MAX_RETRY_DELAY_SECONDS = 20.0
_STYLE_RENDER_CONSTRAINT_MARKER = "【风格渲染约束】"

# aspect -> 通义万相 size（宽*高，星号分隔）
_ASPECT_TO_SIZE: dict[str, str] = {
    "16:9": "1280*720",
    "9:16": "720*1280",
    "1:1": "1024*1024",
    "4:3": "1024*768",
    "3:4": "768*1024",
    "3:2": "1536*1024",
    "2:3": "1024*1536",
}


def _map_size(size: str, aspect: str) -> str:
    """按 aspect 选取通义万相 size；aspect 缺失时默认 16:9 横版。"""
    if aspect and aspect in _ASPECT_TO_SIZE:
        return _ASPECT_TO_SIZE[aspect]
    return _ASPECT_TO_SIZE["16:9"]


def _uses_sync_multimodal_api(model: str) -> bool:
    """Qwen Image 2.0 仅支持同步多模态生成接口。"""
    return model.strip().lower().startswith("qwen-image-2.0")


def _should_extend_prompt(prompt: str) -> bool:
    """已完成结构化风格合成的 prompt 禁止供应商再次改写。"""
    return _STYLE_RENDER_CONSTRAINT_MARKER not in prompt


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
    ) -> list[ImageResult]:
        api_key = (
            settings.image_gen_api_key
            if settings.is_explicitly_configured("image_gen_api_key") and settings.image_gen_api_key
            else settings.dashscope_api_key or settings.image_gen_api_key
        )
        if not api_key:
            raise ProviderConfigurationError(
                "未配置 IMAGE_GEN_API_KEY（或 DASHSCOPE_API_KEY），无法调用通义万相文生图"
            )
        use_model = (
            settings.image_gen_model
            if (
                settings.is_explicitly_configured("image_gen_provider")
                or settings.is_explicitly_configured("image_gen_model")
            )
            and settings.image_gen_model
            else model or "wan2.1-t2i-plus"
        )
        base_url = (
            settings.image_gen_base_url
            if settings.is_explicitly_configured("image_gen_base_url") and settings.image_gen_base_url
            else settings.dashscope_base_url
        ).rstrip("/")
        wanx_size = _map_size(size, aspect)
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
        if normalized_negative_prompt := (negative_prompt or "").strip():
            parameters["negative_prompt"] = normalized_negative_prompt
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
        if normalized_negative_prompt := (negative_prompt or "").strip():
            input_payload["negative_prompt"] = normalized_negative_prompt
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
                raise RuntimeError(f"通义万相轮询失败 status={resp.status_code}")
            data = resp.json().get("output") or {}
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
