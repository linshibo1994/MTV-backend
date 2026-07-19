"""厂商返回 URL 的安全下载工具 - SSRF 防护 + 流式大小限制。

下载时执行 URL 语法、Provider 白名单和连接点 DNS 三层校验：
- 仅允许公网 http/https
- 拒绝私网/回环/链路本地/组播/保留/未指定 IP
- DNS 只在 connect_tcp 解析一次，并直连校验后的 IP，避免 DNS rebinding
- 手动逐跳跟随重定向并逐跳校验（不依赖 httpx follow_redirects）

safe_download_bytes 在下载前/中双重检查体积上限，防止超大响应 OOM：
- 先查 Content-Length 头快速拒绝
- 再流式逐块累计，超限立即中断连接
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Callable
from pathlib import Path

import httpx

from app.providers.safe_http_transport import build_safe_async_http_transport, require_public_ip
from app.providers.streaming_io import (
    STREAM_CHUNK_SIZE,
    CancellationCallback,
    async_chunks_to_file,
)

MAX_PROVIDER_REDIRECTS = 3
MAX_PROVIDER_VIDEO_BYTES = 512 * 1024 * 1024

_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_CROSS_ORIGIN_SENSITIVE_HEADERS = {
    "api-key",
    "apikey",
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
    "x-apikey",
    "x-goog-api-key",
}


def validate_public_http_url(url: str) -> None:
    """校验 http/https URL 语法；IP 字面量同时执行公网校验。

    域名的 DNS 结果必须在实际 connect_tcp 连接点校验，不能在这里预解析，
    否则校验与连接之间会产生 DNS rebinding 竞态。
    """
    try:
        parsed = httpx.URL(url)
    except (TypeError, ValueError, httpx.InvalidURL) as exc:
        raise ValueError("video_url_not_allowed") from exc
    host = parsed.host
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or (parsed.port is not None and not 1 <= parsed.port <= 65535)
    ):
        raise ValueError("video_url_not_allowed")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        normalized = host.rstrip(".").lower()
        if normalized == "localhost" or normalized.endswith(".localhost"):
            raise ValueError("video_url_not_allowed")
        try:
            ascii_host = normalized.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError("video_url_not_allowed") from exc
        if len(ascii_host) > 253 or any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
            for label in ascii_host.split(".")
        ):
            raise ValueError("video_url_not_allowed")
    else:
        require_public_ip(host)


async def safe_download_bytes(
    url: str,
    *,
    max_bytes: int,
    timeout: float = 120.0,
    max_redirects: int = MAX_PROVIDER_REDIRECTS,
    extra_headers: dict[str, str] | None = None,
    url_validator: Callable[[str], None] | None = None,
) -> bytes:
    """安全下载厂商返回的 URL 字节流。

    流程：逐跳 SSRF 校验 → 不跟随重定向（手动逐跳）→ Content-Length 预检 →
    流式累计字节数，超 max_bytes 立即中断。
    """
    headers = dict(extra_headers or {})
    transport = build_safe_async_http_transport()
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
        transport=transport,
        trust_env=False,
    ) as http:
        current = url
        for redirect_count in range(max_redirects + 1):
            validate_public_http_url(current)
            if url_validator is not None:
                url_validator(current)
            http.cookies.clear()
            async with http.stream("GET", current, headers=headers) as resp:
                if resp.status_code in _REDIRECT_STATUSES:
                    if redirect_count >= max_redirects:
                        raise RuntimeError("download_too_many_redirects")
                    location = resp.headers.get("location")
                    if not location:
                        raise RuntimeError("download_redirect_missing")
                    redirected = str(resp.url.join(location))
                    if _url_origin(redirected) != _url_origin(current):
                        headers = _strip_cross_origin_sensitive_headers(headers)
                    current = redirected
                    continue
                resp.raise_for_status()
                content_length = resp.headers.get("content-length")
                if content_length and int(content_length) > max_bytes:
                    raise RuntimeError("download_too_large")
                total = 0
                buf = bytearray()
                async for chunk in resp.aiter_bytes(65536):
                    total += len(chunk)
                    if total > max_bytes:
                        raise RuntimeError("download_too_large")
                    buf.extend(chunk)
                return bytes(buf)
        raise RuntimeError("download_too_many_redirects")


async def safe_download_to_file(
    url: str,
    destination: str | Path,
    *,
    max_bytes: int,
    timeout: float = 120.0,
    max_redirects: int = MAX_PROVIDER_REDIRECTS,
    extra_headers: dict[str, str] | None = None,
    url_validator: Callable[[str], None] | None = None,
    cancellation_probe: CancellationCallback | None = None,
) -> int:
    """逐跳校验并把远端响应流式写入文件，返回实际字节数。"""
    headers = dict(extra_headers or {})
    current = url
    transport = build_safe_async_http_transport()
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
        transport=transport,
        trust_env=False,
    ) as http:
        for redirect_count in range(max_redirects + 1):
            if cancellation_probe is not None:
                await cancellation_probe()
            validate_public_http_url(current)
            if url_validator is not None:
                url_validator(current)
            http.cookies.clear()
            async with http.stream("GET", current, headers=headers) as resp:
                if resp.status_code in _REDIRECT_STATUSES:
                    if redirect_count >= max_redirects:
                        raise RuntimeError("download_too_many_redirects")
                    location = resp.headers.get("location")
                    if not location:
                        raise RuntimeError("download_redirect_missing")
                    redirected = str(resp.url.join(location))
                    if _url_origin(redirected) != _url_origin(current):
                        headers = _strip_cross_origin_sensitive_headers(headers)
                    current = redirected
                    continue

                resp.raise_for_status()
                declared_size = _content_length(resp.headers.get("content-length"))
                return await async_chunks_to_file(
                    resp.aiter_bytes(STREAM_CHUNK_SIZE),
                    destination,
                    max_bytes=max_bytes,
                    declared_size=declared_size,
                    cancellation_probe=cancellation_probe,
                )
    raise RuntimeError("download_too_many_redirects")


def _url_origin(url: str) -> tuple[str, str, int]:
    parsed = httpx.URL(url)
    default_port = 443 if parsed.scheme == "https" else 80
    return parsed.scheme, parsed.host.lower(), parsed.port or default_port


def _strip_cross_origin_sensitive_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        key: value
        for key, value in headers.items()
        if not _is_cross_origin_sensitive_header(key)
    }


def _is_cross_origin_sensitive_header(name: str) -> bool:
    normalized = name.strip().lower()
    return normalized in _CROSS_ORIGIN_SENSITIVE_HEADERS or normalized.endswith("-api-key")


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError("download_size_invalid") from exc
    if parsed < 0:
        raise RuntimeError("download_size_invalid")
    return parsed
