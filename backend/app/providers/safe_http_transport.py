"""Provider 下载专用的 DNS 锁定 HTTP 传输层。"""

from __future__ import annotations

import ipaddress
import logging
import socket
import threading
from collections.abc import Awaitable, Callable, Iterable, Sequence

import anyio
import httpcore
import httpx

HostResolver = Callable[[str, int], Awaitable[Sequence[str]]]

_SUPPORTED_HTTPX_VERSION = "0.28.1"
_SUPPORTED_HTTPCORE_VERSION = "1.0.9"
_HTTPX_REQUEST_LOG_MESSAGE = 'HTTP Request: %s %s "%s %d %s"'
_HTTPX_LOG_FILTER_MARKER = "_safe_download_query_redaction_filter"
_HTTPX_LOG_FILTER_LOCK = threading.Lock()


class _HttpxRequestQueryRedactionFilter(logging.Filter):
    """移除 HTTPX 请求 INFO 日志中的完整查询串，保留主机、路径和响应状态。"""

    _safe_download_query_redaction_filter = True

    def filter(self, record: logging.LogRecord) -> bool:
        if (
            record.name != "httpx"
            or record.msg != _HTTPX_REQUEST_LOG_MESSAGE
            or not isinstance(record.args, tuple)
            or len(record.args) != 5
        ):
            return True

        args = list(record.args)
        url = args[1]
        if isinstance(url, httpx.URL):
            args[1] = str(url.copy_with(query=None))
        else:
            # 受支持版本应始终传入 httpx.URL；异常结构按 fail closed 处理。
            args[1] = "<redacted-url>"
        record.args = tuple(args)
        return True


def _install_httpx_request_log_filter() -> None:
    """以进程级幂等方式安装 HTTPX 查询串脱敏过滤器。"""
    logger = logging.getLogger("httpx")
    with _HTTPX_LOG_FILTER_LOCK:
        if any(getattr(item, _HTTPX_LOG_FILTER_MARKER, False) for item in logger.filters):
            return
        logger.addFilter(_HttpxRequestQueryRedactionFilter())


async def _resolve_host(host: str, port: int) -> Sequence[str]:
    """通过 AnyIO 在线程池/事件循环适配层执行一次系统 DNS 解析。"""
    try:
        infos = await anyio.getaddrinfo(
            host,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise httpcore.ConnectError("download_host_resolution_failed") from exc
    return [str(info[4][0]) for info in infos]


def require_public_ip(value: str) -> str:
    """返回规范化公网 IP；任何非全局地址都按 SSRF 目标拒绝。"""
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError("video_url_not_allowed") from exc
    if (
        not address.is_global
        or address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        raise ValueError("video_url_not_allowed")
    return address.compressed


class SafeAsyncNetworkBackend(httpcore.AsyncNetworkBackend):
    """解析、校验并直连 IP，消除校验与连接之间的 DNS 竞态。"""

    def __init__(
        self,
        *,
        resolver: HostResolver | None = None,
        network_backend: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        self._resolver = resolver or _resolve_host
        self._network_backend = network_backend or httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        deadline = None if timeout is None else anyio.current_time() + timeout
        try:
            literal = ipaddress.ip_address(host)
        except ValueError:
            try:
                remaining = self._remaining_timeout(
                    deadline,
                    error_code="download_host_resolution_timed_out",
                )
                if remaining is None:
                    resolved = await self._resolver(host, port)
                else:
                    try:
                        with anyio.fail_after(remaining):
                            resolved = await self._resolver(host, port)
                    except TimeoutError as exc:
                        raise httpcore.ConnectTimeout(
                            "download_host_resolution_timed_out"
                        ) from exc
            except OSError as exc:
                raise httpcore.ConnectError("download_host_resolution_failed") from exc
        else:
            resolved = [literal.compressed]

        # 必须在发起任何连接前校验全集，防止“公网 + 私网”混合答案回退到内网。
        candidates = list(dict.fromkeys(require_public_ip(value) for value in resolved))
        if not candidates:
            raise ValueError("video_url_not_allowed")

        last_error: httpcore.ConnectError | httpcore.ConnectTimeout | None = None
        for candidate in candidates:
            try:
                remaining = self._remaining_timeout(
                    deadline,
                    error_code="download_host_connection_timed_out",
                )
                if remaining is None:
                    return await self._network_backend.connect_tcp(
                        candidate,
                        port,
                        timeout=None,
                        local_address=local_address,
                        socket_options=socket_options,
                    )
                try:
                    with anyio.fail_after(remaining):
                        return await self._network_backend.connect_tcp(
                            candidate,
                            port,
                            timeout=remaining,
                            local_address=local_address,
                            socket_options=socket_options,
                        )
                except TimeoutError as exc:
                    raise httpcore.ConnectTimeout(
                        "download_host_connection_timed_out"
                    ) from exc
            except (httpcore.ConnectError, httpcore.ConnectTimeout) as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    @staticmethod
    def _remaining_timeout(deadline: float | None, *, error_code: str) -> float | None:
        if deadline is None:
            return None
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            raise httpcore.ConnectTimeout(error_code)
        return remaining

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise ValueError("video_url_not_allowed")

    async def sleep(self, seconds: float) -> None:
        await self._network_backend.sleep(seconds)


def build_safe_async_http_transport(
    *,
    resolver: HostResolver | None = None,
    network_backend: httpcore.AsyncNetworkBackend | None = None,
) -> httpx.AsyncHTTPTransport:
    """构造禁用代理、启用 TLS 校验并锁定 DNS 结果的 HTTPX transport。"""
    _install_httpx_request_log_filter()
    if (
        httpx.__version__ != _SUPPORTED_HTTPX_VERSION
        or httpcore.__version__ != _SUPPORTED_HTTPCORE_VERSION
    ):
        raise RuntimeError("safe_http_transport_incompatible")

    transport = httpx.AsyncHTTPTransport(verify=True, trust_env=False, retries=0)
    # HTTPX 0.28 尚未公开 network_backend 注入参数；这里集中约束唯一的私有兼容点。
    # 结构或受支持版本变化时必须 fail closed，避免升级后静默退回二次 DNS 解析。
    pool = getattr(transport, "_pool", None)
    if not isinstance(pool, httpcore.AsyncConnectionPool) or not hasattr(pool, "_network_backend"):
        raise RuntimeError("safe_http_transport_incompatible")
    pool._network_backend = SafeAsyncNetworkBackend(  # type: ignore[attr-defined]
        resolver=resolver,
        network_backend=network_backend,
    )
    return transport


_install_httpx_request_log_filter()
