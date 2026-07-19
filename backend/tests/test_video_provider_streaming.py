"""视频 Provider 流式下载与原子存储契约测试。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import ssl
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import anyio
import httpcore
import httpx
import pytest

from app.core.config import Settings
from app.providers.video_gen import VideoGenerationRequest


def _settings(**kwargs) -> Settings:
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


def _request(
    *,
    job_id: str,
    provider_task_id: str,
    storage_attempt_id: str = "lease-token-sensitive",
    model: str = "video-model",
    first_frame: str | None = None,
) -> VideoGenerationRequest:
    return VideoGenerationRequest(
        prompt="镜头提示词",
        reference_images=[],
        reference_image_names=[],
        reference_audios=[],
        reference_audio_names=[],
        model=model,
        job_id=job_id,
        first_frame=first_frame,
        metadata={
            "provider_task_id": provider_task_id,
            "storage_attempt_id": storage_attempt_id,
        },
    )


class _AtomicFakeStorage:
    """输出路径只允许 put_file；任何 bytes 上传都立即失败。"""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.upload_paths: list[str] = []

    async def put(self, _key: str, _data: bytes) -> str:
        raise AssertionError("视频输出禁止使用 storage.put(bytes)")

    async def put_file(self, key: str, path: str) -> str:
        self.upload_paths.append(path)
        self.objects[key] = Path(path).read_bytes()
        return key

    async def promote(self, source_key: str, final_key: str) -> str:
        self.objects[final_key] = self.objects.pop(source_key)
        return final_key

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.objects.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self.objects


class _StreamResponse:
    def __init__(
        self,
        url: str,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
    ) -> None:
        self.url = httpx.URL(url)
        self.status_code = status_code
        self.headers = httpx.Headers(headers or {})
        self._chunks = chunks or []
        self.iterated = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_status_{self.status_code}")

    async def aiter_bytes(self, _chunk_size: int):
        self.iterated = True
        for chunk in self._chunks:
            yield chunk


class _StreamHttpClient:
    def __init__(self, responses: list[_StreamResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.cookies = httpx.Cookies()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def stream(self, _method: str, url: str, *, headers: dict[str, str]):
        self.calls.append((url, dict(headers)))
        return self._responses.pop(0)


def _redirect_chain(redirects: int, *, include_success: bool) -> list[_StreamResponse]:
    responses = [
        _StreamResponse(
            f"https://provider.example/hop-{index}",
            status_code=302,
            headers={"location": f"https://provider.example/hop-{index + 1}"},
        )
        for index in range(redirects)
    ]
    if include_success:
        responses.append(
            _StreamResponse(
                f"https://provider.example/hop-{redirects}",
                chunks=[b"video"],
            )
        )
    return responses


class _RecordingNetworkStream(httpcore.AsyncNetworkStream):
    def __init__(self, response: bytes = b"") -> None:
        self._response = bytearray(response)
        self.writes: list[bytes] = []
        self.tls_server_names: list[str | None] = []
        self.tls_contexts: list[ssl.SSLContext] = []
        self.closed = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        chunk = bytes(self._response[:max_bytes])
        del self._response[:max_bytes]
        return chunk

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self.writes.append(buffer)

    async def aclose(self) -> None:
        self.closed = True

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.tls_contexts.append(ssl_context)
        self.tls_server_names.append(server_hostname)
        return self


class _RecordingNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, stream: _RecordingNetworkStream | None = None) -> None:
        self.stream = stream or _RecordingNetworkStream()
        self.tcp_calls: list[tuple[str, int]] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options=None,
    ) -> httpcore.AsyncNetworkStream:
        self.tcp_calls.append((host, port))
        return self.stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options=None,
    ) -> httpcore.AsyncNetworkStream:
        raise AssertionError("安全下载不应连接 Unix socket")

    async def sleep(self, seconds: float) -> None:
        return None


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "http://localhost/video.mp4",
        "http://127.0.0.1/video.mp4",
        "http://100.64.0.1/video.mp4",
        "http://224.0.0.1/video.mp4",
        "http://[64:ff9b::7f00:1]/video.mp4",
        "http://provider.example:0/video.mp4",
        "http://provider.example:65536/video.mp4",
        "http://bad_host.example/video.mp4",
    ],
)
def test_validate_public_http_url_rejects_non_public_or_malformed_targets(url: str) -> None:
    from app.providers._url_safety import validate_public_http_url

    with pytest.raises(ValueError, match="video_url_not_allowed"):
        validate_public_http_url(url)


def test_validate_public_http_url_defers_domain_resolution_to_connect_point() -> None:
    from app.providers._url_safety import validate_public_http_url

    validate_public_http_url("https://provider.example/video.mp4")


@pytest.mark.parametrize(
    ("package_name", "version"),
    [
        ("httpx", "0.28.2"),
        ("httpcore", "1.0.10"),
        ("httpx", "0.29.0"),
    ],
)
def test_safe_transport_fails_closed_on_unverified_dependency_version(
    monkeypatch,
    package_name: str,
    version: str,
) -> None:
    from app.providers import safe_http_transport

    package = getattr(safe_http_transport, package_name)
    monkeypatch.setattr(package, "__version__", version)

    with pytest.raises(RuntimeError, match="safe_http_transport_incompatible"):
        safe_http_transport.build_safe_async_http_transport()


@pytest.mark.asyncio
async def test_safe_network_backend_connects_only_to_single_resolved_public_ip() -> None:
    from app.providers.safe_http_transport import SafeAsyncNetworkBackend

    resolutions: list[tuple[str, int]] = []

    async def resolver(host: str, port: int) -> list[str]:
        resolutions.append((host, port))
        return ["8.8.8.8"]

    network = _RecordingNetworkBackend()
    backend = SafeAsyncNetworkBackend(resolver=resolver, network_backend=network)

    assert await backend.connect_tcp("provider.example", 443) is network.stream
    assert resolutions == [("provider.example", 443)]
    assert network.tcp_calls == [("8.8.8.8", 443)]


@pytest.mark.asyncio
async def test_safe_network_backend_rejects_mixed_dns_answers_before_connecting() -> None:
    from app.providers.safe_http_transport import SafeAsyncNetworkBackend

    async def resolver(_host: str, _port: int) -> list[str]:
        return ["8.8.8.8", "127.0.0.1"]

    network = _RecordingNetworkBackend()
    backend = SafeAsyncNetworkBackend(resolver=resolver, network_backend=network)

    with pytest.raises(ValueError, match="video_url_not_allowed"):
        await backend.connect_tcp("provider.example", 443)

    assert network.tcp_calls == []


@pytest.mark.asyncio
async def test_safe_network_backend_validates_ip_literals_without_dns() -> None:
    from app.providers.safe_http_transport import SafeAsyncNetworkBackend

    async def resolver(_host: str, _port: int) -> list[str]:
        raise AssertionError("IP 字面量不应触发 DNS")

    network = _RecordingNetworkBackend()
    backend = SafeAsyncNetworkBackend(resolver=resolver, network_backend=network)

    await backend.connect_tcp("1.1.1.1", 443)
    with pytest.raises(ValueError, match="video_url_not_allowed"):
        await backend.connect_tcp("169.254.169.254", 80)

    assert network.tcp_calls == [("1.1.1.1", 443)]


@pytest.mark.asyncio
async def test_safe_network_backend_dns_timeout_cancels_resolver_without_background_task() -> None:
    from app.providers.safe_http_transport import SafeAsyncNetworkBackend

    resolver_started = anyio.Event()
    resolver_stopped = anyio.Event()

    async def resolver(_host: str, _port: int) -> list[str]:
        resolver_started.set()
        try:
            await anyio.sleep_forever()
        finally:
            resolver_stopped.set()
        return []

    network = _RecordingNetworkBackend()
    backend = SafeAsyncNetworkBackend(resolver=resolver, network_backend=network)

    with anyio.fail_after(0.25):
        with pytest.raises(httpcore.ConnectTimeout, match="download_host_resolution_timed_out"):
            await backend.connect_tcp("provider.example", 443, timeout=0.02)

    assert resolver_started.is_set()
    assert resolver_stopped.is_set()
    assert network.tcp_calls == []


@pytest.mark.asyncio
async def test_safe_network_backend_shares_one_deadline_across_candidate_ips() -> None:
    from app.providers.safe_http_transport import SafeAsyncNetworkBackend

    forwarded_timeouts: list[float | None] = []
    second_candidate_stopped = anyio.Event()

    class DeadlineNetworkBackend(_RecordingNetworkBackend):
        async def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: float | None = None,
            local_address: str | None = None,
            socket_options=None,
        ) -> httpcore.AsyncNetworkStream:
            self.tcp_calls.append((host, port))
            forwarded_timeouts.append(timeout)
            if len(self.tcp_calls) == 1:
                await anyio.sleep(0.03)
                raise httpcore.ConnectError("first_candidate_failed")
            try:
                await anyio.sleep_forever()
            finally:
                second_candidate_stopped.set()
            return self.stream

    async def resolver(_host: str, _port: int) -> list[str]:
        return ["8.8.8.8", "1.1.1.1"]

    network = DeadlineNetworkBackend()
    backend = SafeAsyncNetworkBackend(resolver=resolver, network_backend=network)
    started_at = anyio.current_time()

    with anyio.fail_after(0.25):
        with pytest.raises(httpcore.ConnectTimeout, match="download_host_connection_timed_out"):
            await backend.connect_tcp("provider.example", 443, timeout=0.06)

    elapsed = anyio.current_time() - started_at
    assert network.tcp_calls == [("8.8.8.8", 443), ("1.1.1.1", 443)]
    assert len(forwarded_timeouts) == 2
    assert forwarded_timeouts[0] is not None
    assert forwarded_timeouts[1] is not None
    assert 0 < forwarded_timeouts[1] < forwarded_timeouts[0] <= 0.06
    assert elapsed < 0.15
    assert second_candidate_stopped.is_set()


@pytest.mark.asyncio
async def test_safe_transport_preserves_original_host_header_and_tls_sni() -> None:
    from app.providers.safe_http_transport import build_safe_async_http_transport

    async def resolver(host: str, port: int) -> list[str]:
        assert (host, port) == ("provider.example", 443)
        return ["8.8.8.8"]

    stream = _RecordingNetworkStream(
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
    )
    network = _RecordingNetworkBackend(stream)
    transport = build_safe_async_http_transport(resolver=resolver, network_backend=network)

    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        response = await client.get("https://provider.example/video")

    assert response.text == "ok"
    assert network.tcp_calls == [("8.8.8.8", 443)]
    assert stream.tls_server_names == ["provider.example"]
    assert stream.tls_contexts[0].check_hostname is True
    assert stream.tls_contexts[0].verify_mode == ssl.CERT_REQUIRED
    assert b"Host: provider.example\r\n" in b"".join(stream.writes)


@pytest.mark.asyncio
async def test_safe_download_to_file_strips_sensitive_headers_on_cross_origin_redirect(
    monkeypatch, tmp_path: Path
) -> None:
    from app.providers import _url_safety
    from app.providers.safe_http_transport import SafeAsyncNetworkBackend

    first = _StreamResponse(
        "https://provider.example/video",
        status_code=302,
        headers={"location": "https://cdn.example/result.mp4"},
    )
    second = _StreamResponse("https://cdn.example/result.mp4", chunks=[b"abc", b"def"])
    client = _StreamHttpClient([first, second])
    client_options: dict[str, object] = {}
    validated: list[str] = []
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)

    def client_factory(**kwargs):
        client_options.update(kwargs)
        return client

    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", client_factory)
    destination = tmp_path / "video.mp4"

    size = await _url_safety.safe_download_to_file(
        "https://provider.example/video",
        destination,
        max_bytes=32,
        extra_headers={
            "Authorization": "Bearer secret",
            "X-Goog-Api-Key": "secret-key",
            "X-API-Key": "generic-secret-key",
            "aPi-KeY": "alternate-secret-key",
            "Cookie": "session=secret",
            "X-Trace": "trace-id",
        },
        url_validator=validated.append,
    )

    assert size == 6
    assert destination.read_bytes() == b"abcdef"
    assert validated == [
        "https://provider.example/video",
        "https://cdn.example/result.mp4",
    ]
    redirected_headers = {key.lower(): value for key, value in client.calls[1][1].items()}
    assert "authorization" not in redirected_headers
    assert "x-goog-api-key" not in redirected_headers
    assert "x-api-key" not in redirected_headers
    assert "api-key" not in redirected_headers
    assert "cookie" not in redirected_headers
    assert redirected_headers["x-trace"] == "trace-id"
    assert client_options["follow_redirects"] is False
    assert client_options["trust_env"] is False
    transport = client_options["transport"]
    assert isinstance(transport, httpx.AsyncHTTPTransport)
    pool = transport._pool  # type: ignore[attr-defined]
    assert isinstance(pool._network_backend, SafeAsyncNetworkBackend)  # type: ignore[attr-defined]
    assert pool._ssl_context.check_hostname is True  # type: ignore[attr-defined]
    assert pool._ssl_context.verify_mode == ssl.CERT_REQUIRED  # type: ignore[attr-defined]
    await transport.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("download_kind", ["bytes", "file"])
async def test_safe_download_allows_three_redirects_by_default(
    monkeypatch,
    tmp_path: Path,
    download_kind: str,
) -> None:
    from app.providers import _url_safety

    client = _StreamHttpClient(_redirect_chain(3, include_success=True))
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)

    if download_kind == "bytes":
        assert await _url_safety.safe_download_bytes(
            "https://provider.example/hop-0",
            max_bytes=32,
        ) == b"video"
    else:
        destination = tmp_path / "video.mp4"
        assert await _url_safety.safe_download_to_file(
            "https://provider.example/hop-0",
            destination,
            max_bytes=32,
        ) == 5
        assert destination.read_bytes() == b"video"

    assert len(client.calls) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("download_kind", ["bytes", "file"])
async def test_safe_download_rejects_fourth_redirect_by_default(
    monkeypatch,
    tmp_path: Path,
    download_kind: str,
) -> None:
    from app.providers import _url_safety

    client = _StreamHttpClient(_redirect_chain(4, include_success=False))
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)

    with pytest.raises(RuntimeError, match="download_too_many_redirects"):
        if download_kind == "bytes":
            await _url_safety.safe_download_bytes(
                "https://provider.example/hop-0",
                max_bytes=32,
            )
        else:
            await _url_safety.safe_download_to_file(
                "https://provider.example/hop-0",
                tmp_path / "video.mp4",
                max_bytes=32,
            )

    assert len(client.calls) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("download_kind", ["bytes", "file"])
async def test_safe_download_does_not_replay_response_cookie_to_redirect_subdomain(
    monkeypatch,
    tmp_path: Path,
    download_kind: str,
) -> None:
    from app.providers import _url_safety

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "provider.example":
            return httpx.Response(
                302,
                headers={
                    "location": "https://cdn.provider.example/result.mp4",
                    "set-cookie": "provider_session=secret; Domain=provider.example; Path=/",
                },
            )
        return httpx.Response(200, content=b"video")

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(_url_safety, "build_safe_async_http_transport", lambda: transport)
    destination = tmp_path / "video.mp4"

    if download_kind == "bytes":
        result = await _url_safety.safe_download_bytes(
            "https://provider.example/video",
            max_bytes=32,
        )
        assert result == b"video"
    else:
        await _url_safety.safe_download_to_file(
            "https://provider.example/video",
            destination,
            max_bytes=32,
        )
        assert destination.read_bytes() == b"video"

    assert [request.url.host for request in requests] == [
        "provider.example",
        "cdn.provider.example",
    ]
    assert "cookie" not in requests[1].headers


@pytest.mark.asyncio
@pytest.mark.parametrize("download_kind", ["bytes", "file"])
async def test_safe_download_redacts_presigned_query_from_http_logs(
    monkeypatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    download_kind: str,
) -> None:
    from app.providers import _url_safety

    sensitive_url = (
        "https://provider.example/video.mp4"
        "?X-Tos-Signature=signature-secret"
        "&X-Tos-Credential=credential-secret"
        "&token=token-secret"
    )
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, content=b"video"))
    monkeypatch.setattr(_url_safety, "build_safe_async_http_transport", lambda: transport)
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    if download_kind == "bytes":
        assert await _url_safety.safe_download_bytes(sensitive_url, max_bytes=32) == b"video"
    else:
        destination = tmp_path / "video.mp4"
        assert (
            await _url_safety.safe_download_to_file(sensitive_url, destination, max_bytes=32) == 5
        )
        assert destination.read_bytes() == b"video"

    messages = "\n".join(record.getMessage() for record in caplog.records)
    for sensitive_fragment in (
        "X-Tos-Signature",
        "signature-secret",
        "X-Tos-Credential",
        "credential-secret",
        "token",
        "token-secret",
    ):
        assert sensitive_fragment not in messages
    assert "HTTP Request: GET https://provider.example/video.mp4" in messages
    assert "HTTP/1.1 200 OK" in messages


@pytest.mark.asyncio
async def test_safe_transport_httpcore_debug_logs_keep_diagnostics_without_query_secrets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from app.providers.safe_http_transport import build_safe_async_http_transport

    async def resolver(_host: str, _port: int) -> list[str]:
        return ["8.8.8.8"]

    stream = _RecordingNetworkStream(
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
    )
    transport = build_safe_async_http_transport(
        resolver=resolver,
        network_backend=_RecordingNetworkBackend(stream),
    )
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        response = await client.get(
            "https://provider.example/video"
            "?X-Tos-Signature=signature-secret&token=token-secret"
        )

    assert response.text == "ok"
    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "signature-secret" not in messages
    assert "token-secret" not in messages
    assert "connect_tcp.started host='provider.example' port=443" in messages
    assert "send_request_headers.started" in messages
    assert "HTTP Request: GET https://provider.example/video" in messages


@pytest.mark.asyncio
async def test_safe_download_to_file_rejects_content_length_before_streaming(
    monkeypatch, tmp_path: Path
) -> None:
    from app.providers import _url_safety

    response = _StreamResponse(
        "https://provider.example/video",
        headers={"content-length": "9"},
        chunks=[b"not-read"],
    )
    client = _StreamHttpClient([response])
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    destination = tmp_path / "video.mp4"

    with pytest.raises(RuntimeError, match="download_too_large"):
        await _url_safety.safe_download_to_file(
            "https://provider.example/video", destination, max_bytes=8
        )

    assert response.iterated is False
    assert not destination.exists()


@pytest.mark.asyncio
async def test_safe_download_to_file_enforces_stream_limit_and_removes_partial_file(
    monkeypatch, tmp_path: Path
) -> None:
    from app.providers import _url_safety

    response = _StreamResponse(
        "https://provider.example/video",
        chunks=[b"1234", b"56789"],
    )
    client = _StreamHttpClient([response])
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    destination = tmp_path / "video.mp4"

    with pytest.raises(RuntimeError, match="download_too_large"):
        await _url_safety.safe_download_to_file(
            "https://provider.example/video", destination, max_bytes=8
        )

    assert response.iterated is True
    assert not destination.exists()


@pytest.mark.asyncio
async def test_safe_download_to_file_rejects_empty_body_and_removes_file(
    monkeypatch, tmp_path: Path
) -> None:
    from app.providers import _url_safety

    client = _StreamHttpClient([_StreamResponse("https://provider.example/video")])
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    destination = tmp_path / "video.mp4"

    with pytest.raises(RuntimeError, match="download_empty"):
        await _url_safety.safe_download_to_file(
            "https://provider.example/video", destination, max_bytes=8
        )

    assert not destination.exists()


@pytest.mark.asyncio
async def test_async_chunks_to_file_rejects_negative_declared_size(tmp_path: Path) -> None:
    from app.providers.streaming_io import async_chunks_to_file

    async def chunks():
        yield b"video"

    destination = tmp_path / "video.mp4"
    with pytest.raises(ValueError, match="declared_size"):
        await async_chunks_to_file(
            chunks(),
            destination,
            max_bytes=32,
            declared_size=-1,
        )

    assert not destination.exists()


@pytest.mark.asyncio
async def test_async_chunks_to_file_removes_partial_file_when_cancelled(
    tmp_path: Path,
) -> None:
    from app.providers.streaming_io import async_chunks_to_file

    async def chunks():
        yield b"partial"
        raise asyncio.CancelledError

    destination = tmp_path / "video.mp4"
    with pytest.raises(asyncio.CancelledError):
        await async_chunks_to_file(chunks(), destination, max_bytes=32)

    assert not destination.exists()


@pytest.mark.asyncio
async def test_async_chunks_to_file_checks_cancellation_between_chunks(
    tmp_path: Path,
) -> None:
    from app.providers.streaming_io import async_chunks_to_file

    chunks_read = 0

    async def chunks():
        nonlocal chunks_read
        for chunk in (b"first", b"second", b"third"):
            chunks_read += 1
            yield chunk

    cancellation_probe = AsyncMock(
        side_effect=[None, RuntimeError("download_canceled")]
    )
    destination = tmp_path / "video.mp4"

    with pytest.raises(RuntimeError, match="download_canceled"):
        await async_chunks_to_file(
            chunks(),
            destination,
            max_bytes=32,
            cancellation_probe=cancellation_probe,
        )

    assert chunks_read == 1
    assert cancellation_probe.await_count == 2
    assert not destination.exists()


@pytest.mark.asyncio
async def test_atomic_put_file_cleans_partial_source_and_final_objects(tmp_path: Path) -> None:
    from app.providers.streaming_io import atomic_put_file

    source = tmp_path / "video.mp4"
    source.write_bytes(b"video")

    class FailingStorage(_AtomicFakeStorage):
        async def promote(self, source_key: str, final_key: str) -> str:
            self.objects[final_key] = self.objects[source_key]
            raise RuntimeError("promotion_failed")

    storage = FailingStorage()
    with pytest.raises(RuntimeError, match="promotion_failed"):
        await atomic_put_file(
            storage,
            "generate_images/video/job-attempt.mp4",
            source,
            attempt_id="attempt",
        )

    assert storage.objects == {}
    assert "generate_images/video/job-attempt.mp4" in storage.deleted
    assert any(".upload-attempt-" in key for key in storage.deleted)


@pytest.mark.asyncio
async def test_atomic_put_file_cleans_objects_when_upload_fails(tmp_path: Path) -> None:
    from app.providers.streaming_io import atomic_put_file

    source = tmp_path / "video.mp4"
    source.write_bytes(b"video")

    class FailingStorage(_AtomicFakeStorage):
        async def put_file(self, key: str, path: str) -> str:
            self.objects[key] = Path(path).read_bytes()
            raise RuntimeError("upload_failed")

    storage = FailingStorage()
    final_key = "generate_images/video/job-attempt.mp4"
    with pytest.raises(RuntimeError, match="upload_failed"):
        await atomic_put_file(storage, final_key, source, attempt_id="attempt")

    assert storage.objects == {}
    assert final_key in storage.deleted
    assert any(".upload-attempt-" in key for key in storage.deleted)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["upload", "promote", "missing-temp"])
async def test_atomic_put_file_preserves_preexisting_legacy_final(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    from app.providers.streaming_io import atomic_put_file

    source = tmp_path / "video.mp4"
    source.write_bytes(b"new-video")
    final_key = "generate_images/video/legacy-job.mp4"

    class LegacyStorage(_AtomicFakeStorage):
        async def put_file(self, key: str, path: str) -> str:
            if failure_stage == "missing-temp":
                return key
            self.objects[key] = Path(path).read_bytes()
            if failure_stage == "upload":
                raise RuntimeError("upload_failed")
            return key

        async def promote(self, source_key: str, target_key: str) -> str:
            if failure_stage == "promote":
                raise RuntimeError("promotion_failed")
            return await super().promote(source_key, target_key)

    storage = LegacyStorage()
    storage.objects[final_key] = b"old-video"

    with pytest.raises(RuntimeError):
        await atomic_put_file(
            storage,
            final_key,
            source,
            attempt_id="legacy",
            preserve_existing_final=True,
        )

    assert storage.objects == {final_key: b"old-video"}
    assert final_key not in storage.deleted
    assert not any(".upload-" in key for key in storage.objects)


@pytest.mark.asyncio
async def test_atomic_put_file_enqueues_failed_object_cleanup_without_masking_error(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.core import database
    from app.providers.streaming_io import atomic_put_file
    from app.services import media_storage_jobs

    source = tmp_path / "video.mp4"
    source.write_bytes(b"video")
    final_key = "generate_images/video/job-attempt.mp4"

    class Storage(_AtomicFakeStorage):
        async def put_file(self, key: str, path: str) -> str:
            self.objects[key] = Path(path).read_bytes()
            raise RuntimeError("original_upload_failed")

        async def delete(self, key: str) -> None:
            self.deleted.append(key)
            raise RuntimeError("delete_failed")

    class Db:
        commit = AsyncMock()

    db = Db()

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return None

    enqueue_delete = AsyncMock()
    monkeypatch.setattr(database, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)
    storage = Storage()

    with pytest.raises(RuntimeError, match="original_upload_failed"):
        await atomic_put_file(storage, final_key, source, attempt_id="attempt")

    enqueued = {
        call.kwargs["source_key"]: call.kwargs["idempotency_key"]
        for call in enqueue_delete.await_args_list
    }
    assert final_key in enqueued
    temp_key = next(key for key in enqueued if key != final_key)
    assert ".upload-attempt-" in temp_key
    assert enqueued == {
        key: f"video-delete:{hashlib.sha256(key.encode('utf-8')).hexdigest()}"
        for key in (temp_key, final_key)
    }
    assert db.commit.await_count == 2


@pytest.mark.asyncio
async def test_atomic_put_file_does_not_compensate_preexisting_legacy_final(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.core import database
    from app.providers.streaming_io import atomic_put_file
    from app.services import media_storage_jobs

    source = tmp_path / "video.mp4"
    source.write_bytes(b"new-video")
    final_key = "generate_images/video/legacy-job.mp4"

    class Storage(_AtomicFakeStorage):
        async def put_file(self, key: str, path: str) -> str:
            self.objects[key] = Path(path).read_bytes()
            raise RuntimeError("original_upload_failed")

        async def delete(self, key: str) -> None:
            self.deleted.append(key)
            raise RuntimeError("delete_failed")

    class SessionContext:
        async def __aenter__(self):
            return SimpleNamespace(commit=AsyncMock())

        async def __aexit__(self, exc_type, exc, tb):
            return None

    enqueue_delete = AsyncMock()
    monkeypatch.setattr(database, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)
    storage = Storage()
    storage.objects[final_key] = b"old-video"

    with pytest.raises(RuntimeError, match="original_upload_failed"):
        await atomic_put_file(
            storage,
            final_key,
            source,
            attempt_id="legacy",
            preserve_existing_final=True,
        )

    assert final_key not in storage.deleted
    assert all(call.kwargs["source_key"] != final_key for call in enqueue_delete.await_args_list)
    assert storage.objects[final_key] == b"old-video"


@pytest.mark.asyncio
async def test_cleanup_compensation_failure_does_not_mask_upload_error(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.core import database
    from app.providers.streaming_io import atomic_put_file

    source = tmp_path / "video.mp4"
    source.write_bytes(b"video")

    class Storage(_AtomicFakeStorage):
        async def put_file(self, key: str, path: str) -> str:
            raise RuntimeError("original_upload_failed")

        async def delete(self, key: str) -> None:
            raise RuntimeError("delete_failed")

    monkeypatch.setattr(
        database,
        "async_session_factory",
        lambda: (_ for _ in ()).throw(RuntimeError("database_unavailable")),
    )

    with pytest.raises(RuntimeError, match="original_upload_failed"):
        await atomic_put_file(
            Storage(),
            "generate_images/video/job-attempt.mp4",
            source,
            attempt_id="attempt",
        )


def test_storage_attempt_suffix_preserves_worker_hash_and_hashes_external_value() -> None:
    from app.providers.streaming_io import storage_attempt_suffix

    worker_attempt = "0123456789abcdef"
    assert storage_attempt_suffix({"storage_attempt_id": worker_attempt}) == worker_attempt

    external = storage_attempt_suffix({"storage_attempt_id": "lease-token-sensitive"})
    assert re.fullmatch(r"[0-9a-f]{16}", external)
    assert "lease-token-sensitive" not in external


async def _write_fake_video(path: str | Path) -> int:
    from app.providers.streaming_io import async_chunks_to_file

    async def chunks():
        yield b"video-"
        yield b"bytes"

    return await async_chunks_to_file(chunks(), path, max_bytes=1024)


@pytest.mark.asyncio
async def test_bailian_provider_streams_to_file_and_preserves_result_contract(
    monkeypatch,
) -> None:
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    class PollClient:
        async def aclose(self) -> None:
            return None

        async def post(self, *_args, **_kwargs):
            raise AssertionError("恢复任务不得重复提交")

    storage = _AtomicFakeStorage()
    provider = BailianVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr("app.providers.video_gen_bailian.httpx.AsyncClient", lambda **_kwargs: PollClient())
    monkeypatch.setattr(
        provider,
        "_poll",
        AsyncMock(return_value="https://result.aliyuncs.com/video.mp4"),
    )
    downloaded_paths: list[str] = []

    async def download(_url: str, path: str | Path) -> int:
        downloaded_paths.append(str(path))
        return await _write_fake_video(path)

    monkeypatch.setattr(provider, "_download_to_file", download)
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    result = await provider.generate(
        _request(job_id="job-bailian", provider_task_id="task-bailian")
    )

    assert re.fullmatch(
        r"generate_images/video/job-bailian-[0-9a-f]{16}\.mp4",
        result.video_storage_key,
    )
    assert storage.objects[result.video_storage_key] == b"video-bytes"
    assert result.provider_job_id == "task-bailian"
    assert downloaded_paths and not Path(downloaded_paths[0]).exists()
    assert not any(".upload-" in key for key in storage.objects)


@pytest.mark.asyncio
async def test_volcengine_provider_streams_url_and_preserves_result_contract(
    monkeypatch,
) -> None:
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    storage = _AtomicFakeStorage()
    provider = VolcengineVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(
        provider,
        "_submit_and_poll",
        AsyncMock(
            return_value=(
                "https://cdn.example/video.mp4",
                {
                    "task_id": "task-volc",
                    "volcengine_status": "succeeded",
                    "job_id": "job-volc",
                },
            )
        ),
    )
    monkeypatch.setattr(
        provider,
        "_download_to_file",
        lambda _url, path, **_kwargs: _write_fake_video(path),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    result = await provider.generate(
        _request(job_id="job-volc", provider_task_id="task-volc")
    )

    assert re.fullmatch(
        r"generate_images/video/job-volc-[0-9a-f]{16}\.mp4",
        result.video_storage_key,
    )
    assert storage.objects[result.video_storage_key] == b"video-bytes"
    assert result.provider_job_id == "task-volc"


@pytest.mark.parametrize("close_kind", ["sync", "async"])
@pytest.mark.asyncio
async def test_volcengine_submit_and_poll_supports_sync_and_async_ark_close(
    monkeypatch,
    close_kind: str,
) -> None:
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    tasks = SimpleNamespace(
        create=AsyncMock(side_effect=AssertionError("恢复任务不得重复提交")),
        get=AsyncMock(return_value=SimpleNamespace(status="cancelled")),
    )
    closed: list[str] = []
    if close_kind == "async":

        async def close() -> None:
            closed.append(close_kind)

    else:

        def close() -> None:
            closed.append(close_kind)

    client = SimpleNamespace(
        content_generation=SimpleNamespace(tasks=tasks),
        close=close,
    )
    provider = VolcengineVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)

    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await provider._submit_and_poll(
            "video-model",
            "prompt",
            [],
            [],
            None,
            lambda *_args: None,
            provider_task_id=f"volcengine-{close_kind}-close",
        )

    assert exc_info.value.provider_task_id == f"volcengine-{close_kind}-close"
    assert exc_info.value.terminal_status == "canceled"
    tasks.create.assert_not_awaited()
    tasks.get.assert_awaited_once_with(
        task_id=f"volcengine-{close_kind}-close"
    )
    assert closed == [close_kind]


@pytest.mark.asyncio
async def test_volcengine_close_compatibility_hook_stays_inside_managed_deadline(
    monkeypatch,
) -> None:
    import time

    from app.providers import video_gen
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    primary_error = VideoGenerationRemoteTerminalError(
        provider_task_id="volcengine-compatibility-close",
        terminal_status="failed",
    )
    close_cancelled = asyncio.Event()
    close_finished = asyncio.Event()
    release_close = asyncio.Event()

    class ContentGeneration:
        @property
        def tasks(self):
            raise primary_error

    class Client:
        content_generation = ContentGeneration()

        async def close(self) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                current = asyncio.current_task()
                assert current is not None
                while current.cancelling():
                    current.uncancel()
                await release_close.wait()
            finally:
                close_finished.set()

    client = Client()
    provider = VolcengineVideoGenProvider()
    compatibility_calls: list[object] = []

    async def compatibility_close(client_to_close) -> None:
        compatibility_calls.append(client_to_close)
        await provider._close_client(client_to_close)

    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(
        provider,
        "_close_client_safely",
        compatibility_close,
        raising=False,
    )
    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)
    asyncio.get_running_loop().call_later(0.15, release_close.set)

    started_at = time.monotonic()
    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await provider._submit_and_poll(
            "video-model",
            "prompt",
            [],
            [],
            None,
            lambda *_args: None,
        )
    elapsed = time.monotonic() - started_at

    assert exc_info.value is primary_error
    assert compatibility_calls == [client]
    assert close_cancelled.is_set()
    assert elapsed < 0.08
    assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1
    await asyncio.wait_for(close_finished.wait(), timeout=0.3)
    await asyncio.sleep(0)
    assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()


@pytest.mark.parametrize(
    "url",
    [
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/model/video.mp4",
        "https://ark-content-generation-ap-southeast-1.tos-ap-southeast-1.volces.com/video.mp4",
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com:443/video.mp4",
        "https://ark.cn-beijing.volces.com/api/v3/video.mp4",
    ],
)
def test_volcengine_download_host_allows_official_ark_origins(url: str) -> None:
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    VolcengineVideoGenProvider()._validate_download_host(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://cdn.example.com/video.mp4",
        "http://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/video.mp4",
        "https://user:password@ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/video.mp4",
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com:8443/video.mp4",
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com.evil.test/video.mp4",
        "https://ark-content-generation-cn-beijing.tos-ap-southeast-1.volces.com/video.mp4",
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com:bad/video.mp4",
    ],
)
def test_volcengine_download_host_rejects_untrusted_origin(url: str) -> None:
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        VolcengineVideoGenProvider()._validate_download_host(url)


@pytest.mark.asyncio
async def test_volcengine_download_rejects_untrusted_initial_url_before_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    client = _StreamHttpClient(
        [_StreamResponse("https://cdn.example.com/video.mp4", chunks=[b"video"])]
    )
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        await VolcengineVideoGenProvider()._download_to_file(
            "https://cdn.example.com/video.mp4",
            tmp_path / "video.mp4",
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_volcengine_download_revalidates_redirect_before_second_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    official_url = (
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/model/video.mp4"
    )
    first = _StreamResponse(
        official_url,
        status_code=302,
        headers={"location": "https://cdn.example.com/video.mp4"},
    )
    second = _StreamResponse("https://cdn.example.com/video.mp4", chunks=[b"video"])
    client = _StreamHttpClient([first, second])
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        await VolcengineVideoGenProvider()._download_to_file(
            official_url,
            tmp_path / "video.mp4",
        )

    assert [url for url, _headers in client.calls] == [official_url]


@pytest.mark.parametrize("base_url", ["", "   "])
def test_openai_client_uses_sdk_default_for_explicit_blank_base_url(
    monkeypatch,
    base_url: str,
) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    captured: dict[str, object] = {}
    sentinel = object()

    def build_client(**kwargs):
        captured.update(kwargs)
        return sentinel

    configured = _settings(
        video_gen_provider="openai",
        video_gen_api_key="test-openai-key",
        video_gen_base_url=base_url,
    )
    assert configured.is_explicitly_configured("video_gen_base_url")
    monkeypatch.setattr(video_gen_openai, "settings", configured)
    monkeypatch.setattr("openai.AsyncOpenAI", build_client)

    provider = OpenAIVideoGenProvider()
    assert provider._build_client() is sentinel
    assert provider._api_base_url() == "https://api.openai.com/v1"
    assert captured == {
        "api_key": "test-openai-key",
        "base_url": None,
        "max_retries": 0,
    }


def test_openai_client_preserves_explicit_nonempty_base_url(monkeypatch) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    captured: dict[str, object] = {}

    configured = _settings(
        video_gen_provider="openai",
        video_gen_api_key="test-openai-key",
        video_gen_base_url="https://gateway.example/v1",
    )
    monkeypatch.setattr(video_gen_openai, "settings", configured)
    monkeypatch.setattr(
        "openai.AsyncOpenAI",
        lambda **kwargs: captured.update(kwargs) or object(),
    )

    provider = OpenAIVideoGenProvider()
    provider._build_client()

    assert captured == {
        "api_key": "test-openai-key",
        "base_url": "https://gateway.example/v1",
        "max_retries": 0,
    }
    assert provider._api_base_url() == "https://gateway.example/v1"


@pytest.mark.asyncio
async def test_openai_provider_uses_safe_download_and_preserves_result_contract(monkeypatch) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    calls: dict[str, object] = {}

    class Videos:
        async def retrieve(self, video_id: str):
            calls["retrieve"] = video_id
            return SimpleNamespace(status="completed")

        with_streaming_response = SimpleNamespace(
            download_content=lambda _video_id: (_ for _ in ()).throw(
                AssertionError("内容下载必须经过共享安全下载器")
            )
        )

        async def download_content(self, _video_id: str):
            raise AssertionError("必须使用 with_streaming_response")

    client = SimpleNamespace(videos=Videos(), close=AsyncMock())
    storage = _AtomicFakeStorage()
    provider = OpenAIVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://api.openai.com/v1",
        ),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    async def download(video_id: str, path: str | Path, **_kwargs) -> int:
        calls["download"] = video_id
        return await _write_fake_video(path)

    monkeypatch.setattr(provider, "_download_to_file", download)

    result = await provider.generate(
        _request(job_id="job-openai", provider_task_id="task-openai", model="sora-2")
    )

    assert calls == {
        "retrieve": "task-openai",
        "download": "task-openai",
    }
    assert storage.objects[result.video_storage_key] == b"video-bytes"
    assert result.provider_job_id == "task-openai"
    assert re.fullmatch(
        r"generate_images/video/job-openai-[0-9a-f]{16}\.mp4",
        result.video_storage_key,
    )
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_openai_download_rejects_fourth_redirect_through_safe_downloader(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety, video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    client = _StreamHttpClient(_redirect_chain(4, include_success=False))
    validated: list[str] = []
    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://provider.example/v1",
        ),
    )
    monkeypatch.setattr(
        _url_safety,
        "validate_public_http_url",
        lambda url: validated.append(url),
    )
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    destination = tmp_path / "video.mp4"

    with pytest.raises(RuntimeError, match="download_too_many_redirects"):
        await OpenAIVideoGenProvider()._download_to_file("video/id", destination)

    assert len(client.calls) == 4
    assert len(validated) == 4
    assert client.calls[0][0] == "https://provider.example/v1/videos/video%2Fid/content"
    assert client.calls[0][1]["Authorization"] == "Bearer test-openai-key"
    assert not destination.exists()


@pytest.mark.asyncio
async def test_openai_download_streams_success_through_safe_downloader(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety, video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    download_url = "https://provider.example/v1/videos/video-id/content"
    response = _StreamResponse(download_url, chunks=[b"video-", b"bytes"])
    client = _StreamHttpClient([response])
    validated: list[str] = []
    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://provider.example/v1",
        ),
    )
    monkeypatch.setattr(
        _url_safety,
        "validate_public_http_url",
        lambda url: validated.append(url),
    )
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    destination = tmp_path / "video.mp4"

    written = await OpenAIVideoGenProvider()._download_to_file("video-id", destination)

    assert written == 11
    assert destination.read_bytes() == b"video-bytes"
    assert validated == [download_url]
    assert client.calls == [
        (download_url, {"Authorization": "Bearer test-openai-key"})
    ]


@pytest.mark.asyncio
async def test_openai_download_revalidates_redirect_before_second_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety, video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    initial_url = "https://provider.example/v1/videos/video-id/content"
    first = _StreamResponse(
        initial_url,
        status_code=302,
        headers={"location": "https://attacker.example/video.mp4"},
    )
    client = _StreamHttpClient(
        [first, _StreamResponse("https://attacker.example/video.mp4", chunks=[b"video"])]
    )
    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://provider.example/v1",
        ),
    )
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    stripped_headers: list[dict[str, str]] = []
    original_strip = _url_safety._strip_cross_origin_sensitive_headers

    def strip_headers(headers: dict[str, str]) -> dict[str, str]:
        stripped = original_strip(headers)
        stripped_headers.append(stripped)
        return stripped

    monkeypatch.setattr(_url_safety, "_strip_cross_origin_sensitive_headers", strip_headers)

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        await OpenAIVideoGenProvider()._download_to_file(
            "video-id",
            tmp_path / "video.mp4",
        )

    assert [url for url, _headers in client.calls] == [initial_url]
    assert stripped_headers == [{}]


@pytest.mark.parametrize(
    "url",
    [
        "https://provider.example/v1/videos/video-id/content",
        "https://provider.example:443/v1/videos/video-id/content",
    ],
)
def test_openai_download_host_allows_configured_https_origin(monkeypatch, url: str) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://provider.example/v1",
        ),
    )

    OpenAIVideoGenProvider()._validate_download_host(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://attacker.example/v1/videos/video-id/content",
        "http://provider.example/v1/videos/video-id/content",
        "https://user:password@provider.example/v1/videos/video-id/content",
        "https://provider.example:8443/v1/videos/video-id/content",
        "https://provider.example:bad/v1/videos/video-id/content",
    ],
)
def test_openai_download_host_rejects_untrusted_origin(monkeypatch, url: str) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://provider.example/v1",
        ),
    )

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        OpenAIVideoGenProvider()._validate_download_host(url)


@pytest.mark.asyncio
async def test_google_provider_streams_authenticated_uri_and_never_eager_download(
    monkeypatch,
) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    configured = _settings(
        video_gen_provider="gemini",
        video_gen_api_key="test-video-key",
        video_gen_model="veo-model",
    )
    monkeypatch.setattr(video_gen_google, "settings", configured)
    video = SimpleNamespace(
        uri="https://generativelanguage.googleapis.com/v1beta/files/abc123",
        video_bytes=None,
    )
    completed = SimpleNamespace(
        name="operations/veo-stream",
        done=True,
        error=None,
        result=SimpleNamespace(generated_videos=[SimpleNamespace(video=video)]),
        response=None,
    )
    client = SimpleNamespace(
        aio=SimpleNamespace(
            files=SimpleNamespace(
                download=AsyncMock(side_effect=AssertionError("禁止 eager files.download"))
            ),
            aclose=AsyncMock(),
        )
    )
    storage = _AtomicFakeStorage()
    provider = GoogleVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_poll", AsyncMock(return_value=completed))
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    download_calls: list[tuple[str, dict]] = []

    async def download(url: str, path: str | Path, **kwargs) -> int:
        download_calls.append((url, kwargs))
        kwargs["url_validator"](url)
        return await _write_fake_video(path)

    monkeypatch.setattr(video_gen_google, "safe_download_to_file", download)

    result = await provider.generate(
        _request(job_id="job-google", provider_task_id="operations/veo-stream")
    )

    assert download_calls[0][0] == (
        "https://generativelanguage.googleapis.com/v1beta/files/abc123:download?alt=media"
    )
    assert download_calls[0][1]["extra_headers"] == {
        "x-goog-api-key": "test-video-key"
    }
    client.aio.files.download.assert_not_awaited()
    assert storage.objects[result.video_storage_key] == b"video-bytes"
    assert result.provider_job_id == "operations/veo-stream"
    assert re.fullmatch(
        r"generate_images/video/job-google-[0-9a-f]{16}\.mp4",
        result.video_storage_key,
    )
    client.aio.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_google_provider_rejects_large_inline_bytes_before_file_creation(
    tmp_path: Path,
) -> None:
    from app.providers.video_gen_google import GoogleVideoGenProvider

    video = SimpleNamespace(uri=None, video_bytes=b"x" * (16 * 1024 * 1024 + 1))
    operation = SimpleNamespace(
        result=SimpleNamespace(generated_videos=[SimpleNamespace(video=video)]),
        response=None,
    )
    destination = tmp_path / "video.mp4"

    with pytest.raises(RuntimeError, match="video_inline_too_large"):
        await GoogleVideoGenProvider()._read_video_to_file(operation, destination)

    assert not destination.exists()


@pytest.mark.asyncio
async def test_openai_provider_download_uses_512_mib_limit(monkeypatch, tmp_path: Path) -> None:
    from app.providers import _url_safety, video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    captured: dict[str, int] = {}

    async def download(_url: str, _path: str | Path, **kwargs) -> int:
        captured["max_bytes"] = kwargs["max_bytes"]
        return 1

    monkeypatch.setattr(
        video_gen_openai,
        "settings",
        _settings(
            video_gen_provider="openai",
            video_gen_api_key="test-openai-key",
            video_gen_base_url="https://api.openai.com/v1",
        ),
    )
    monkeypatch.setattr(_url_safety, "safe_download_to_file", download)

    await OpenAIVideoGenProvider()._download_to_file(
        "video-id",
        tmp_path / "video.mp4",
    )

    assert captured["max_bytes"] == 512 * 1024 * 1024


@pytest.mark.parametrize(
    "url",
    [
        "https://dashscope.aliyuncs.com/api/v1/tasks/task-id",
        "https://dashscope-result.oss-cn-hangzhou.aliyuncs.com/video.mp4",
        "https://dashscope-result.oss-cn-hangzhou.aliyuncs.com:443/video.mp4",
    ],
)
def test_bailian_download_host_allows_audited_official_origins(
    monkeypatch,
    url: str,
) -> None:
    from app.providers import video_gen_bailian
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    monkeypatch.setattr(
        video_gen_bailian,
        "settings",
        _settings(
            video_gen_provider="bailian",
            video_gen_api_key="test-bailian-key",
            video_gen_base_url="https://dashscope.aliyuncs.com",
        ),
    )

    BailianVideoGenProvider()._validate_download_host(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://attacker.oss-cn-hangzhou.aliyuncs.com/video.mp4",
        "https://dashscope-result.evil.oss-cn-hangzhou.aliyuncs.com/video.mp4",
        "https://dashscope-result.oss-cn-hangzhou.aliyuncs.com.evil.test/video.mp4",
        "http://dashscope-result.oss-cn-hangzhou.aliyuncs.com/video.mp4",
        "https://user:password@dashscope-result.oss-cn-hangzhou.aliyuncs.com/video.mp4",
        "https://dashscope-result.oss-cn-hangzhou.aliyuncs.com:8443/video.mp4",
        "https://dashscope-result.oss-cn-hangzhou.aliyuncs.com:bad/video.mp4",
    ],
)
def test_bailian_download_host_rejects_untrusted_origin(monkeypatch, url: str) -> None:
    from app.providers import video_gen_bailian
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    monkeypatch.setattr(
        video_gen_bailian,
        "settings",
        _settings(
            video_gen_provider="bailian",
            video_gen_api_key="test-bailian-key",
            video_gen_base_url="https://dashscope.aliyuncs.com",
        ),
    )

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        BailianVideoGenProvider()._validate_download_host(url)


@pytest.mark.asyncio
async def test_google_provider_download_uses_512_mib_limit(monkeypatch, tmp_path: Path) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    captured: dict[str, int] = {}

    async def download(_url: str, _path: str | Path, **kwargs) -> int:
        captured["max_bytes"] = kwargs["max_bytes"]
        return 1

    monkeypatch.setattr(
        video_gen_google,
        "settings",
        _settings(video_gen_provider="gemini", video_gen_api_key="key"),
    )
    monkeypatch.setattr(video_gen_google, "safe_download_to_file", download)
    operation = SimpleNamespace(
        result=SimpleNamespace(
            generated_videos=[
                SimpleNamespace(
                    video=SimpleNamespace(
                        uri="https://generativelanguage.googleapis.com/v1beta/files/abc123",
                        video_bytes=None,
                    )
                )
            ]
        ),
        response=None,
    )

    await GoogleVideoGenProvider()._read_video_to_file(operation, tmp_path / "video.mp4")

    assert captured["max_bytes"] == 512 * 1024 * 1024


@pytest.mark.asyncio
async def test_bailian_provider_download_uses_512_mib_limit(monkeypatch, tmp_path: Path) -> None:
    from app.providers import _url_safety
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    captured: dict[str, int] = {}

    async def download(_url: str, _path: str | Path, **kwargs) -> int:
        captured["max_bytes"] = kwargs["max_bytes"]
        return 1

    monkeypatch.setattr(_url_safety, "safe_download_to_file", download)

    await BailianVideoGenProvider()._download_to_file(
        "https://dashscope-result.oss-cn-hangzhou.aliyuncs.com/video.mp4",
        tmp_path / "video.mp4",
    )

    assert captured["max_bytes"] == 512 * 1024 * 1024


@pytest.mark.asyncio
async def test_volcengine_provider_download_uses_512_mib_limit(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    captured: dict[str, int] = {}

    async def download(_url: str, _path: str | Path, **kwargs) -> int:
        captured["max_bytes"] = kwargs["max_bytes"]
        return 1

    monkeypatch.setattr(_url_safety, "safe_download_to_file", download)

    await VolcengineVideoGenProvider()._download_to_file(
        "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/video.mp4",
        tmp_path / "video.mp4",
    )

    assert captured["max_bytes"] == 512 * 1024 * 1024


def test_google_download_url_rejects_untrusted_uri_host(monkeypatch) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    monkeypatch.setattr(
        video_gen_google,
        "settings",
        _settings(video_gen_provider="gemini", video_gen_api_key="key"),
    )

    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        GoogleVideoGenProvider()._build_download_url(
            "https://attacker.example/v1beta/files/abc123"
        )


def test_google_download_url_forces_media_query_and_rejects_non_file_paths(
    monkeypatch,
) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    monkeypatch.setattr(
        video_gen_google,
        "settings",
        _settings(video_gen_provider="gemini", video_gen_api_key="key"),
    )
    provider = GoogleVideoGenProvider()

    assert provider._build_download_url(
        "https://generativelanguage.googleapis.com/v1beta/files/abc123?alt=json&key=bad"
    ) == (
        "https://generativelanguage.googleapis.com/v1beta/files/abc123:download?alt=media"
    )
    for uri in (
        "https://generativelanguage.googleapis.com/v1beta/files/",
        "https://generativelanguage.googleapis.com/v1beta/files/../models/model",
        "https://generativelanguage.googleapis.com/v1beta/files/abc123/extra",
        "https://generativelanguage.googleapis.com/v1beta/models/model",
    ):
        with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
            provider._build_download_url(uri)


def test_google_relative_file_uri_uses_explicit_gateway_origin(monkeypatch) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    monkeypatch.setattr(
        video_gen_google,
        "settings",
        _settings(
            video_gen_provider="gemini",
            video_gen_api_key="gateway-key",
            video_gen_base_url="https://gateway.example/api/v1",
        ),
    )

    assert GoogleVideoGenProvider()._build_download_url("files/abc123") == (
        "https://gateway.example/v1beta/files/abc123:download?alt=media"
    )


@pytest.mark.asyncio
async def test_google_download_allows_exact_storage_redirect_after_api_key_is_stripped(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety, video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    monkeypatch.setattr(
        video_gen_google,
        "settings",
        _settings(video_gen_provider="gemini", video_gen_api_key="key"),
    )
    first = _StreamResponse(
        "https://generativelanguage.googleapis.com/v1beta/files/abc123:download?alt=media",
        status_code=302,
        headers={"location": "https://storage.googleapis.com/bucket/video.mp4"},
    )
    second = _StreamResponse(
        "https://storage.googleapis.com/bucket/video.mp4",
        chunks=[b"video"],
    )
    client = _StreamHttpClient([first, second])
    monkeypatch.setattr(_url_safety, "validate_public_http_url", lambda _url: None)
    monkeypatch.setattr(_url_safety.httpx, "AsyncClient", lambda **_kwargs: client)
    destination = tmp_path / "video.mp4"

    await _url_safety.safe_download_to_file(
        str(first.url),
        destination,
        max_bytes=32,
        extra_headers={"x-goog-api-key": "secret"},
        url_validator=GoogleVideoGenProvider()._validate_download_hop,
    )

    redirected_headers = {key.lower() for key in client.calls[1][1]}
    assert "x-goog-api-key" not in redirected_headers
    assert destination.read_bytes() == b"video"


@pytest.mark.asyncio
async def test_all_video_providers_preflight_reference_size_before_read(
    monkeypatch,
) -> None:
    from app.providers.video_gen_bailian import BailianVideoGenProvider
    from app.providers.video_gen_google import GoogleVideoGenProvider
    from app.providers.video_gen_openai import OpenAIVideoGenProvider
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    storage = SimpleNamespace(
        get_size=AsyncMock(return_value=50 * 1024 * 1024 + 1),
        get=AsyncMock(return_value=b"must-not-read"),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    request = _request(
        job_id="job-ref",
        provider_task_id="task-ref",
        first_frame="frames/first.png",
    )

    calls = (
        BailianVideoGenProvider()._encode_single("frames/first.png"),
        OpenAIVideoGenProvider._load_input_reference(request),
        GoogleVideoGenProvider._load_storage_image("frames/first.png", "image/png"),
        VolcengineVideoGenProvider()._encode_storage_refs(
            [{"storage_key": "frames/first.png", "mime_type": "image/png"}],
            "image/png",
        ),
    )
    for call in calls:
        with pytest.raises(RuntimeError, match="reference_image_too_large"):
            await call

    storage.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_volcengine_preflight_rejects_aggregate_reference_size(monkeypatch) -> None:
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    storage = SimpleNamespace(
        get_size=AsyncMock(return_value=50 * 1024 * 1024),
        get=AsyncMock(return_value=b"must-not-read"),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    references = [
        {"storage_key": f"images/{index}.png", "mime_type": "image/png"}
        for index in range(5)
    ]

    with pytest.raises(RuntimeError, match="reference_media_total_too_large"):
        await VolcengineVideoGenProvider()._preflight_reference_sizes(
            references,
            [],
            None,
        )

    storage.get.assert_not_awaited()
