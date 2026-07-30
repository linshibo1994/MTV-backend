"""MeiZi 三段式协议与 StorageProvider 契约测试；禁止访问真实媒资服务。"""

from __future__ import annotations

import asyncio
import hashlib
import os
import traceback
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.models.meizi_storage_object import MeiziStorageObject
from app.providers import storage_meizi as storage_meizi_module
from app.providers.meizi_client import (
    MeiziClientError,
    MeiziFileCenterClient,
    MeiziMediaKind,
    MeiziUploadIdentity,
    MeiziUploadResult,
)
from app.providers.storage import StorageObjectConflictError, StorageObjectNotFoundError
from app.providers.storage_local import LocalStorageProvider
from app.providers.storage_meizi import (
    MeiziObjectSnapshot,
    MeiziStorageProvider,
    SqlAlchemyMeiziObjectRepository,
    classify_storage_key,
)


def _settings(**overrides) -> Settings:
    values = {
        "_env_file": None,
        "app_env": "test",
        "cors_origins": ["https://app.example.com"],
        "session_secret": "test-only-session-secret-at-least-32-chars",
        "storage_driver": "meizi",
        "meizi_base_url": "https://mc.example.com/endpoint/",
        "meizi_upload_user_name": "upload-user",
        "meizi_upload_tenant_id": 101679,
        "meizi_upload_group_id": 100000000279,
        "meizi_upload_source": 1001,
        "meizi_upload_salt": "upload-secret",
        "meizi_front_user_name": "",
        "meizi_front_tenant_id": 101679,
        "meizi_front_group_id": 0,
        "meizi_front_source": 1001,
        "meizi_front_salt": "front-secret",
        "meizi_processing_poll_attempts": 2,
        "meizi_processing_poll_interval_seconds": 0,
        "volc_image_audit_public_base_url": "https://api.example.com",
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize(
    ("media_kind", "extension", "mime_type", "lib_type"),
    [
        ("img", ".png", "image/png", 703),
        ("audio", ".mp3", "audio/mpeg", 702),
        ("video", ".mp4", "video/mp4", 701),
        ("file", ".pdf", "application/pdf", 704),
    ],
)
async def test_meizi_client_uses_signed_three_step_protocol_for_all_media_kinds(
    media_kind: MeiziMediaKind,
    extension: str,
    mime_type: str,
    lib_type: int,
) -> None:
    config = _settings()
    requests: list[httpx.Request] = []
    request_id = "123456789012345678"
    timestamp = 1785312000123

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith(f"/{media_kind}/uploadToken"):
            token = {
                "id": 123,
                "videoId": 456,
                "cloudType": 1,
                "key": "remote-key",
                "token": "temporary-upload-token",
                "uploadUrl": "https://upload.example.com/object",
                "coverUrl": "https://cdn.example.com/media/item",
            }
            return httpx.Response(
                200,
                json={"code": 0, "data": {"uploadToken": token}},
                request=request,
            )
        if request.url.host == "upload.example.com":
            return httpx.Response(200, request=request)
        if request.url.path.endswith(f"/{media_kind}/complete"):
            return httpx.Response(200, json={"code": 0}, request=request)
        if request.url.path.endswith("/video/info"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "status": 1,
                        "videoUrl": "https://cdn.example.com/media/video.mp4",
                    },
                },
                request=request,
            )
        raise AssertionError(f"出现未预期的请求: {request.method} {request.url.path}")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        trust_env=False,
    ) as http:
        client = MeiziFileCenterClient(
            config=config,
            http_client=http,
            request_id_factory=lambda: request_id,
            clock_ms=lambda: timestamp,
        )
        result = await client.upload_bytes(
            filename=f"asset{extension}",
            data=b"media-payload",
            mime_type=mime_type,
            media_kind=media_kind,
        )

    token_request = requests[0]
    token_form = parse_qs(token_request.content.decode())
    expected_token = hashlib.md5(
        (
            f"{request_id}:{timestamp}:101679:100000000279:"
            "upload-user:1001:upload-secret"
        ).encode(),
        usedforsecurity=False,
    ).hexdigest()
    assert token_request.url.path == f"/endpoint/{media_kind}/uploadToken"
    assert token_form["requestId"] == [request_id]
    assert token_form["timeStamp"] == [str(timestamp)]
    assert token_form["libType"] == [str(lib_type)]
    assert token_form["token"] == [expected_token]
    assert "upload-secret" not in token_request.content.decode()

    object_request = requests[1]
    object_body = object_request.content
    assert object_request.url == httpx.URL("https://upload.example.com/object")
    assert b'name="key"' in object_body
    assert b'name="token"' in object_body
    assert b'name="file"; filename="file"' in object_body
    assert b"media-payload" in object_body

    complete_request = requests[2]
    assert complete_request.url.path == f"/endpoint/{media_kind}/complete"
    complete_query = dict(complete_request.url.params.multi_items())
    assert complete_query["requestId"] == request_id
    assert complete_query["token"] == expected_token
    assert complete_query["id"] == "123"
    assert complete_query["sync"] == "1"
    assert complete_request.content == b"[]"
    assert result.media_id == 123
    assert result.info_id == (456 if media_kind == "video" else 123)
    assert result.lib_type == lib_type
    assert result.request_id == request_id
    assert result.public_url.startswith("https://cdn.example.com/")


async def test_meizi_client_rejects_complete_business_error_and_compensates() -> None:
    deleted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal deleted
        if request.url.path.endswith("/img/uploadToken"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "uploadToken": {
                            "id": 123,
                            "cloudType": 1,
                            "key": "remote-key",
                            "token": "temporary-upload-token",
                            "uploadUrl": "https://upload.example.com/object",
                            "coverUrl": "https://cdn.example.com/media/item.png",
                        }
                    },
                },
                request=request,
            )
        if request.url.host == "upload.example.com":
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/img/complete"):
            return httpx.Response(
                200,
                json={"code": 10300, "message": "服务器处理失败"},
                request=request,
            )
        if request.url.path.endswith("/media/shiftDelete"):
            deleted = True
            return httpx.Response(200, json={"code": 0}, request=request)
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            request_id_factory=lambda: "100",
            clock_ms=lambda: 200,
        )
        with pytest.raises(MeiziClientError, match="meizi_complete_rejected"):
            await client.upload_bytes(
                filename="a.png",
                data=b"png",
                mime_type="image/png",
                media_kind="img",
            )

    assert deleted is True


@pytest.mark.parametrize("status_code", [302, 500])
async def test_meizi_client_rejects_empty_complete_http_error_and_compensates(
    status_code: int,
) -> None:
    deleted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal deleted
        if request.url.path.endswith("/img/uploadToken"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "uploadToken": {
                            "id": 123,
                            "cloudType": 1,
                            "key": "remote-key",
                            "token": "temporary-upload-token",
                            "uploadUrl": "https://upload.example.com/object",
                            "coverUrl": "https://cdn.example.com/media/item.png",
                        }
                    },
                },
                request=request,
            )
        if request.url.host == "upload.example.com":
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/img/complete"):
            return httpx.Response(status_code, request=request)
        if request.url.path.endswith("/media/shiftDelete"):
            deleted = True
            return httpx.Response(200, json={"code": 0}, request=request)
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            request_id_factory=lambda: "100",
            clock_ms=lambda: 200,
        )
        with pytest.raises(MeiziClientError, match="meizi_complete_failed"):
            await client.upload_bytes(
                filename="a.png",
                data=b"png",
                mime_type="image/png",
                media_kind="img",
            )

    assert deleted is True


async def test_meizi_client_never_uses_cover_url_when_video_info_times_out() -> None:
    deleted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal deleted
        if request.url.path.endswith("/video/uploadToken"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "uploadToken": {
                            "id": 123,
                            "videoId": 456,
                            "cloudType": 1,
                            "key": "remote-key",
                            "token": "temporary-upload-token",
                            "uploadUrl": "https://upload.example.com/object",
                            "coverUrl": "https://cdn.example.com/media/cover.png",
                        }
                    },
                },
                request=request,
            )
        if request.url.host == "upload.example.com":
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/video/complete"):
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/video/info"):
            return httpx.Response(
                200,
                json={"code": 0, "data": {"status": 3}},
                request=request,
            )
        if request.url.path.endswith("/media/shiftDelete"):
            deleted = True
            return httpx.Response(200, json={"code": 0}, request=request)
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            request_id_factory=lambda: "100",
            clock_ms=lambda: 200,
        )
        with pytest.raises(MeiziClientError, match="meizi_media_processing_timeout"):
            await client.upload_bytes(
                filename="a.mp4",
                data=b"video",
                mime_type="video/mp4",
                media_kind="video",
            )

    assert deleted is True


@pytest.mark.parametrize("cloud_type", [1, 2])
async def test_meizi_client_builds_qiniu_and_aliyun_multipart_fields(
    cloud_type: int,
) -> None:
    upload_body = b""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal upload_body
        if request.url.path.endswith("/img/uploadToken"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "uploadToken": {
                            "id": 11,
                            "cloudType": cloud_type,
                            "key": "object-key",
                            "token": "signature-or-token",
                            "accessid": "temporary-access-id",
                            "policy": "temporary-policy",
                            "callback": "temporary-callback",
                            "uploadUrl": "https://upload.example.com/object",
                            "coverUrl": "https://cdn.example.com/item.png",
                        }
                    },
                },
                request=request,
            )
        if request.url.host == "upload.example.com":
            upload_body = request.content
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/img/complete"):
            return httpx.Response(200, request=request)
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            request_id_factory=lambda: "100",
            clock_ms=lambda: 200,
        ).upload_bytes(
            filename="a.png",
            data=b"png",
            mime_type="image/png",
            media_kind="img",
        )

    if cloud_type == 1:
        assert b'name="token"' in upload_body
        assert b'name="OSSAccessKeyId"' not in upload_body
        assert b'name="policy"' not in upload_body
    else:
        assert b'name="OSSAccessKeyId"' in upload_body
        assert b'name="policy"' in upload_body
        assert b'name="Signature"' in upload_body
        assert b'name="callback"' in upload_body


async def test_meizi_client_persists_identity_and_compensates_failed_direct_upload() -> None:
    calls: list[str] = []
    identities: list[MeiziUploadIdentity] = []
    compensation: list[tuple[int, bool]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/audio/uploadToken"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "uploadToken": {
                            "id": 99,
                            "cloudType": 1,
                            "key": "key",
                            "token": "sensitive-token",
                            "uploadUrl": "https://upload.example.com/object",
                            "coverUrl": "https://cdn.example.com/item.mp3",
                        }
                    },
                },
                request=request,
            )
        if request.url.host == "upload.example.com":
            return httpx.Response(503, request=request)
        if request.url.path.endswith("/media/shiftDelete"):
            return httpx.Response(200, json={"code": 0, "data": None}, request=request)
        raise AssertionError(request.url)

    async def on_identity(identity: MeiziUploadIdentity) -> None:
        identities.append(identity)

    async def on_compensation(identity: MeiziUploadIdentity, deleted: bool) -> None:
        compensation.append((identity.media_id, deleted))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            request_id_factory=lambda: "123",
            clock_ms=lambda: 456,
        )
        with pytest.raises(MeiziClientError, match="meizi_object_upload_failed") as raised:
            await client.upload_bytes(
                filename="a.mp3",
                data=b"audio",
                mime_type="audio/mpeg",
                media_kind="audio",
                on_identity=on_identity,
                on_compensation=on_compensation,
            )

    assert identities[0].media_id == 99
    assert compensation == [(99, True)]
    assert calls[-1].endswith("/media/shiftDelete")
    assert not any(path.endswith("/audio/complete") for path in calls)
    assert "sensitive-token" not in str(raised.value)
    assert "front-secret" not in str(raised.value)


async def test_meizi_client_rejects_insecure_public_url_and_compensates() -> None:
    deleted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal deleted
        if request.url.path.endswith("/img/uploadToken"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "uploadToken": {
                            "id": 5,
                            "cloudType": 1,
                            "key": "key",
                            "token": "token",
                            "uploadUrl": "https://upload.example.com/object",
                            "coverUrl": "http://127.0.0.1/private.png",
                        }
                    },
                },
                request=request,
            )
        if request.url.host == "upload.example.com":
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/img/complete"):
            return httpx.Response(200, request=request)
        if request.url.path.endswith("/img/info"):
            return httpx.Response(
                200,
                json={"code": 0, "data": {"imgUrl": "http://127.0.0.1/private.png"}},
                request=request,
            )
        if request.url.path.endswith("/media/shiftDelete"):
            deleted = True
            return httpx.Response(200, json={"code": 0}, request=request)
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(MeiziClientError, match="meizi_url_not_allowed"):
            await MeiziFileCenterClient(
                config=_settings(),
                http_client=http,
                request_id_factory=lambda: "1",
                clock_ms=lambda: 2,
            ).upload_bytes(
                filename="a.png",
                data=b"image",
                mime_type="image/png",
                media_kind="img",
            )
    assert deleted is True


async def test_shift_delete_uses_front_uuid_signature_without_leaking_query_token() -> None:
    captured: httpx.Request | None = None
    request_id = "123e4567-e89b-12d3-a456-426614174000"
    timestamp = 1785312000123

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured
        captured = request
        return httpx.Response(
            500,
            json={"code": 10300, "message": "upstream-failed"},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            request_id_factory=lambda: "upload-id",
            front_request_id_factory=lambda: request_id,
            clock_ms=lambda: timestamp,
        )
        with pytest.raises(MeiziClientError) as raised:
            await client.shift_delete(media_id=123, lib_type=703)

    assert captured is not None
    query = dict(captured.url.params.multi_items())
    expected = hashlib.md5(
        (
            f"{request_id}:{timestamp}:101679:0:"
            ":1001:front-secret"
        ).encode(),
        usedforsecurity=False,
    ).hexdigest()
    assert captured.method == "POST"
    assert captured.url.path == "/endpoint/media/shiftDelete"
    assert query["requestId"] == request_id
    assert query["token"] == expected
    assert query["id"] == "123"
    assert query["libType"] == "703"
    assert captured.headers["Content-Type"] == "application/json"
    assert captured.content == b""
    formatted = "".join(
        traceback.format_exception(
            type(raised.value),
            raised.value,
            raised.value.__traceback__,
        )
    )
    assert expected not in formatted
    assert "token=" not in formatted
    assert "front-secret" not in formatted


@pytest.mark.parametrize(
    ("media_kind", "payload", "expected_url"),
    [
        ("img", {"imgUrl": "https://cdn.example.com/item.png"}, "https://cdn.example.com/item.png"),
        (
            "audio",
            {"maudio": {"url": "https://cdn.example.com/item.mp3"}},
            "https://cdn.example.com/item.mp3",
        ),
        (
            "video",
            {"status": 1, "mvideo": {"downloadUrl": "https://cdn.example.com/item.mp4"}},
            "https://cdn.example.com/item.mp4",
        ),
        (
            "file",
            {"mfile": {"downloadUrl": "https://cdn.example.com/item.pdf"}},
            "https://cdn.example.com/item.pdf",
        ),
    ],
)
async def test_meizi_client_resolves_info_url_for_all_media_kinds(
    media_kind: MeiziMediaKind,
    payload: dict[str, object],
    expected_url: str,
) -> None:
    captured: httpx.Request | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured
        captured = request
        return httpx.Response(
            200,
            json={"code": 0, "data": payload},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = MeiziFileCenterClient(
            config=_settings(),
            http_client=http,
            front_request_id_factory=lambda: "123e4567-e89b-12d3-a456-426614174000",
            clock_ms=lambda: 1785312000123,
        )
        result = await client.resolve_public_url(media_kind=media_kind, info_id=456)

    assert result == expected_url
    assert captured is not None
    assert captured.method == "GET"
    assert captured.url.path == f"/endpoint/{media_kind}/info"
    assert captured.url.params["id"] == "456"


def test_meizi_settings_validate_secrets_urls_and_lib_types() -> None:
    config = _settings()
    assert config.meizi_base_url == "https://mc.example.com/endpoint"
    assert config.meizi_legacy_local_fallback is False
    assert "upload-secret" not in repr(config)
    assert config.meizi_upload_salt.get_secret_value() == "upload-secret"

    with pytest.raises(ValidationError, match="MEIZI_UPLOAD_SALT"):
        _settings(meizi_upload_salt="")
    with pytest.raises(ValidationError, match="必须互不相同"):
        _settings(meizi_lib_type_image=701)
    with pytest.raises(ValidationError, match="必须使用 HTTPS"):
        _settings(meizi_base_url="http://localhost:8000/endpoint")
    with pytest.raises(ValidationError, match="非法主机"):
        _settings(meizi_public_url_allowed_hosts=["*.example.com"])
    without_audit_proxy = _settings(
        volc_image_audit_public_base_url="",
    )
    assert without_audit_proxy.volc_image_audit_public_base_url == ""


def test_storage_factory_selects_meizi_without_falling_back_to_local(
    monkeypatch,
) -> None:
    from app.providers import storage as storage_module

    monkeypatch.setattr(storage_module.settings, "storage_driver", "meizi")
    monkeypatch.setattr(storage_module, "_storage_singleton", None)
    provider = storage_module.get_storage()
    assert isinstance(provider, MeiziStorageProvider)


async def test_meizi_storage_canary_uploads_reads_and_deletes() -> None:
    run_canary = getattr(
        storage_meizi_module,
        "run_meizi_storage_canary",
        None,
    )
    assert callable(run_canary)

    class _CanaryStorage:
        def __init__(self) -> None:
            self.payload: bytes | None = None
            self.calls: list[str] = []

        async def put(self, key: str, data: bytes) -> str:
            self.calls.append("put")
            assert key == "generate_images/_system/meizi-storage-canary.png"
            self.payload = data
            return key

        async def get_size(self, _key: str) -> int:
            self.calls.append("get_size")
            return len(self.payload or b"")

        async def get(self, _key: str) -> bytes:
            self.calls.append("get")
            return self.payload or b""

        async def delete(self, _key: str) -> None:
            self.calls.append("delete")
            self.payload = None

        async def exists(self, _key: str) -> bool:
            self.calls.append("exists")
            return self.payload is not None

    storage = _CanaryStorage()
    result = await run_canary(storage=storage)

    assert result["size_bytes"] > 0
    assert len(result["sha256"]) == 64
    assert storage.calls == ["put", "get_size", "get", "delete", "exists"]


async def test_meizi_storage_canary_cleans_up_after_key_contract_mismatch() -> None:
    run_canary = storage_meizi_module.run_meizi_storage_canary

    class _WrongKeyStorage:
        def __init__(self) -> None:
            self.deleted = False

        async def put(self, _key: str, _data: bytes) -> str:
            return "generate_images/_system/unexpected.png"

        async def delete(self, key: str) -> None:
            assert key == "generate_images/_system/meizi-storage-canary.png"
            self.deleted = True

    storage = _WrongKeyStorage()
    with pytest.raises(RuntimeError, match="meizi_canary_storage_key_mismatch"):
        await run_canary(storage=storage)  # type: ignore[arg-type]

    assert storage.deleted is True


@pytest.mark.parametrize(
    ("storage_key", "expected"),
    [
        ("generate_images/m/a.png", ("img", "image/png", "a.png")),
        ("upload_images/m/a.mp3", ("audio", "audio/mpeg", "a.mp3")),
        (
            "generate_images/video/a.mp4.upload-attempt-0123456789abcdef0123456789abcdef.tmp",
            ("video", "video/mp4", "a.mp4"),
        ),
        (
            "generate_outputs/script/video.attempt-2-0123456789abcdef.mp4.tmp",
            ("video", "video/mp4", "video.attempt-2-0123456789abcdef.mp4"),
        ),
        ("upload_scripts/script/a.pdf", ("file", "application/pdf", "a.pdf")),
    ],
)
def test_classify_storage_key_handles_business_and_temporary_keys(
    storage_key: str,
    expected: tuple[str, str, str],
) -> None:
    assert classify_storage_key(storage_key) == expected


class _MemoryRepository:
    def __init__(self) -> None:
        self.rows: dict[str, MeiziObjectSnapshot] = {}

    async def get(self, storage_key: str) -> MeiziObjectSnapshot | None:
        return self.rows.get(storage_key)

    async def reserve_upload(
        self,
        *,
        storage_key: str,
        identity: MeiziUploadIdentity,
        mime_type: str,
        size_bytes: int,
        sha256: str,
        delete_namespace: str,
    ) -> None:
        current = self.rows.get(storage_key)
        if current is not None and current.status != "deleted":
            raise StorageObjectConflictError(storage_key)
        self.rows[storage_key] = MeiziObjectSnapshot(
            storage_key=storage_key,
            media_id=identity.media_id,
            info_id=identity.info_id,
            lib_type=identity.lib_type,
            media_kind=identity.media_kind,
            request_id=identity.request_id,
            public_url=None,
            mime_type=mime_type,
            size_bytes=size_bytes,
            sha256=sha256,
            delete_namespace=delete_namespace,
            status="uploading",
        )

    async def activate(self, *, storage_key: str, result: MeiziUploadResult) -> None:
        row = self.rows[storage_key]
        assert row.media_id == result.media_id
        self.rows[storage_key] = replace(
            row,
            public_url=result.public_url,
            status="active",
        )

    async def mark_compensation(
        self,
        *,
        storage_key: str,
        identity: MeiziUploadIdentity,
        deleted: bool,
    ) -> None:
        row = self.rows.get(storage_key)
        if row is not None and row.media_id == identity.media_id:
            self.rows[storage_key] = replace(
                row,
                status="deleted" if deleted else "deleting",
            )

    async def mark_cleanup_pending(
        self,
        *,
        storage_key: str,
        media_id: int,
    ) -> None:
        row = self.rows.get(storage_key)
        if row is not None and row.media_id == media_id:
            self.rows[storage_key] = replace(row, status="cleanup_pending")

    async def begin_delete(self, storage_key: str) -> MeiziObjectSnapshot | None:
        row = self.rows.get(storage_key)
        if row is None or row.status == "deleted":
            return None
        row = replace(row, status="deleting")
        self.rows[storage_key] = row
        return row

    async def mark_deleted(self, *, storage_key: str, media_id: int) -> None:
        row = self.rows.get(storage_key)
        if row is not None and row.media_id == media_id:
            self.rows[storage_key] = replace(row, status="deleted")

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        target = self.rows.get(final_key)
        if target is not None and target.status != "deleted":
            return False
        source = self.rows.get(source_key)
        if source is None or source.status != "active":
            raise StorageObjectNotFoundError(source_key)
        self.rows[final_key] = replace(source, storage_key=final_key)
        del self.rows[source_key]
        return True

    async def claim_stale_cleanup(
        self,
        *,
        stale_before,
        limit: int,
    ) -> list[MeiziObjectSnapshot]:
        del stale_before
        claimed = [
            row
            for row in self.rows.values()
            if row.status in {"uploading", "deleting", "cleanup_pending"}
        ][:limit]
        for row in claimed:
            if row.status != "cleanup_pending":
                self.rows[row.storage_key] = replace(row, status="deleting")
        return claimed


class _FakeMeiziClient:
    def __init__(self) -> None:
        self.next_id = 1
        self.objects: dict[int, bytes] = {}
        self.deleted: list[tuple[int, int]] = []
        self.upload_calls = 0

    @staticmethod
    def _lib_type(media_kind: MeiziMediaKind) -> int:
        return {"img": 703, "audio": 702, "video": 701, "file": 704}[media_kind]

    async def upload_bytes(
        self,
        *,
        filename: str,
        data: bytes,
        mime_type: str,
        media_kind: MeiziMediaKind,
        on_identity,
        on_compensation,
    ) -> MeiziUploadResult:
        del filename, mime_type, on_compensation
        return await self._upload(data, media_kind, on_identity)

    async def upload_path(
        self,
        *,
        filename: str,
        path: str,
        mime_type: str,
        media_kind: MeiziMediaKind,
        on_identity,
        on_compensation,
    ) -> MeiziUploadResult:
        del filename, mime_type, on_compensation
        return await self._upload(Path(path).read_bytes(), media_kind, on_identity)

    async def _upload(self, data: bytes, media_kind: MeiziMediaKind, on_identity) -> MeiziUploadResult:
        media_id = self.next_id
        self.next_id += 1
        self.upload_calls += 1
        identity = MeiziUploadIdentity(
            media_id=media_id,
            info_id=media_id,
            lib_type=self._lib_type(media_kind),
            media_kind=media_kind,
            request_id=f"request-{media_id}",
        )
        await on_identity(identity)
        self.objects[media_id] = data
        return MeiziUploadResult(
            media_id=media_id,
            info_id=media_id,
            lib_type=identity.lib_type,
            media_kind=media_kind,
            public_url=f"https://cdn.example.com/media/{media_id}",
            request_id=identity.request_id,
        )

    async def shift_delete(self, *, media_id: int, lib_type: int) -> None:
        self.deleted.append((media_id, lib_type))
        self.objects.pop(media_id, None)


async def test_meizi_storage_provider_upload_read_promote_and_delete_contract(
    tmp_path: Path,
) -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()

    async def resolve_public_host(host: str, port: int) -> list[str]:
        assert host == "cdn.example.com"
        assert port == 443
        return ["93.184.216.34"]

    def download(request: httpx.Request) -> httpx.Response:
        media_id = int(request.url.path.rsplit("/", 1)[1])
        payload = meizi.objects[media_id]
        range_value = request.headers["range"].removeprefix("bytes=")
        start_text, end_text = range_value.split("-", 1)
        start, end = int(start_text), int(end_text)
        return httpx.Response(
            206,
            content=payload[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(payload)}"},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(download)) as http:
        provider = MeiziStorageProvider(
            config=_settings(),
            client=meizi,  # type: ignore[arg-type]
            repository=repository,
            download_client=http,
            public_url_resolver=resolve_public_host,
        )
        source = "upload_images/_tmp/material/audio.mp3"
        final = "upload_images/material/audio.mp3"
        payload = b"0123456789"
        source_file = tmp_path / "audio.mp3"
        source_file.write_bytes(payload)

        assert await provider.put_file(source, str(source_file)) == source
        assert await provider.put_file(source, str(source_file)) == source
        assert meizi.upload_calls == 1
        assert await provider.exists(source) is True
        assert await provider.get_size(source) == len(payload)
        assert await provider.signed_url(source, expires_seconds=600) is None
        assert await provider.external_read_url(
            source,
            expires_seconds=600,
        ) == "https://cdn.example.com/media/1"
        assert await provider.get(source) == payload
        assert b"".join(
            [chunk async for chunk in provider.iter_chunks(source, start=2, end=6)]
        ) == b"23456"

        assert await provider.promote_if_absent(source, final) is True
        assert await provider.exists(source) is False
        assert await provider.exists(final) is True
        await provider.delete(final)
        await provider.delete(final)
        assert await provider.exists(final) is False
        assert meizi.deleted == [(1, 702)]
        assert repository.rows[final].status == "deleted"


async def test_meizi_external_read_url_revalidates_saved_public_url() -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    provider = MeiziStorageProvider(
        config=_settings(meizi_public_url_allowed_hosts=["assets.example.com"]),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
    )
    key = "generate_images/material/image.png"

    await provider.put(key, b"image")

    with pytest.raises(RuntimeError, match="meizi_media_url_invalid"):
        await provider.external_read_url(key, expires_seconds=600)


async def test_meizi_external_read_url_rejects_private_dns_answers() -> None:
    async def resolve_private_host(host: str, port: int) -> list[str]:
        assert host == "cdn.example.com"
        assert port == 443
        return ["127.0.0.1"]

    repository = _MemoryRepository()
    provider = MeiziStorageProvider(
        config=_settings(meizi_public_url_allowed_hosts=["cdn.example.com"]),
        client=_FakeMeiziClient(),  # type: ignore[arg-type]
        repository=repository,
        public_url_resolver=resolve_private_host,
    )
    key = "generate_images/material/private-dns.png"

    await provider.put(key, b"image")

    with pytest.raises(RuntimeError, match="meizi_media_url_invalid"):
        await provider.external_read_url(key, expires_seconds=600)


async def test_meizi_persists_losing_concurrent_upload_when_compensation_fails() -> None:
    repository = _MemoryRepository()
    key = "generate_images/material/concurrent.png"
    namespace = storage_meizi_module.meizi_delete_namespace(_settings())

    class _ConcurrentLoserClient:
        async def upload_bytes(
            self,
            *,
            filename: str,
            data: bytes,
            mime_type: str,
            media_kind: MeiziMediaKind,
            on_identity,
            on_compensation,
        ) -> MeiziUploadResult:
            del filename, data, mime_type
            identity = MeiziUploadIdentity(
                media_id=2,
                info_id=2,
                lib_type=703,
                media_kind=media_kind,
                request_id="losing-request",
            )
            repository.rows[key] = MeiziObjectSnapshot(
                storage_key=key,
                media_id=1,
                info_id=1,
                lib_type=703,
                media_kind="img",
                request_id="winning-request",
                public_url="https://cdn.example.com/media/1",
                mime_type="image/png",
                size_bytes=6,
                sha256=hashlib.sha256(b"winner").hexdigest(),
                delete_namespace=namespace,
                status="active",
            )
            try:
                await on_identity(identity)
            except Exception:
                # 等价真实客户端 shiftDelete 失败后的持久补偿回调。
                await on_compensation(identity, False)
                raise
            raise AssertionError("并发输家不得继续直传")

    provider = MeiziStorageProvider(
        config=_settings(),
        client=_ConcurrentLoserClient(),  # type: ignore[arg-type]
        repository=repository,
    )

    with pytest.raises(StorageObjectConflictError):
        await provider.put(key, b"loser")

    assert repository.rows[key].media_id == 1
    assert repository.rows[key].status == "active"
    cleanup_debts = [
        row for row in repository.rows.values() if row.media_id == 2
    ]
    assert len(cleanup_debts) == 1
    assert cleanup_debts[0].storage_key != key
    assert cleanup_debts[0].status == "deleting"
    assert cleanup_debts[0].delete_namespace == namespace


async def test_meizi_delete_namespace_blocks_rotated_front_identity() -> None:
    repository = _MemoryRepository()
    original_client = _FakeMeiziClient()
    original_provider = MeiziStorageProvider(
        config=_settings(),
        client=original_client,  # type: ignore[arg-type]
        repository=repository,
    )
    key = "generate_images/material/delete-namespace.png"

    await original_provider.put(key, b"image")

    namespace = repository.rows[key].delete_namespace
    assert len(namespace) == 64
    assert namespace.isascii()
    assert all(character in "0123456789abcdef" for character in namespace)

    rotated_client = _FakeMeiziClient()
    rotated_provider = MeiziStorageProvider(
        config=_settings(meizi_front_tenant_id=101680),
        client=rotated_client,  # type: ignore[arg-type]
        repository=repository,
    )
    error_type = getattr(
        storage_meizi_module,
        "MeiziDeleteNamespaceMismatchError",
        RuntimeError,
    )

    with pytest.raises(error_type, match="meizi_delete_namespace_mismatch"):
        await rotated_provider.delete(key)

    assert rotated_client.deleted == []
    assert repository.rows[key].status == "active"


def test_meizi_delete_namespace_binds_remote_endpoint() -> None:
    production = storage_meizi_module.meizi_delete_namespace(
        _settings(meizi_base_url="https://mc.example.com/endpoint")
    )
    testing = storage_meizi_module.meizi_delete_namespace(
        _settings(meizi_base_url="https://testmc.example.com/endpoint")
    )

    assert production != testing


async def test_meizi_storage_provider_preserves_source_on_final_conflict() -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    provider = MeiziStorageProvider(
        config=_settings(),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
    )
    source = "generate_images/material/tmp/source.png"
    final = "generate_images/material/final.png"
    await provider.put(source, b"source")
    await provider.put(final, b"target")

    assert await provider.promote_if_absent(source, final) is False
    assert await provider.exists(source) is True
    with pytest.raises(StorageObjectConflictError):
        await provider.promote(source, final)
    assert await provider.exists(source) is True
    assert meizi.deleted == []


async def test_meizi_legacy_local_fallback_routes_only_unmapped_objects(
    tmp_path: Path,
) -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    legacy = LocalStorageProvider(root=str(tmp_path / "legacy"))

    def download(request: httpx.Request) -> httpx.Response:
        media_id = int(request.url.path.rsplit("/", 1)[1])
        payload = meizi.objects[media_id]
        start_text, end_text = request.headers["range"].removeprefix("bytes=").split(
            "-",
            1,
        )
        start, end = int(start_text), int(end_text)
        return httpx.Response(
            206,
            content=payload[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(payload)}"},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(download)) as http:
        provider = MeiziStorageProvider(
            config=_settings(),
            client=meizi,  # type: ignore[arg-type]
            repository=repository,
            download_client=http,
            legacy_provider=legacy,
        )
        legacy_key = "upload_images/legacy/old.png"
        await legacy.put(legacy_key, b"legacy")

        assert await provider.exists(legacy_key) is True
        assert await provider.get(legacy_key) == b"legacy"
        assert await provider.get_size(legacy_key) == 6
        assert b"".join(
            [chunk async for chunk in provider.iter_chunks(legacy_key, start=1, end=3)]
        ) == b"ega"
        assert (await provider.signed_url(legacy_key) or "").endswith(legacy_key)

        await provider.delete(legacy_key)
        assert await legacy.exists(legacy_key) is False
        assert meizi.deleted == []

        remote_key = "generate_images/material/new.png"
        await legacy.put(remote_key, b"stale-local-copy")
        await provider.put(remote_key, b"remote")

        assert await legacy.exists(remote_key) is False
        assert await provider.get(remote_key) == b"remote"
        await provider.delete(remote_key)
        assert meizi.deleted == [(1, 703)]


async def test_meizi_legacy_fallback_cleans_deleted_mapping_without_resurrection(
    tmp_path: Path,
) -> None:
    repository = _MemoryRepository()
    legacy = LocalStorageProvider(root=str(tmp_path / "legacy"))
    key = "generate_images/material/deleted.png"
    await legacy.put(key, b"stale")
    repository.rows[key] = MeiziObjectSnapshot(
        storage_key=key,
        media_id=8,
        info_id=8,
        lib_type=703,
        media_kind="img",
        request_id="request-8",
        public_url="https://cdn.example.com/media/8",
        mime_type="image/png",
        size_bytes=5,
        sha256=hashlib.sha256(b"stale").hexdigest(),
        delete_namespace=storage_meizi_module.meizi_delete_namespace(_settings()),
        status="deleted",
    )
    provider = MeiziStorageProvider(
        config=_settings(),
        client=_FakeMeiziClient(),  # type: ignore[arg-type]
        repository=repository,
        legacy_provider=legacy,
    )

    assert await provider.exists(key) is False
    assert await provider.signed_url(key) is None
    with pytest.raises(StorageObjectNotFoundError):
        await provider.get(key)
    await provider.delete(key)
    assert await legacy.exists(key) is False


async def test_meizi_remote_delete_removes_same_key_legacy_copy(
    tmp_path: Path,
) -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    legacy = LocalStorageProvider(root=str(tmp_path / "legacy"))
    provider = MeiziStorageProvider(
        config=_settings(),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
        legacy_provider=legacy,
    )
    key = "generate_images/material/delete-both.png"
    await provider.put(key, b"remote")
    await legacy.put(key, b"stale-local-copy")

    await provider.delete(key)

    assert repository.rows[key].status == "deleted"
    assert await legacy.exists(key) is False


async def test_meizi_delete_retries_legacy_cleanup_without_repeating_remote_delete(
    tmp_path: Path,
) -> None:
    class _FailOnceLegacyStorage(LocalStorageProvider):
        def __init__(self, root: str) -> None:
            super().__init__(root)
            self.delete_calls = 0

        async def delete(self, key: str) -> None:
            self.delete_calls += 1
            if self.delete_calls == 1:
                raise OSError("simulated legacy cleanup failure")
            await super().delete(key)

    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    legacy = _FailOnceLegacyStorage(root=str(tmp_path / "legacy"))
    key = "generate_images/material/retry-local-cleanup.png"
    payload = b"stale-local-copy"
    await legacy.put(key, payload)
    repository.rows[key] = MeiziObjectSnapshot(
        storage_key=key,
        media_id=9,
        info_id=9,
        lib_type=703,
        media_kind="img",
        request_id="request-9",
        public_url="https://cdn.example.com/media/9",
        mime_type="image/png",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        delete_namespace=storage_meizi_module.meizi_delete_namespace(_settings()),
        status="active",
    )
    provider = MeiziStorageProvider(
        config=_settings(),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
        legacy_provider=legacy,
    )

    with pytest.raises(OSError, match="simulated legacy cleanup failure"):
        await provider.delete(key)

    assert repository.rows[key].status == "cleanup_pending"
    assert await legacy.exists(key) is True
    assert meizi.deleted == [(9, 703)]

    await provider.delete(key)

    assert repository.rows[key].status == "deleted"
    assert await legacy.exists(key) is False
    assert legacy.delete_calls == 2
    assert meizi.deleted == [(9, 703)]


async def test_meizi_idempotent_promote_removes_legacy_source_for_remote_target(
    tmp_path: Path,
) -> None:
    repository = _MemoryRepository()
    legacy = LocalStorageProvider(root=str(tmp_path / "legacy"))
    provider = MeiziStorageProvider(
        config=_settings(),
        client=_FakeMeiziClient(),  # type: ignore[arg-type]
        repository=repository,
        legacy_provider=legacy,
    )
    source = "generate_images/material/pending-local.png"
    final = "generate_images/material/already-remote.png"
    await provider.put(final, b"remote")
    await legacy.put(source, b"pending-local")

    assert await provider.promote(source, final) == final
    assert await legacy.exists(source) is False


async def test_meizi_legacy_fallback_handles_pending_promote_without_overwrite(
    tmp_path: Path,
) -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    legacy = LocalStorageProvider(root=str(tmp_path / "legacy"))
    provider = MeiziStorageProvider(
        config=_settings(),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
        legacy_provider=legacy,
    )

    local_source = "upload_images/_tmp/legacy.png"
    local_final = "upload_images/material/legacy.png"
    await legacy.put(local_source, b"pending")
    assert await provider.promote_if_absent(local_source, local_final) is True
    assert await legacy.exists(local_source) is False
    assert await provider.get(local_final) == b"pending"

    remote_source = "generate_images/material/remote.tmp.png"
    occupied_final = "generate_images/material/occupied.png"
    await provider.put(remote_source, b"remote")
    await legacy.put(occupied_final, b"existing")

    assert await provider.promote_if_absent(remote_source, occupied_final) is False
    assert await provider.exists(remote_source) is True
    assert await legacy.get(occupied_final) == b"existing"

    assert await provider.promote(remote_source, occupied_final) == occupied_final
    assert await legacy.exists(occupied_final) is False
    assert await provider.exists(occupied_final) is True


async def test_meizi_reconciles_stale_upload_and_retries_failed_delete() -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()
    provider = MeiziStorageProvider(
        config=_settings(meizi_cleanup_stale_seconds=60),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
    )
    success_key = "generate_images/material/orphan.png"
    await provider.put(success_key, b"orphan")
    repository.rows[success_key] = replace(
        repository.rows[success_key],
        status="uploading",
    )

    assert await provider.reconcile_stale_deletions() == {
        "claimed": 1,
        "deleted": 1,
        "failed": 0,
    }
    assert repository.rows[success_key].status == "deleted"
    assert meizi.deleted == [(1, 703)]

    class _FailingDeleteClient(_FakeMeiziClient):
        async def shift_delete(self, *, media_id: int, lib_type: int) -> None:
            del media_id, lib_type
            raise MeiziClientError("temporary-delete-failure")

    failing = _FailingDeleteClient()
    failing_provider = MeiziStorageProvider(
        config=_settings(meizi_cleanup_stale_seconds=60),
        client=failing,  # type: ignore[arg-type]
        repository=repository,
    )
    failed_key = "upload_images/material/orphan.mp3"
    repository.rows[failed_key] = MeiziObjectSnapshot(
        storage_key=failed_key,
        media_id=9,
        info_id=9,
        lib_type=702,
        media_kind="audio",
        request_id="request-9",
        public_url=None,
        mime_type="audio/mpeg",
        size_bytes=10,
        sha256=hashlib.sha256(b"incomplete").hexdigest(),
        delete_namespace=storage_meizi_module.meizi_delete_namespace(_settings()),
        status="deleting",
    )

    assert await failing_provider.reconcile_stale_deletions() == {
        "claimed": 1,
        "deleted": 0,
        "failed": 1,
    }
    assert repository.rows[failed_key].status == "deleting"


async def test_meizi_reconciler_retries_legacy_cleanup_without_remote_redelete() -> None:
    repository = _MemoryRepository()
    meizi = _FakeMeiziClient()

    class _FailOnceLegacy:
        def __init__(self) -> None:
            self.calls = 0
            self.failures = 0

        async def delete(self, _storage_key: str) -> None:
            self.calls += 1
            if self.failures:
                self.failures -= 1
                raise OSError("legacy-delete-failed")

    legacy = _FailOnceLegacy()
    provider = MeiziStorageProvider(
        config=_settings(meizi_cleanup_stale_seconds=60),
        client=meizi,  # type: ignore[arg-type]
        repository=repository,
        legacy_provider=legacy,  # type: ignore[arg-type]
    )
    key = "generate_images/material/legacy-cleanup.png"
    await provider.put(key, b"remote")
    repository.rows[key] = replace(repository.rows[key], status="deleting")
    legacy.failures = 1

    assert await provider.reconcile_stale_deletions() == {
        "claimed": 1,
        "deleted": 0,
        "failed": 1,
    }
    assert repository.rows[key].status == "cleanup_pending"
    assert meizi.deleted == [(1, 703)]

    assert await provider.reconcile_stale_deletions() == {
        "claimed": 1,
        "deleted": 1,
        "failed": 0,
    }
    assert repository.rows[key].status == "deleted"
    assert meizi.deleted == [(1, 703)]


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过 MeiZi 晋升并发测试",
)
async def test_postgres_meizi_promote_if_absent_serializes_missing_final_key() -> None:
    database_url = os.environ["MTV_TEST_POSTGRES_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(database_url)
    regular_sessions = async_sessionmaker(engine, expire_on_commit=False)
    suffix = uuid4().hex
    source_keys = [
        f"generate_images/concurrent/{suffix}-a.tmp.png",
        f"generate_images/concurrent/{suffix}-b.tmp.png",
    ]
    final_key = f"generate_images/concurrent/{suffix}.png"
    all_keys = [*source_keys, final_key]
    media_id = uuid4().int & ((1 << 62) - 1)

    class _QueryRendezvous:
        def __init__(self) -> None:
            self.arrivals = 0
            self.both_arrived = asyncio.Event()

        async def wait(self) -> None:
            self.arrivals += 1
            if self.arrivals >= 2:
                self.both_arrived.set()
            try:
                await asyncio.wait_for(self.both_arrived.wait(), timeout=0.25)
            except TimeoutError:
                pass

    rendezvous = _QueryRendezvous()

    class _RendezvousSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            result = await super().execute(statement, *args, **kwargs)
            rendered = str(statement)
            if "FROM meizi_storage_objects" in rendered and "FOR UPDATE" in rendered:
                await rendezvous.wait()
            return result

    concurrent_sessions = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=_RendezvousSession,
    )
    repository = SqlAlchemyMeiziObjectRepository(concurrent_sessions)
    try:
        async with regular_sessions() as session:
            session.add_all(
                [
                    MeiziStorageObject(
                        storage_key=source_key,
                        media_id=media_id + index,
                        info_id=media_id + index,
                        lib_type=703,
                        media_kind="img",
                        request_id=f"request-{suffix}-{index}",
                        public_url=f"https://cdn.example.com/{suffix}-{index}.png",
                        mime_type="image/png",
                        size_bytes=1,
                        sha256=hashlib.sha256(bytes([index])).hexdigest(),
                        delete_namespace=storage_meizi_module.meizi_delete_namespace(
                            _settings()
                        ),
                        status="active",
                    )
                    for index, source_key in enumerate(source_keys, start=1)
                ]
            )
            await session.commit()

        outcomes = await asyncio.gather(
            *[
                repository.promote_if_absent(source_key, final_key)
                for source_key in source_keys
            ],
            return_exceptions=True,
        )

        assert all(isinstance(outcome, bool) for outcome in outcomes)
        assert sum(outcome is True for outcome in outcomes) == 1
        assert sum(outcome is False for outcome in outcomes) == 1
        async with regular_sessions() as session:
            rows = list(
                (
                    await session.execute(
                        select(MeiziStorageObject).where(
                            MeiziStorageObject.storage_key.in_(all_keys)
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert sum(row.storage_key == final_key for row in rows) == 1
        assert sum(row.storage_key in source_keys for row in rows) == 1
    finally:
        async with regular_sessions() as session:
            await session.execute(
                delete(MeiziStorageObject).where(
                    MeiziStorageObject.storage_key.in_(all_keys)
                )
            )
            await session.commit()
        await engine.dispose()


async def test_meizi_storage_read_error_does_not_leak_signed_public_url() -> None:
    repository = _MemoryRepository()
    key = "generate_images/material/private.png"
    secret_query = "temporary-download-signature"
    repository.rows[key] = MeiziObjectSnapshot(
        storage_key=key,
        media_id=7,
        info_id=7,
        lib_type=703,
        media_kind="img",
        request_id="request-7",
        public_url=f"https://cdn.example.com/media/7?token={secret_query}",
        mime_type="image/png",
        size_bytes=4,
        sha256=hashlib.sha256(b"data").hexdigest(),
        delete_namespace=storage_meizi_module.meizi_delete_namespace(_settings()),
        status="active",
    )

    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connect-failed", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as http:
        provider = MeiziStorageProvider(
            config=_settings(),
            client=_FakeMeiziClient(),  # type: ignore[arg-type]
            repository=repository,
            download_client=http,
        )
        with pytest.raises(RuntimeError, match="meizi_media_read_failed") as raised:
            await provider.get(key)

    formatted = "".join(
        traceback.format_exception(
            type(raised.value),
            raised.value,
            raised.value.__traceback__,
        )
    )
    assert secret_query not in formatted
    assert "token=" not in formatted
