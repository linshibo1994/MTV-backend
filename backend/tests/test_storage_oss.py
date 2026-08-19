"""阿里云 OSS V2 Provider 配置与跨实现契约测试；禁止访问真实 OSS。"""

from __future__ import annotations

import asyncio
import importlib
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.providers.storage import (
    StorageObjectNotFoundError,
    parse_mid_from_key,
    storage_key_for_write,
    validate_key_prefix,
)


class _ServiceError(Exception):
    def __init__(self, *, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


class _OperationError(Exception):
    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self._error = error

    def unwrap(self) -> Exception:
        return self._error


class _Request:
    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)


class _StaticCredentialsProvider:
    def __init__(self, *, access_key_id: str, access_key_secret: str) -> None:
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret


class _Body:
    def __init__(self, data: bytes, *, fail_read: bool = False) -> None:
        self.data = data
        self.fail_read = fail_read
        self.closed = False

    def read(self) -> bytes:
        if self.fail_read:
            raise RuntimeError("read failed")
        return self.data

    def iter_bytes(self, *, block_size: int):
        for offset in range(0, len(self.data), block_size):
            yield self.data[offset : offset + block_size]

    def close(self) -> None:
        self.closed = True


class _MemoryClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.configs: list[object] = []
        self.put_requests: list[_Request] = []
        self.copy_requests: list[_Request] = []
        self.get_requests: list[_Request] = []
        self.last_body: _Body | None = None
        self.copy_error: _OperationError | None = None
        self.fail_body_read = False
        self.presign_expires_seconds: int | None = None

    def bind_config(self, config):
        self.configs.append(config)
        return self

    def put_object(self, request: _Request) -> None:
        self.put_requests.append(request)
        self.objects[request.key] = bytes(request.body)

    def put_object_from_file(self, request: _Request, filepath: str) -> None:
        self.put_requests.append(request)
        self.objects[request.key] = Path(filepath).read_bytes()

    def get_object(self, request: _Request):
        self.get_requests.append(request)
        if request.key not in self.objects:
            raise _operation_error(404, "NoSuchKey")
        data = self.objects[request.key]
        range_header = getattr(request, "range_header", None)
        if range_header:
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_header)
            assert match is not None
            start, end = (int(value) for value in match.groups())
            data = data[start : end + 1]
        self.last_body = _Body(data, fail_read=self.fail_body_read)
        return SimpleNamespace(body=self.last_body)

    def head_object(self, request: _Request):
        if request.key not in self.objects:
            raise _operation_error(404, "NoSuchKey")
        return SimpleNamespace(content_length=len(self.objects[request.key]))

    def is_object_exist(self, *, bucket: str, key: str) -> bool:
        assert bucket == "flowapp"
        return key in self.objects

    def delete_object(self, request: _Request) -> None:
        self.objects.pop(request.key, None)

    def copy_object(self, request: _Request) -> None:
        self.copy_requests.append(request)
        if self.copy_error is not None:
            raise self.copy_error
        if request.source_key not in self.objects:
            raise _operation_error(404, "NoSuchKey")
        if getattr(request, "forbid_overwrite", None) == "true" and request.key in self.objects:
            raise _operation_error(409, "FileAlreadyExists")
        self.objects[request.key] = self.objects[request.source_key]

    def presign(self, request: _Request, *, expires):
        self.presign_expires_seconds = int(expires.total_seconds())
        return SimpleNamespace(
            url=f"https://flowapp.oss-cn-hangzhou.aliyuncs.com/{request.key}?signed=1"
        )


def _operation_error(status_code: int, code: str) -> _OperationError:
    return _OperationError(_ServiceError(status_code=status_code, code=code))


def _settings_values() -> dict[str, object]:
    return {
        "storage_driver": "oss",
        "aliyun_oss_endpoint": "https://oss-cn-hangzhou.aliyuncs.com",
        "aliyun_oss_access_key_id": "test-access-key-id",
        "aliyun_oss_access_key_secret": "test-access-key-secret",
        "aliyun_oss_bucket": "flowapp",
        "aliyun_oss_region": "cn-hangzhou",
        "_env_file": None,
    }


@contextmanager
def _load_provider(monkeypatch: pytest.MonkeyPatch, client: _MemoryClient):
    provider_module_name = "app.providers.storage_oss"
    previous_provider = sys.modules.pop(provider_module_name, None)
    fake_oss = ModuleType("alibabacloud_oss_v2")
    fake_oss.config = SimpleNamespace(  # type: ignore[attr-defined]
        load_default=lambda: SimpleNamespace(
            credentials_provider=None,
            region=None,
            endpoint=None,
        )
    )
    fake_oss.credentials = SimpleNamespace(  # type: ignore[attr-defined]
        StaticCredentialsProvider=_StaticCredentialsProvider
    )
    fake_oss.Client = client.bind_config  # type: ignore[attr-defined]
    fake_oss.PutObjectRequest = _Request  # type: ignore[attr-defined]
    fake_oss.CopyObjectRequest = _Request  # type: ignore[attr-defined]
    fake_oss.GetObjectRequest = _Request  # type: ignore[attr-defined]
    fake_oss.HeadObjectRequest = _Request  # type: ignore[attr-defined]
    fake_oss.DeleteObjectRequest = _Request  # type: ignore[attr-defined]
    fake_oss.StreamBody = _Body  # type: ignore[attr-defined]
    fake_oss.exceptions = SimpleNamespace(  # type: ignore[attr-defined]
        OperationError=_OperationError,
        ServiceError=_ServiceError,
    )
    monkeypatch.setitem(sys.modules, "alibabacloud_oss_v2", fake_oss)
    try:
        yield importlib.import_module(provider_module_name)
    finally:
        sys.modules.pop(provider_module_name, None)
        if previous_provider is not None:
            sys.modules[provider_module_name] = previous_provider


def _provider(module, client: _MemoryClient):
    provider = object.__new__(module.AliyunOSSStorageProvider)
    provider._bucket = "flowapp"
    provider._client = client
    return provider


@pytest.mark.parametrize(
    ("driver", "accepted"),
    [
        ("local", True),
        ("oss", True),
        ("s3", False),
        ("meizi", False),
    ],
)
def test_settings_storage_driver_only_accepts_local_and_oss(
    driver: str,
    accepted: bool,
) -> None:
    values = _settings_values()
    values["storage_driver"] = driver

    if accepted:
        assert Settings(**values).storage_driver == driver
        return

    with pytest.raises(ValidationError, match="storage_driver"):
        Settings(**values)


def test_aliyun_oss_settings_normalize_public_endpoint_and_hide_secrets() -> None:
    values = _settings_values()
    values["aliyun_oss_endpoint"] = "https://OSS-CN-HANGZHOU.aliyuncs.com/"
    values["aliyun_oss_region"] = "CN-HANGZHOU"

    configured = Settings(**values)

    assert configured.aliyun_oss_endpoint == "https://oss-cn-hangzhou.aliyuncs.com"
    assert configured.aliyun_oss_region == "cn-hangzhou"
    assert str(configured.aliyun_oss_access_key_id) == "**********"
    assert str(configured.aliyun_oss_access_key_secret) == "**********"


def test_aliyun_oss_settings_normalize_public_cdn_url() -> None:
    values = _settings_values()
    values["aliyun_oss_public_url"] = "https://CDN-AI-PUBLIC.COMMIND.COM/"

    configured = Settings(**values)

    assert configured.aliyun_oss_public_url == "https://cdn-ai-public.commind.com"


@pytest.mark.parametrize(
    "public_url",
    [
        "http://cdn-ai-public.commind.com",
        "https://localhost",
        "https://127.0.0.1",
        "https://user:password@cdn-ai-public.commind.com",
        "https://cdn-ai-public.commind.com/path",
        "https://cdn-ai-public.commind.com?token=test",
        "https://cdn-ai-public.commind.com#fragment",
    ],
)
def test_aliyun_oss_settings_reject_unsafe_public_cdn_url(public_url: str) -> None:
    values = _settings_values()
    values["aliyun_oss_public_url"] = public_url

    with pytest.raises(ValidationError, match="ALIYUN_OSS_PUBLIC_URL"):
        Settings(**values)


def test_storage_keys_accept_mtv_root_and_new_oss_writes_use_it(monkeypatch) -> None:
    from app.providers import storage as storage_module

    configured = Settings(**_settings_values())
    monkeypatch.setattr(storage_module, "settings", configured)

    legacy_key = "upload_images/material-1/portrait.png"
    rooted_key = "MTV/upload_images/material-1/portrait.png"

    assert validate_key_prefix(rooted_key) == rooted_key
    assert parse_mid_from_key(rooted_key) == "material-1"
    assert storage_key_for_write(legacy_key) == f"MTV/{legacy_key}"
    assert storage_key_for_write(rooted_key) == rooted_key


def test_new_upload_and_render_keys_use_mtv_root_in_oss(monkeypatch) -> None:
    from app.api.v1 import media
    from app.providers import storage as storage_module
    from app.services import timeline_render

    monkeypatch.setattr(storage_module, "settings", SimpleNamespace(storage_driver="oss"))

    upload_key = media._storage_key_for_upload("material-1", "portrait.png", "image/png")
    temporary_upload_key = media._temp_storage_key_for_upload(
        "material-1", "portrait.png", "image/png"
    )
    render_key = timeline_render._output_storage_key("render-1", "script-1", "episode-1")

    assert upload_key.startswith("MTV/upload_images/material-1/")
    assert temporary_upload_key.startswith("MTV/upload_images/_tmp/material-1/")
    assert render_key == "MTV/generate_outputs/script-1/episode-1/render-1.mp4"


def test_new_generated_image_keys_use_mtv_root_in_oss(monkeypatch) -> None:
    from app.providers import storage as storage_module
    from app.workers import generation

    monkeypatch.setattr(storage_module, "settings", SimpleNamespace(storage_driver="oss"))
    storage = SimpleNamespace(put=AsyncMock())
    image = generation.ValidatedImage(
        data=b"image",
        mime_type="image/png",
        extension="png",
        width=1,
        height=1,
    )

    persisted = asyncio.run(
        generation._persist_validated_image("material-1", "job-1", "lease-1", image, storage)
    )

    assert persisted.final_key.startswith("MTV/generate_images/material-1/")
    assert persisted.temp_key.startswith("MTV/generate_images/material-1/tmp/job-1/")
    storage.put.assert_awaited_once_with(persisted.temp_key, b"image")


def test_video_worker_recovery_keys_use_mtv_root_in_oss(monkeypatch) -> None:
    from app.providers import storage as storage_module
    from app.workers import video_generation

    monkeypatch.setattr(storage_module, "settings", SimpleNamespace(storage_driver="oss"))

    assert video_generation._video_output_storage_key("job-1", "attempt-1") == (
        "MTV/generate_images/video/job-1-attempt-1.mp4"
    )
    assert video_generation._video_last_frame_storage_key("group-1", "attempt-1") == (
        "/MTV/generate_images/video/frames/group-1_attempt-1_last.png"
    )


@pytest.mark.parametrize(
    ("field_name", "env_name"),
    [
        ("aliyun_oss_endpoint", "ALIYUN_OSS_ENDPOINT"),
        ("aliyun_oss_access_key_id", "ALIYUN_OSS_ACCESS_KEY_ID"),
        ("aliyun_oss_access_key_secret", "ALIYUN_OSS_ACCESS_KEY_SECRET"),
        ("aliyun_oss_bucket", "ALIYUN_OSS_BUCKET"),
        ("aliyun_oss_region", "ALIYUN_OSS_REGION"),
    ],
)
def test_aliyun_oss_settings_reject_missing_required_values(
    field_name: str,
    env_name: str,
) -> None:
    values = _settings_values()
    values[field_name] = ""

    with pytest.raises(ValidationError, match=env_name):
        Settings(**values)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://oss-cn-hangzhou.aliyuncs.com",
        "https://oss-cn-shanghai.aliyuncs.com",
        "https://oss-cn-hangzhou-internal.aliyuncs.com",
        "https://flowapp.oss-cn-hangzhou.aliyuncs.com",
        "https://user:password@oss-cn-hangzhou.aliyuncs.com",
        "https://oss-cn-hangzhou.aliyuncs.com/path",
    ],
)
def test_aliyun_oss_settings_reject_unsafe_or_mismatched_endpoint(endpoint: str) -> None:
    values = _settings_values()
    values["aliyun_oss_endpoint"] = endpoint

    with pytest.raises(ValidationError, match="ALIYUN_OSS_ENDPOINT"):
        Settings(**values)


@pytest.mark.parametrize("bucket", ["ab", "FlowApp", "-flowapp", "flow_app"])
def test_aliyun_oss_settings_reject_invalid_bucket(bucket: str) -> None:
    values = _settings_values()
    values["aliyun_oss_bucket"] = bucket

    with pytest.raises(ValidationError, match="ALIYUN_OSS_BUCKET"):
        Settings(**values)


def test_aliyun_oss_provider_builds_v2_client_from_explicit_settings(monkeypatch) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        configured = Settings(**_settings_values())
        monkeypatch.setattr(module, "settings", configured)

        provider = module.AliyunOSSStorageProvider()

        assert provider._bucket == "flowapp"
        assert len(client.configs) == 1
        config = client.configs[0]
        assert config.region == "cn-hangzhou"
        assert config.endpoint == "https://oss-cn-hangzhou.aliyuncs.com"
        assert config.credentials_provider.access_key_id == "test-access-key-id"
        assert config.credentials_provider.access_key_secret == "test-access-key-secret"


def test_aliyun_oss_provider_read_write_range_sign_and_delete_contract(
    monkeypatch,
    tmp_path,
) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        provider = _provider(module, client)
        upload_path = tmp_path / "payload.bin"
        upload_path.write_bytes(b"file-payload")

        async def scenario() -> None:
            key = "upload_images/material-1/data.bin"
            file_key = "upload_images/material-1/file.bin"
            assert await provider.put(f"/{key}", b"abcdef") == key
            assert await provider.put_file(file_key, str(upload_path)) == file_key
            assert [request.acl for request in client.put_requests] == ["private", "private"]
            assert await provider.exists(key) is True
            assert await provider.get_size(key) == 6
            assert await provider.get(key) == b"abcdef"
            assert client.last_body is not None and client.last_body.closed is True

            chunks = [
                chunk
                async for chunk in provider.iter_chunks(
                    key,
                    chunk_size=2,
                    start=1,
                    end=4,
                )
            ]
            assert b"".join(chunks) == b"bcde"
            assert client.get_requests[-1].range_header == "bytes=1-4"
            assert client.get_requests[-1].range_behavior == "standard"
            assert client.last_body is not None and client.last_body.closed is True

            signed = await provider.signed_url(key, expires_seconds=600)
            assert signed.startswith("https://flowapp.oss-cn-hangzhou.aliyuncs.com/")
            assert client.presign_expires_seconds == 600

            with pytest.raises(RuntimeError, match="ALIYUN_OSS_PUBLIC_URL"):
                await provider.external_read_url(key, expires_seconds=600)

            await provider.delete(key)
            assert await provider.exists(key) is False
            assert await provider.get(file_key) == b"file-payload"

        asyncio.run(scenario())


def test_aliyun_oss_external_read_url_uses_configured_public_cdn_without_signing(
    monkeypatch,
) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        values = _settings_values()
        values["aliyun_oss_public_url"] = "https://cdn-ai-public.commind.com"
        monkeypatch.setattr(module, "settings", Settings(**values))
        provider = module.AliyunOSSStorageProvider()

        async def scenario() -> None:
            url = await provider.external_read_url(
                "/MTV/upload_images/material-1/portrait #1.png",
                expires_seconds=600,
            )

            assert (
                url
                == "https://cdn-ai-public.commind.com/MTV/upload_images/material-1/portrait%20%231.png"
            )
            assert client.presign_expires_seconds is None

        asyncio.run(scenario())


def test_aliyun_oss_provider_uses_public_read_acl_for_writes_when_public_url_configured(
    monkeypatch,
    tmp_path,
) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        values = _settings_values()
        values["aliyun_oss_public_url"] = "https://cdn-ai-public.commind.com"
        monkeypatch.setattr(module, "settings", Settings(**values))
        provider = _provider(module, client)
        upload_path = tmp_path / "payload.bin"
        upload_path.write_bytes(b"file-payload")

        async def scenario() -> None:
            source = "upload_images/_tmp/material-1/source.bin"
            final = "upload_images/material-1/final.bin"

            assert await provider.put("upload_images/material-1/data.bin", b"payload")
            assert await provider.put_file("upload_images/material-1/file.bin", str(upload_path))
            client.objects[source] = b"candidate"
            assert await provider.promote(source, final) == final

        asyncio.run(scenario())

        assert [request.acl for request in client.put_requests] == ["public-read", "public-read"]
        assert client.copy_requests[-1].acl == "public-read"


def test_public_url_payload_adds_cdn_urls_without_changing_storage_keys(monkeypatch) -> None:
    from app.providers import storage as storage_module

    values = _settings_values()
    values["aliyun_oss_public_url"] = "https://cdn-ai-public.commind.com"
    monkeypatch.setattr(storage_module, "settings", Settings(**values))
    payload = {
        "storage_key": "MTV/upload_images/material-1/portrait #1.png",
        "nested": {
            "video_storage_key": "MTV/generate_images/video/job-1.mp4",
        },
        "items": [
            {"storageKey": "MTV/generate_images/material-1/portrait.png"},
        ],
    }

    result = storage_module.public_urls_for_payload(payload)

    assert result == {
        "storage_key": "MTV/upload_images/material-1/portrait #1.png",
        "public_url": "https://cdn-ai-public.commind.com/MTV/upload_images/material-1/portrait%20%231.png",
        "nested": {
            "video_storage_key": "MTV/generate_images/video/job-1.mp4",
            "video_public_url": "https://cdn-ai-public.commind.com/MTV/generate_images/video/job-1.mp4",
        },
        "items": [
            {
                "storageKey": "MTV/generate_images/material-1/portrait.png",
                "publicUrl": "https://cdn-ai-public.commind.com/MTV/generate_images/material-1/portrait.png",
            },
        ],
    }
    assert payload["nested"].get("video_public_url") is None


def test_aliyun_oss_put_file_rejects_copy_object_limit_before_upload(
    monkeypatch,
    tmp_path,
) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        provider = _provider(module, client)
        upload_path = tmp_path / "five-gib.bin"
        with upload_path.open("wb") as upload_file:
            upload_file.truncate(module.MAX_SIMPLE_OBJECT_BYTES)

        with pytest.raises(ValueError, match="小于 5 GiB"):
            asyncio.run(
                provider.put_file(
                    "upload_images/material-1/five-gib.bin",
                    str(upload_path),
                )
            )

        assert client.objects == {}


def test_aliyun_oss_promote_distinguishes_replay_from_lost_source(monkeypatch) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        provider = _provider(module, client)
        source = "upload_images/_tmp/material-1/source.bin"
        final = "upload_images/material-1/final.bin"

        async def scenario() -> None:
            client.objects[source] = b"payload"
            assert await provider.promote(source, final) == final
            assert source not in client.objects
            assert client.objects[final] == b"payload"
            assert client.copy_requests[-1].acl == "private"

            assert await provider.promote(source, final) == final
            del client.objects[final]
            with pytest.raises(StorageObjectNotFoundError):
                await provider.promote(source, final)

        asyncio.run(scenario())


def test_aliyun_oss_promote_if_absent_is_atomic_and_preserves_conflicting_source(
    monkeypatch,
) -> None:
    client = _MemoryClient()
    with _load_provider(monkeypatch, client) as module:
        provider = _provider(module, client)
        source = "upload_images/_tmp/material-1/source.bin"
        final = "upload_images/material-1/final.bin"

        async def scenario() -> None:
            client.objects[source] = b"candidate"
            client.objects[final] = b"existing"
            assert await provider.promote_if_absent(source, final) is False
            assert client.objects[source] == b"candidate"
            assert client.objects[final] == b"existing"
            assert client.copy_requests[-1].forbid_overwrite == "true"

            del client.objects[final]
            assert await provider.promote_if_absent(source, final) is True
            assert source not in client.objects
            assert client.objects[final] == b"candidate"

            del client.objects[final]
            with pytest.raises(StorageObjectNotFoundError):
                await provider.promote_if_absent(source, final)

        asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status_code", "code"),
    [
        (409, "ObjectLocked"),
        (404, "NoSuchBucket"),
        (403, "AccessDenied"),
        (500, "InternalError"),
    ],
)
def test_aliyun_oss_promote_if_absent_only_maps_exact_destination_conflict(
    monkeypatch,
    status_code: int,
    code: str,
) -> None:
    client = _MemoryClient()
    client.copy_error = _operation_error(status_code, code)
    with _load_provider(monkeypatch, client) as module:
        provider = _provider(module, client)

        with pytest.raises(_OperationError):
            asyncio.run(
                provider.promote_if_absent(
                    "upload_images/_tmp/material-1/source.bin",
                    "upload_images/material-1/final.bin",
                )
            )


def test_aliyun_oss_get_closes_response_body_after_read_failure(monkeypatch) -> None:
    client = _MemoryClient()
    client.objects["upload_images/material-1/data.bin"] = b"payload"
    client.fail_body_read = True
    with _load_provider(monkeypatch, client) as module:
        provider = _provider(module, client)

        with pytest.raises(RuntimeError, match="read failed"):
            asyncio.run(provider.get("upload_images/material-1/data.bin"))
        assert client.last_body is not None and client.last_body.closed is True


def test_aliyun_oss_factory_fails_closed_when_optional_dependency_is_missing(
    monkeypatch,
) -> None:
    from app.providers import storage

    monkeypatch.setattr(storage.settings, "storage_driver", "oss")
    monkeypatch.setattr(storage, "_storage_singleton", None)
    monkeypatch.setitem(sys.modules, "app.providers.storage_oss", None)

    with pytest.raises(RuntimeError, match="阿里云 OSS 存储不可用"):
        storage.get_storage()


def test_locked_aliyun_oss_sdk_serializes_forbid_overwrite_header() -> None:
    oss = pytest.importorskip("alibabacloud_oss_v2")
    put_request = oss.PutObjectRequest(
        bucket="flowapp",
        key="upload_images/material-1/source.bin",
        acl="private",
        body=b"payload",
    )
    request = oss.CopyObjectRequest(
        bucket="flowapp",
        key="upload_images/material-1/final.bin",
        source_bucket="flowapp",
        source_key="upload_images/_tmp/material-1/source.bin",
        acl="private",
        forbid_overwrite="true",
    )

    assert put_request.acl == "private"
    assert put_request._attribute_map["acl"] == {
        "tag": "input",
        "position": "header",
        "rename": "x-oss-object-acl",
    }
    assert request.acl == "private"
    assert request._attribute_map["acl"] == {
        "tag": "input",
        "position": "header",
        "rename": "x-oss-object-acl",
    }
    assert request.forbid_overwrite == "true"
    assert request._attribute_map["forbid_overwrite"] == {
        "tag": "input",
        "position": "header",
        "rename": "x-oss-forbid-overwrite",
        "type": "bool",
    }
    service_error = oss.exceptions.ServiceError(
        status_code=409,
        code="FileAlreadyExists",
        request_id="test-request-id",
        message="object already exists",
        ec="",
        timestamp="2026-08-05T00:00:00Z",
        request_target="https://oss-cn-hangzhou.aliyuncs.com",
    )
    operation_error = oss.exceptions.OperationError(name="CopyObject", error=service_error)
    assert operation_error.unwrap() is service_error
    assert service_error.status_code == 409
    assert service_error.code == "FileAlreadyExists"
