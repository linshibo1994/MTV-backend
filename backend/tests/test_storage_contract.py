"""对象存储抽象和媒体 Range 的跨实现契约。"""

import asyncio
import importlib
import inspect
import io
import sys
from types import ModuleType

import pytest

from app.api.v1.media import _parse_range
from app.providers.storage import StorageProvider
from app.providers.storage_local import LocalStorageProvider


def test_storage_signed_url_uses_expires_seconds_keyword(tmp_path) -> None:
    protocol_parameters = inspect.signature(StorageProvider.signed_url).parameters
    local_parameters = inspect.signature(LocalStorageProvider.signed_url).parameters

    assert "expires_seconds" in protocol_parameters
    assert "expires_seconds" in local_parameters
    assert "expires_in" not in protocol_parameters
    assert "expires_in" not in local_parameters

    provider = LocalStorageProvider(str(tmp_path))
    assert provider.signed_url(
        "upload_images/material-1/image.png", expires_seconds=60
    ).endswith("/upload_images/material-1/image.png")


def test_media_range_rejects_multiple_ranges() -> None:
    assert _parse_range("bytes=0-1,4-5", total=10) is None


def test_local_promote_distinguishes_replayed_success_from_lost_source(tmp_path) -> None:
    async def scenario() -> None:
        from app.providers.storage import StorageObjectNotFoundError

        provider = LocalStorageProvider(str(tmp_path))
        source_key = "upload_images/_tmp/material-1/source.png"
        final_key = "upload_images/material-1/final.png"

        await provider.put(final_key, b"already-promoted")
        assert await provider.promote(source_key, final_key) == final_key
        assert await provider.get(final_key) == b"already-promoted"

        await provider.delete(final_key)
        with pytest.raises(StorageObjectNotFoundError):
            await provider.promote(source_key, final_key)

    asyncio.run(scenario())


def test_s3_promote_distinguishes_replayed_success_from_lost_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ClientError(Exception):
        def __init__(self, response: dict, operation_name: str) -> None:
            super().__init__(operation_name)
            self.response = response

    boto3_module = ModuleType("boto3")
    boto3_module.client = lambda *_args, **_kwargs: None  # type: ignore[attr-defined]
    botocore_module = ModuleType("botocore")
    botocore_client_module = ModuleType("botocore.client")
    botocore_client_module.BaseClient = object  # type: ignore[attr-defined]
    botocore_exceptions_module = ModuleType("botocore.exceptions")
    botocore_exceptions_module.ClientError = _ClientError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    monkeypatch.setitem(sys.modules, "botocore", botocore_module)
    monkeypatch.setitem(sys.modules, "botocore.client", botocore_client_module)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions_module)
    monkeypatch.delitem(sys.modules, "app.providers.storage_s3", raising=False)

    from app.providers.storage import StorageObjectNotFoundError

    S3StorageProvider = importlib.import_module(
        "app.providers.storage_s3"
    ).S3StorageProvider

    class _MissingSourceClient:
        def __init__(self, *, final_exists: bool) -> None:
            self.final_exists = final_exists

        def copy_object(self, **_kwargs) -> None:
            raise _ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "CopyObject",
            )

        def head_object(self, **_kwargs) -> dict:
            if self.final_exists:
                return {"ContentLength": 1}
            raise _ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )

        def delete_object(self, **_kwargs) -> None:
            raise AssertionError("source 缺失时不得执行删除")

    async def scenario() -> None:
        source_key = "upload_images/_tmp/material-1/source.png"
        final_key = "upload_images/material-1/final.png"

        replay = object.__new__(S3StorageProvider)
        replay._bucket = "test-bucket"
        replay._client = _MissingSourceClient(final_exists=True)
        assert await replay.promote(source_key, final_key) == final_key

        lost = object.__new__(S3StorageProvider)
        lost._bucket = "test-bucket"
        lost._client = _MissingSourceClient(final_exists=False)
        with pytest.raises(StorageObjectNotFoundError):
            await lost.promote(source_key, final_key)

    asyncio.run(scenario())


def test_s3_promote_if_absent_uses_destination_precondition_and_preserves_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ClientError(Exception):
        def __init__(self, response: dict, operation_name: str) -> None:
            super().__init__(operation_name)
            self.response = response

    boto3_module = ModuleType("boto3")
    boto3_module.client = lambda *_args, **_kwargs: None  # type: ignore[attr-defined]
    botocore_module = ModuleType("botocore")
    botocore_client_module = ModuleType("botocore.client")
    botocore_client_module.BaseClient = object  # type: ignore[attr-defined]
    botocore_exceptions_module = ModuleType("botocore.exceptions")
    botocore_exceptions_module.ClientError = _ClientError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    monkeypatch.setitem(sys.modules, "botocore", botocore_module)
    monkeypatch.setitem(sys.modules, "botocore.client", botocore_client_module)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions_module)
    monkeypatch.delitem(sys.modules, "app.providers.storage_s3", raising=False)

    S3StorageProvider = importlib.import_module(
        "app.providers.storage_s3"
    ).S3StorageProvider

    class _ConditionalConflictClient:
        def __init__(self) -> None:
            self.put_kwargs: dict | None = None
            self.deleted = False

        def get_object(self, **_kwargs) -> dict:
            return {"Body": io.BytesIO(b"source"), "ContentLength": 6}

        def put_object(self, **kwargs) -> None:
            self.put_kwargs = kwargs
            raise _ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )

        def delete_object(self, **_kwargs) -> None:
            self.deleted = True

    async def scenario() -> None:
        client = _ConditionalConflictClient()
        provider = object.__new__(S3StorageProvider)
        provider._bucket = "test-bucket"
        provider._client = client

        published = await provider.promote_if_absent(
            "upload_images/_tmp/material-1/source.png",
            "upload_images/material-1/final.png",
        )
        assert published is False
        assert client.put_kwargs is not None
        assert client.put_kwargs["IfNoneMatch"] == "*"
        assert client.put_kwargs["ContentLength"] == 6
        assert client.put_kwargs["Body"].closed
        assert client.deleted is False

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (400, "NotImplemented"),
        (501, "NotImplemented"),
    ],
)
def test_s3_promote_if_absent_fails_closed_when_conditional_put_is_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    code: str,
) -> None:
    class _ClientError(Exception):
        def __init__(self, response: dict, operation_name: str) -> None:
            super().__init__(operation_name)
            self.response = response

    boto3_module = ModuleType("boto3")
    boto3_module.client = lambda *_args, **_kwargs: None  # type: ignore[attr-defined]
    botocore_module = ModuleType("botocore")
    botocore_client_module = ModuleType("botocore.client")
    botocore_client_module.BaseClient = object  # type: ignore[attr-defined]
    botocore_exceptions_module = ModuleType("botocore.exceptions")
    botocore_exceptions_module.ClientError = _ClientError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    monkeypatch.setitem(sys.modules, "botocore", botocore_module)
    monkeypatch.setitem(sys.modules, "botocore.client", botocore_client_module)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions_module)
    monkeypatch.delitem(sys.modules, "app.providers.storage_s3", raising=False)

    S3StorageProvider = importlib.import_module(
        "app.providers.storage_s3"
    ).S3StorageProvider

    class _UnsupportedConditionalPutClient:
        def __init__(self) -> None:
            self.body = io.BytesIO(b"source")
            self.deleted = False

        def get_object(self, **_kwargs) -> dict:
            return {"Body": self.body, "ContentLength": 6}

        def put_object(self, **kwargs) -> None:
            assert kwargs["IfNoneMatch"] == "*"
            raise _ClientError(
                {
                    "Error": {"Code": code},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "PutObject",
            )

        def copy_object(self, **_kwargs) -> None:
            raise AssertionError("条件写不受支持时不得回退到可覆盖复制")

        def delete_object(self, **_kwargs) -> None:
            self.deleted = True

    async def scenario() -> None:
        client = _UnsupportedConditionalPutClient()
        provider = object.__new__(S3StorageProvider)
        provider._bucket = "test-bucket"
        provider._client = client

        with pytest.raises(_ClientError):
            await provider.promote_if_absent(
                "upload_images/_tmp/material-1/source.png",
                "upload_images/material-1/final.png",
            )
        assert client.body.closed
        assert client.deleted is False

    asyncio.run(scenario())


def test_local_storage_iter_chunks_rejects_symlink_swapped_after_size_check(monkeypatch, tmp_path) -> None:
    async def scenario() -> None:
        from app.providers import storage_local

        root = tmp_path / "root"
        outside = tmp_path / "outside.txt"
        outside.write_bytes(b"outside-secret")
        target = root / "upload_images" / "material-1" / "a.png"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"inside")
        provider = LocalStorageProvider(str(root))
        real_size_file = storage_local.LocalStorageProvider._size_file
        swapped = False

        def _swap_then_size(path: str) -> int:
            nonlocal swapped
            size = real_size_file(path)
            if not swapped and path.endswith("a.png"):
                swapped = True
                target.unlink()
                target.symlink_to(outside)
            return size

        monkeypatch.setattr(storage_local.LocalStorageProvider, "_size_file", staticmethod(_swap_then_size))

        with pytest.raises(ValueError):
            async for chunk in provider.iter_chunks("upload_images/material-1/a.png", chunk_size=64):
                assert chunk

    asyncio.run(scenario())
