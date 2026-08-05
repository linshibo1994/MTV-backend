"""对象存储抽象和媒体 Range 的跨实现契约。"""

import asyncio
import inspect

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
    assert asyncio.run(
        provider.signed_url(
            "upload_images/material-1/image.png",
            expires_seconds=60,
        )
    ).endswith("/upload_images/material-1/image.png")


def test_local_external_read_url_is_unavailable(tmp_path) -> None:
    protocol_parameters = inspect.signature(StorageProvider.external_read_url).parameters
    local_parameters = inspect.signature(LocalStorageProvider.external_read_url).parameters

    assert "expires_seconds" in protocol_parameters
    assert "expires_seconds" in local_parameters
    provider = LocalStorageProvider(str(tmp_path))
    assert (
        asyncio.run(
            provider.external_read_url(
                "upload_images/material-1/image.png",
                expires_seconds=60,
            )
        )
        is None
    )


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
