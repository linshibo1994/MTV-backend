"""旧版媒体晋升任务的禁止覆盖契约。"""

import asyncio

import pytest

from app.providers.storage import StorageObjectConflictError
from app.providers.storage_local import LocalStorageProvider
from app.services import media_storage_jobs


def test_legacy_promote_preserves_existing_final_and_recoverable_source(tmp_path) -> None:
    async def scenario() -> None:
        provider = LocalStorageProvider(str(tmp_path / "storage"))
        source_key = "upload_images/_tmp/material-1/legacy.png"
        final_key = "upload_images/material-1/legacy.png"
        incoming = b"legacy-incoming"
        existing = b"unrelated-existing-final"
        await provider.put(source_key, incoming)
        await provider.put(final_key, existing)
        claim = media_storage_jobs.MediaStorageJobClaim(
            id="legacy-promote",
            idempotency_key="promote:legacy",
            job_type="promote",
            source_key=source_key,
            final_key=final_key,
            asset_id=None,
            status="running",
            attempt=1,
            lease_token="legacy-lease",
        )

        with pytest.raises(StorageObjectConflictError):
            await media_storage_jobs.promote_with_integrity(provider, claim)

        assert await provider.get(final_key) == existing
        assert await provider.get(source_key) == incoming

    asyncio.run(scenario())


def test_local_promote_if_absent_preserves_existing_final_and_source(tmp_path) -> None:
    async def scenario() -> None:
        provider = LocalStorageProvider(str(tmp_path / "storage"))
        source_key = "upload_images/_tmp/material-1/source.png"
        final_key = "upload_images/material-1/final.png"
        source = b"source"
        final = b"final"
        await provider.put(source_key, source)
        await provider.put(final_key, final)

        assert await provider.promote_if_absent(source_key, final_key) is False
        assert await provider.get(final_key) == final
        assert await provider.get(source_key) == source

    asyncio.run(scenario())
