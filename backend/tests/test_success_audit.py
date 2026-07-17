"""关键成功操作的审计契约。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.api.v1 import asset_library, video_generation
from app.schemas.asset import (
    ApproveAudioIn,
    ApproveImageIn,
    AssetEntryOut,
    ImpactSummaryOut,
    SubjectAssetOut,
)
from app.schemas.video import ActiveProviderUpdateIn


def _subject_out(*, image_id: str | None = None, audio_id: str | None = None) -> SubjectAssetOut:
    def entry(asset_id: str, kind: str) -> AssetEntryOut:
        return AssetEntryOut(
            asset_id=asset_id,
            storage_key=f"/{asset_id}",
            mime_type="image/png" if kind == "image" else "audio/mpeg",
            size_bytes=1,
            source="upload_images",
            kind=kind,
            status="approved",
            is_approved=True,
        )

    return SubjectAssetOut(
        subject_id="subject-1",
        subject_name="主角",
        subject_type="character",
        subject_type_label="角色",
        review_status="approved",
        review_status_label="已通过",
        image_entry=entry(image_id, "image") if image_id else None,
        audio_entries=[entry(audio_id, "audio")] if audio_id else [],
    )


async def test_delete_subject_writes_success_audit(monkeypatch) -> None:
    db = SimpleNamespace(commit=AsyncMock())
    user = SimpleNamespace(user_id="user-1", username="alice")
    subject = SimpleNamespace(
        id="subject-1",
        script_id="script-1",
        name="主角",
        type="character",
        review_status="approved",
        image_entry_id="image-1",
    )
    audit = AsyncMock()
    monkeypatch.setattr(asset_library, "_load_script_or_403", AsyncMock())
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_subject_for_identity_update",
        AsyncMock(return_value=subject),
    )
    monkeypatch.setattr(asset_library.asset_crud, "delete_subject", AsyncMock())
    monkeypatch.setattr(asset_library, "audit_log", audit)

    result = await asset_library.delete_subject(
        "script-1", "subject-1", db=db, user=user
    )

    assert result == {"detail": "已删除"}
    payload = audit.await_args.kwargs
    assert payload["action"] == "subject_delete"
    assert payload["resource_type"] == "subject"
    assert payload["resource_id"] == "subject-1"
    assert payload["before"]["image_entry_id"] == "image-1"
    assert payload["after"] is None
    assert payload["commit"] is True


async def test_approve_image_audits_only_a_real_state_change(monkeypatch) -> None:
    db = SimpleNamespace()
    user = SimpleNamespace(user_id="user-1", username="alice")
    subject = SimpleNamespace(image_entry_id="old-image")
    audit = AsyncMock()
    monkeypatch.setattr(asset_library, "_load_script_or_403", AsyncMock())
    monkeypatch.setattr(
        asset_library, "_load_subject_for_script", AsyncMock(return_value=subject)
    )
    monkeypatch.setattr(asset_library, "audit_log", audit)

    monkeypatch.setattr(
        asset_library.review_service,
        "approve_image_with_lock",
        AsyncMock(return_value=(_subject_out(image_id="new-image"), None)),
    )
    result = await asset_library.approve_image(
        "script-1",
        "subject-1",
        ApproveImageIn(
            candidate_id="new-image",
            expected_image_entry_id="old-image",
            confirm=True,
        ),
        db=db,
        user=user,
    )

    assert result.subject.image_entry is not None
    payload = audit.await_args.kwargs
    assert payload["action"] == "asset_approve_image"
    assert payload["before"] == {"image_entry_id": "old-image"}
    assert payload["after"] == {"image_entry_id": "new-image"}

    audit.reset_mock()
    monkeypatch.setattr(
        asset_library.review_service,
        "approve_image_with_lock",
        AsyncMock(
            return_value=(
                _subject_out(image_id="old-image"),
                ImpactSummaryOut(referenced=True, shot_group_count=1, details=[]),
            )
        ),
    )
    await asset_library.approve_image(
        "script-1",
        "subject-1",
        ApproveImageIn(candidate_id="new-image", expected_image_entry_id="old-image"),
        db=db,
        user=user,
    )
    audit.assert_not_awaited()


async def test_approve_audio_and_remove_candidate_write_success_audits(monkeypatch) -> None:
    db = SimpleNamespace()
    user = SimpleNamespace(user_id="user-1", username="alice")
    subject = SimpleNamespace(review_status="pending")
    audit = AsyncMock()
    monkeypatch.setattr(asset_library, "_load_script_or_403", AsyncMock())
    monkeypatch.setattr(
        asset_library, "_load_subject_for_script", AsyncMock(return_value=subject)
    )
    monkeypatch.setattr(asset_library, "audit_log", audit)
    monkeypatch.setattr(
        asset_library.review_service,
        "approve_audio_with_lock",
        AsyncMock(return_value=_subject_out(audio_id="audio-1")),
    )

    await asset_library.approve_audio(
        "script-1",
        "subject-1",
        ApproveAudioIn(candidate_id="audio-1"),
        db=db,
        user=user,
    )
    approved = audit.await_args.kwargs
    assert approved["action"] == "asset_approve_audio"
    assert approved["resource_id"] == "audio-1"
    assert approved["after"] == {
        "subject_id": "subject-1",
        "status": "approved",
    }

    audit.reset_mock()
    monkeypatch.setattr(
        asset_library.review_service,
        "remove_candidate_with_recalc",
        AsyncMock(return_value=_subject_out()),
    )
    await asset_library.remove_candidate(
        "script-1", "subject-1", "candidate-1", db=db, user=user
    )
    removed = audit.await_args.kwargs
    assert removed["action"] == "asset_candidate_delete"
    assert removed["resource_id"] == "candidate-1"
    assert removed["after"] == {"status": "deleted"}


async def test_provider_switch_writes_before_and_after_audit(monkeypatch) -> None:
    db = SimpleNamespace(commit=AsyncMock())
    user = SimpleNamespace(user_id="admin-1", username="admin", is_admin=True)
    old = SimpleNamespace(
        provider_key="jianyu", job_type="all_reference", model="seedance"
    )
    new = SimpleNamespace(
        provider_key="modelsai", job_type="all_reference", model="seedance"
    )
    audit = AsyncMock()
    monkeypatch.setattr(
        video_generation.video_crud,
        "get_provider_by_key",
        AsyncMock(return_value=new),
    )
    monkeypatch.setattr(
        video_generation.video_crud,
        "get_active_provider",
        AsyncMock(return_value=old),
    )
    monkeypatch.setattr(
        video_generation.video_crud,
        "set_active_provider",
        AsyncMock(return_value=new),
    )
    monkeypatch.setattr(video_generation, "audit_log", audit)

    result = await video_generation.set_active_provider(
        ActiveProviderUpdateIn(provider_key="modelsai"), db=db, user=user
    )

    assert result.provider_key == "modelsai"
    payload = audit.await_args.kwargs
    assert payload["action"] == "provider_switch"
    assert payload["resource_type"] == "video_provider"
    assert payload["before"] == {
        "provider_key": "jianyu",
        "job_type": "all_reference",
        "model": "seedance",
    }
    assert payload["after"] == {
        "provider_key": "modelsai",
        "job_type": "all_reference",
        "model": "seedance",
    }
    assert payload["commit"] is True
