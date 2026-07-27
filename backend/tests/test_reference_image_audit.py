"""视频参考图审核状态的纯服务层测试，不访问火山或对象存储。"""

from __future__ import annotations

import asyncio

import pytest

from app.core.errors import ConflictError
from app.services.reference_aggregator import AggregatedReferences
from app.services.reference_image_audit import (
    ImageAuditDecision,
    audit_references,
    current_audit_state,
    ensure_references_approved,
    reference_image_hash,
)


def _references(sha256: str = "a" * 64) -> AggregatedReferences:
    return AggregatedReferences(
        reference_images=[
            {
                "kind": "storage",
                "storage_key": "generate_images/material/image.png",
                "mime_type": "image/png",
                "size_bytes": 123,
                "sha256": sha256,
            }
        ]
    )


def test_reference_image_audit_snapshot_becomes_pending_after_image_change() -> None:
    first = _references()
    stored = {"status": "approved", "reference_hash": reference_image_hash(first), "results": []}
    assert current_audit_state(stored, first)["status"] == "approved"

    changed = _references("b" * 64)
    state = current_audit_state(stored, changed)
    assert state["status"] == "pending"
    with pytest.raises(ConflictError, match="尚未审核"):
        ensure_references_approved(stored, changed)


def test_reference_image_audit_reject_or_review_blocks_generation(monkeypatch) -> None:
    class ReviewProvider:
        async def audit(self, *, image_url: str, data_id: str) -> ImageAuditDecision:
            assert image_url.startswith("https://")
            assert data_id
            return ImageAuditDecision("review", "review", ["forbidden"], [], "request-1")

    monkeypatch.setattr(
        "app.services.reference_image_audit._public_image_url",
        lambda _key: "https://storage.example.com/image.png",
    )
    state = asyncio.run(audit_references(_references(), provider=ReviewProvider()))
    assert state["status"] == "review"
    with pytest.raises(ConflictError, match="人工复核"):
        ensure_references_approved(state, _references())


def test_reference_image_audit_without_images_does_not_block() -> None:
    references = AggregatedReferences()
    assert current_audit_state({}, references)["status"] == "not_required"
    assert ensure_references_approved({}, references)["status"] == "not_required"
