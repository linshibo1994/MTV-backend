"""视频参考图审核状态与错误契约测试，不访问火山或对象存储。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import HTTPException

from app.api.v1 import video_generation as video_generation_api
from app.core.errors import ConflictError
from app.schemas.video import ReferenceImageAuditIn
from app.services import reference_image_audit as reference_image_audit_module
from app.services.reference_aggregator import AggregatedReferences
from app.services.reference_image_audit import (
    ImageAuditDecision,
    audit_references,
    current_audit_state,
    ensure_references_approved,
    include_first_frame,
    public_audit_state,
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

    async def _public_url(_key: str) -> str:
        return "https://storage.example.com/image.png"

    monkeypatch.setattr(
        "app.services.reference_image_audit._public_image_url",
        _public_url,
    )
    state = asyncio.run(audit_references(_references(), provider=ReviewProvider()))
    assert state["status"] == "review"
    with pytest.raises(ConflictError, match="人工复核"):
        ensure_references_approved(state, _references())


def test_reference_image_audit_without_images_does_not_block() -> None:
    references = AggregatedReferences()
    assert current_audit_state({}, references)["status"] == "not_required"
    assert ensure_references_approved({}, references)["status"] == "not_required"


def test_include_first_frame_rebinds_tokens_to_final_image_order() -> None:
    references = _references()
    references.reference_image_names = ["阿明"]
    references.reference_image_categories = ["character"]
    references.reference_image_refs = [
        {
            "token": "图1",
            "ref": "C01",
            "name": "阿明",
            "category": "character",
            "subject_id": "subject-1",
            "asset_entry_id": "asset-1",
        }
    ]
    include_first_frame(references, "frames/previous.png")
    assert [item["token"] for item in references.reference_image_refs] == ["图1", "图2"]
    assert references.reference_image_refs[0]["category"] == "first_frame"
    assert references.reference_image_refs[1]["ref"] == "C01"


async def test_local_reference_audit_reports_missing_public_url(monkeypatch) -> None:
    class _LocalStorage:
        async def external_read_url(
            self,
            key: str,
            *,
            expires_seconds: int = 300,
        ) -> None:
            assert key == "generate_images/material/image.png"
            assert expires_seconds == 600
            return None

        async def signed_url(self, *_args, **_kwargs) -> str:
            raise AssertionError("审核服务不应直接读取站内 signed_url")

    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "volc_image_audit_public_base_url",
        "",
    )
    monkeypatch.setattr(
        reference_image_audit_module,
        "get_storage",
        lambda: _LocalStorage(),
    )

    with pytest.raises(
        RuntimeError,
        match="reference_image_audit_public_url_unconfigured",
    ) as exc_info:
        await reference_image_audit_module._public_image_url(
            "generate_images/material/image.png"
        )

    assert reference_image_audit_module.reference_image_audit_failure_message(
        exc_info.value
    ) == (
        "本地存储未配置公网 HTTPS 审核地址，"
        "请联系管理员配置 VOLC_IMAGE_AUDIT_PUBLIC_BASE_URL"
    )


async def test_local_reference_audit_uses_expiring_proxy_when_configured(
    monkeypatch,
) -> None:
    class _LocalStorage:
        async def external_read_url(
            self,
            key: str,
            *,
            expires_seconds: int = 300,
        ) -> None:
            assert key == "generate_images/material/image.png"
            assert expires_seconds == 600
            return None

        async def signed_url(self, *_args, **_kwargs) -> str:
            raise AssertionError("本地审核应使用专用短期代理")

    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "volc_image_audit_public_base_url",
        "https://api.example.com",
    )
    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "session_secret",
        "test-only-session-secret-at-least-32-chars",
    )
    monkeypatch.setattr(
        reference_image_audit_module,
        "get_storage",
        lambda: _LocalStorage(),
    )

    url = await reference_image_audit_module._public_image_url(
        "generate_images/material/image.png"
    )

    parsed = urlsplit(url)
    assert (
        f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        == "https://api.example.com/api/x/cctv/zhihui/flow-app/_reference-image-audit"
    )
    token = parse_qs(parsed.query)["token"][0]
    assert reference_image_audit_module.verify_reference_image_audit_token(
        token,
        now=int(time.time()),
    ) == "generate_images/material/image.png"


async def test_reference_audit_config_error_returns_actionable_detail(
    monkeypatch,
    caplog,
) -> None:
    expected_message = (
        "本地存储未配置公网 HTTPS 审核地址，"
        "请联系管理员配置 VOLC_IMAGE_AUDIT_PUBLIC_BASE_URL"
    )
    group = SimpleNamespace(
        shot_group_id="group-1",
        unit_id="group:group-1",
        reference_image_audit={},
    )

    class _Result:
        def scalar_one_or_none(self):
            return group

    class _Db:
        commits = 0

        async def execute(self, _statement):
            return _Result()

        async def commit(self) -> None:
            self.commits += 1

    async def _allow_access(*_args, **_kwargs) -> None:
        return None

    async def _current_state(*_args, **_kwargs):
        return _references(), {
            "status": "pending",
            "reference_hash": reference_image_hash(_references()),
            "image_count": 1,
            "results": [],
        }

    async def _video_model(*_args, **_kwargs):
        return SimpleNamespace(reference_image_audit_required=True)

    async def _fail_audit(*_args, **_kwargs):
        raise RuntimeError("reference_image_audit_public_url_unconfigured")

    monkeypatch.setattr(video_generation_api, "_load_and_check_script", _allow_access)
    monkeypatch.setattr(
        video_generation_api,
        "_current_reference_image_audit_state",
        _current_state,
    )
    monkeypatch.setattr(video_generation_api, "resolve_video_model", _video_model)
    monkeypatch.setattr(reference_image_audit_module, "audit_references", _fail_audit)

    db = _Db()
    body = ReferenceImageAuditIn(
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="group-1",
    )

    with caplog.at_level(logging.WARNING), pytest.raises(HTTPException) as exc_info:
        await video_generation_api.audit_video_reference_images(
            body,
            db=db,
            user=SimpleNamespace(user_id="user-1"),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == expected_message
    assert group.reference_image_audit["message"] == expected_message
    assert db.commits == 1
    assert "reference_image_audit_public_url_unconfigured" in caplog.text


async def test_object_storage_reference_audit_uses_external_read_url_without_proxy_base(
    monkeypatch,
) -> None:
    expected_url = "https://flowapp.oss-cn-shanghai.aliyuncs.com/media/image.png?token=opaque"

    class _ObjectStorage:
        async def external_read_url(
            self,
            key: str,
            *,
            expires_seconds: int = 300,
        ) -> str:
            assert key == "generate_images/material/image.png"
            assert expires_seconds == 600
            return expected_url

        async def signed_url(self, *_args, **_kwargs) -> str:
            raise AssertionError("外部审核 URL 不应重复经过 signed_url")

    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "volc_image_audit_public_base_url",
        "",
    )
    monkeypatch.setattr(
        reference_image_audit_module,
        "get_storage",
        lambda: _ObjectStorage(),
    )

    url = await reference_image_audit_module._public_image_url(
        "generate_images/material/image.png"
    )

    assert url == expected_url


async def test_audit_references_passes_external_url_only_to_audit_provider(
    monkeypatch,
) -> None:
    expected_url = "https://flowapp.oss-cn-shanghai.aliyuncs.com/media/image.png?token=opaque"
    received_urls: list[str] = []

    class _ObjectStorage:
        async def external_read_url(
            self,
            key: str,
            *,
            expires_seconds: int = 300,
        ) -> str:
            assert key == "generate_images/material/image.png"
            assert expires_seconds == 600
            return expected_url

    class _AuditProvider:
        async def audit(self, *, image_url: str, data_id: str) -> ImageAuditDecision:
            received_urls.append(image_url)
            assert data_id
            return ImageAuditDecision(
                "approved",
                "pass",
                ["sensitive-label"],
                ["ocr-secret"],
                "request-secret",
            )

    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "volc_image_audit_public_base_url",
        "",
    )
    monkeypatch.setattr(
        reference_image_audit_module,
        "get_storage",
        lambda: _ObjectStorage(),
    )

    state = await audit_references(_references(), provider=_AuditProvider())
    serialized_state = json.dumps(state)

    assert received_urls == [expected_url]
    assert state["results"] == [{"status": "approved"}]
    assert expected_url not in serialized_state
    assert "token=opaque" not in serialized_state
    assert "generate_images/material/image.png" not in serialized_state
    assert "request-secret" not in serialized_state


def test_public_audit_state_removes_storage_and_provider_details() -> None:
    state = public_audit_state(
        {
            "status": "approved",
            "image_count": 1,
            "audited_at": "2026-07-30T00:00:00+00:00",
            "results": [
                {
                    "status": "approved",
                    "public_url": "https://cdn.example.com/media/image.png?token=opaque",
                    "storage_key": "generate_images/material/image.png",
                    "request_id": "request-secret",
                    "labels": ["sensitive-label"],
                }
            ],
            "message": None,
        }
    )

    assert state == {
        "status": "approved",
        "image_count": 1,
        "audited_at": "2026-07-30T00:00:00+00:00",
        "results": [{"status": "approved"}],
        "message": None,
    }


def test_reference_image_audit_token_rejects_tampering_and_expiry(monkeypatch) -> None:
    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "session_secret",
        "test-only-session-secret-at-least-32-chars",
    )
    token = reference_image_audit_module.create_reference_image_audit_token(
        "generate_images/material/image.png",
        expires_seconds=600,
        now=1_000,
    )

    assert reference_image_audit_module.verify_reference_image_audit_token(
        token,
        now=1_599,
    ) == "generate_images/material/image.png"
    with pytest.raises(ValueError, match="reference_image_audit_token_invalid"):
        reference_image_audit_module.verify_reference_image_audit_token(
            f"{token[:-1]}{'A' if token[-1] != 'A' else 'B'}",
            now=1_001,
        )
    with pytest.raises(ValueError, match="reference_image_audit_token_invalid"):
        reference_image_audit_module.verify_reference_image_audit_token(
            token,
            now=1_601,
        )


async def test_reference_image_audit_proxy_streams_with_token_without_session(
    client,
    monkeypatch,
) -> None:
    payload = b"\x89PNG\r\n\x1a\nimage"

    class _Storage:
        async def exists(self, key: str) -> bool:
            return key == "generate_images/material/image.png"

        async def get_size(self, key: str) -> int:
            assert key == "generate_images/material/image.png"
            return len(payload)

        async def iter_chunks(self, key: str, **_kwargs):
            assert key == "generate_images/material/image.png"
            yield payload

    from app.api.v1 import media

    monkeypatch.setattr(
        reference_image_audit_module.settings,
        "session_secret",
        "test-only-session-secret-at-least-32-chars",
    )
    monkeypatch.setattr(media, "get_storage", lambda: _Storage())
    token = reference_image_audit_module.create_reference_image_audit_token(
        "generate_images/material/image.png",
        expires_seconds=600,
    )

    response = await client.get(
        "/api/x/cctv/zhihui/flow-app/_reference-image-audit",
        params={"token": token},
    )

    assert response.status_code == 200
    assert response.content == payload
    assert response.headers["content-type"] == "image/png"
    assert response.headers["cache-control"] == "private, no-store"

    invalid = await client.get(
        "/api/x/cctv/zhihui/flow-app/_reference-image-audit",
        params={"token": f"{token}tampered"},
    )
    assert invalid.status_code == 404
