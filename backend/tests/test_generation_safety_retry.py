"""生图 Worker 内容安全软化重试的租约与计费边界测试。"""

from __future__ import annotations

import asyncio
import io
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from PIL import Image

from app.models.generation_job import GenerationJob
from app.providers.image_gen import (
    ImageResult,
    ImageSafetyCategory,
    ImageSafetyRejectedError,
)
from app.workers import generation as generation_worker


def _png() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (4, 4), (255, 0, 0)).save(output, format="PNG")
    return output.getvalue()


def _safety_rejection() -> ImageSafetyRejectedError:
    return ImageSafetyRejectedError(
        provider="openai",
        category=ImageSafetyCategory.SAFETY,
        code="content_filter",
    )


def _job() -> GenerationJob:
    return GenerationJob(
        job_id="job-safety",
        owner_user_id="user-1",
        material_id="material-1",
        script_id="script-1",
        subject_id="subject-1",
        idempotency_key="idempotency-safety",
        request_hash="request-hash",
        input_version=1,
        attempt=7,
        job_type="generate_images",
        status="processing",
        progress=10,
        input_json={"prompt": "PRIVATE_ORIGINAL_PROMPT"},
    )


def _patch_valid_checks(monkeypatch) -> None:
    monkeypatch.setattr(
        generation_worker,
        "_input_version_is_current",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        generation_worker,
        "_job_token_is_current",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        generation_worker,
        "_heartbeat_or_stop",
        AsyncMock(return_value=True),
    )


def test_safety_retry_uses_original_moderate_strict_with_three_provider_calls(monkeypatch) -> None:
    async def scenario() -> None:
        _patch_valid_checks(monkeypatch)
        prompts: list[str] = []

        async def generate(**kwargs):
            prompts.append(kwargs["prompt"])
            if len(prompts) < 3:
                raise _safety_rejection()
            return [ImageResult(data=_png(), mime_type="image/png")]

        results, metrics = await generation_worker._generate_with_safety_softening(
            SimpleNamespace(rollback=AsyncMock()),
            _job(),
            SimpleNamespace(generate=generate),
            prompt="角色经历鲜血淋漓的搏斗，{character_ref}保持不变",
            negative_prompt="",
            model="gpt-image-2",
            size="1k",
            aspect="1:1",
            count=1,
            lease_token="lease-1",
        )

        assert len(results) == 1
        assert len(prompts) == 3
        assert "鲜血淋漓" in prompts[0]
        assert "克制的受伤痕迹" in prompts[1]
        assert "剪影化、非接触式的动作对峙" in prompts[2]
        assert all("{character_ref}" in item for item in prompts)
        audit = metrics["content_safety"]
        assert audit["provider_call_count"] == 3
        assert [item["level"] for item in audit["attempts"]] == [
            "original",
            "moderate",
            "strict",
        ]
        assert [item["outcome"] for item in audit["attempts"]] == [
            "rejected",
            "rejected",
            "succeeded",
        ]
        serialized = repr(metrics)
        assert "鲜血淋漓" not in serialized
        assert "克制的受伤痕迹" not in serialized
        assert "character_ref" not in serialized
        assert all(len(item["prompt_sha256"]) == 64 for item in audit["attempts"])

    asyncio.run(scenario())


def test_safety_retry_exhaustion_attaches_sanitized_metrics(monkeypatch) -> None:
    async def scenario() -> None:
        _patch_valid_checks(monkeypatch)
        provider = SimpleNamespace(generate=AsyncMock(side_effect=_safety_rejection()))

        with pytest.raises(ImageSafetyRejectedError) as exc_info:
            await generation_worker._generate_with_safety_softening(
                SimpleNamespace(rollback=AsyncMock()),
                _job(),
                provider,
                prompt="PRIVATE_REJECTED_PROMPT 鲜血",
                negative_prompt="PRIVATE_NEGATIVE_PROMPT",
                model="gpt-image-2",
                size="1k",
                aspect="1:1",
                count=1,
                lease_token="lease-1",
            )

        assert provider.generate.await_count == 3
        metrics = exc_info.value.retry_metrics
        assert metrics["content_safety"]["provider_call_count"] == 3
        assert metrics["content_safety"]["terminal_category"] == "safety"
        assert "PRIVATE_REJECTED_PROMPT" not in repr(metrics)
        assert "PRIVATE_NEGATIVE_PROMPT" not in repr(metrics)

    asyncio.run(scenario())


def test_safety_retry_stops_before_second_call_when_lease_is_lost(monkeypatch) -> None:
    async def scenario() -> None:
        provider = SimpleNamespace(generate=AsyncMock(side_effect=_safety_rejection()))
        monkeypatch.setattr(
            generation_worker,
            "_input_version_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_job_token_is_current",
            AsyncMock(side_effect=[True, False]),
        )
        monkeypatch.setattr(
            generation_worker,
            "_heartbeat_or_stop",
            AsyncMock(return_value=True),
        )
        db = SimpleNamespace(rollback=AsyncMock())

        with pytest.raises(generation_worker._GenerationExecutionStopped, match="lease_lost"):
            await generation_worker._generate_with_safety_softening(
                db,
                _job(),
                provider,
                prompt="提示",
                negative_prompt="",
                model="gpt-image-2",
                size="1k",
                aspect="1:1",
                count=1,
                lease_token="lease-1",
            )

        provider.generate.assert_awaited_once()
        db.rollback.assert_awaited_once()

    asyncio.run(scenario())


def test_safety_retry_stops_before_second_call_when_input_version_changes(monkeypatch) -> None:
    async def scenario() -> None:
        provider = SimpleNamespace(generate=AsyncMock(side_effect=_safety_rejection()))
        monkeypatch.setattr(
            generation_worker,
            "_input_version_is_current",
            AsyncMock(side_effect=[True, False]),
        )
        monkeypatch.setattr(
            generation_worker,
            "_job_token_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_heartbeat_or_stop",
            AsyncMock(return_value=True),
        )
        db = SimpleNamespace(rollback=AsyncMock())

        with pytest.raises(generation_worker._GenerationExecutionStopped, match="input_version"):
            await generation_worker._generate_with_safety_softening(
                db,
                _job(),
                provider,
                prompt="提示",
                negative_prompt="",
                model="gpt-image-2",
                size="1k",
                aspect="1:1",
                count=1,
                lease_token="lease-1",
            )

        provider.generate.assert_awaited_once()
        db.rollback.assert_awaited_once()

    asyncio.run(scenario())


def test_non_safety_error_is_not_retried(monkeypatch) -> None:
    async def scenario() -> None:
        _patch_valid_checks(monkeypatch)
        provider = SimpleNamespace(generate=AsyncMock(side_effect=TimeoutError("timeout")))

        with pytest.raises(TimeoutError, match="timeout"):
            await generation_worker._generate_with_safety_softening(
                SimpleNamespace(rollback=AsyncMock()),
                _job(),
                provider,
                prompt="提示",
                negative_prompt="",
                model="gpt-image-2",
                size="1k",
                aspect="1:1",
                count=1,
                lease_token="lease-1",
            )

        provider.generate.assert_awaited_once()

    asyncio.run(scenario())


def test_run_generation_success_after_safety_rejections_settles_once(monkeypatch) -> None:
    async def scenario() -> None:
        claim = generation_worker.GenerationJobClaim(
            job_id="job-safety",
            lease_token="lease-1",
            attempt=7,
            input_version=1,
        )
        job = _job()
        job.status = "starting"
        job.input_json = {
            "prompt": "角色经历鲜血淋漓的搏斗",
            "model": "gpt-image-2",
            "size": "1k",
            "aspect": "1:1",
            "count": 1,
            "subject_id": "subject-1",
            "reservation_id": "reservation-1",
        }
        db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
        monkeypatch.setattr(
            generation_worker,
            "claim_generation_job",
            AsyncMock(return_value=claim),
        )
        monkeypatch.setattr(generation_worker, "_get_generation_job", AsyncMock(return_value=job))
        monkeypatch.setattr(
            generation_worker,
            "_input_version_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_job_token_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_heartbeat_or_stop",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_resolve_script_id",
            AsyncMock(return_value="script-1"),
        )
        monkeypatch.setattr(
            generation_worker,
            "_persist_validated_image",
            AsyncMock(
                return_value=generation_worker.PersistedImage(
                    temp_key="temp.png",
                    final_key="final.png",
                    sha256="a" * 64,
                    image=generation_worker.validate_image_result(
                        ImageResult(data=_png(), mime_type="image/png")
                    ),
                )
            ),
        )
        monkeypatch.setattr(
            generation_worker,
            "_load_subject_for_candidate",
            AsyncMock(return_value=SimpleNamespace()),
        )
        monkeypatch.setattr(
            generation_worker,
            "add_candidate",
            AsyncMock(return_value=SimpleNamespace(id="asset-1")),
        )
        monkeypatch.setattr(
            generation_worker,
            "_lock_current_generation_job",
            AsyncMock(return_value=job),
        )
        settle = AsyncMock()
        release = AsyncMock()
        finish = AsyncMock(return_value=True)
        monkeypatch.setattr(generation_worker, "credits_settle", settle)
        monkeypatch.setattr(generation_worker, "credits_release", release)
        monkeypatch.setattr(generation_worker, "finish_generation_job", finish)
        monkeypatch.setattr(
            generation_worker,
            "_promote_generated_objects",
            AsyncMock(),
        )
        provider = SimpleNamespace(
            generate=AsyncMock(
                side_effect=[
                    _safety_rejection(),
                    _safety_rejection(),
                    [ImageResult(data=_png(), mime_type="image/png")],
                ]
            )
        )

        await generation_worker.run_generation_once(
            db,
            job.job_id,
            owner="worker-1",
            provider=provider,
            storage=SimpleNamespace(),
        )

        assert provider.generate.await_count == 3
        settle.assert_awaited_once_with(
            db,
            reservation_id="reservation-1",
            actual=generation_worker.settings.price_image_gen,
            commit=False,
        )
        release.assert_not_awaited()
        finish.assert_awaited_once()
        finish_metrics = finish.await_args.kwargs["metrics"]
        assert finish_metrics["content_safety"]["provider_call_count"] == 3

    asyncio.run(scenario())


def test_run_generation_exhausted_safety_rejections_releases_once(monkeypatch) -> None:
    async def scenario() -> None:
        claim = generation_worker.GenerationJobClaim(
            job_id="job-safety",
            lease_token="lease-1",
            attempt=7,
            input_version=1,
        )
        job = _job()
        job.status = "starting"
        job.input_json = {
            "prompt": "PRIVATE_REJECTED_PROMPT 鲜血",
            "model": "gpt-image-2",
            "size": "1k",
            "aspect": "1:1",
            "count": 1,
            "subject_id": "subject-1",
            "reservation_id": "reservation-1",
        }
        db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
        monkeypatch.setattr(
            generation_worker,
            "claim_generation_job",
            AsyncMock(return_value=claim),
        )
        monkeypatch.setattr(generation_worker, "_get_generation_job", AsyncMock(return_value=job))
        monkeypatch.setattr(
            generation_worker,
            "_input_version_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_job_token_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_heartbeat_or_stop",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            generation_worker,
            "_resolve_script_id",
            AsyncMock(return_value="script-1"),
        )
        monkeypatch.setattr(
            generation_worker,
            "_lock_current_generation_job",
            AsyncMock(return_value=job),
        )
        settle = AsyncMock()
        release = AsyncMock()
        finish = AsyncMock(return_value=True)
        monkeypatch.setattr(generation_worker, "credits_settle", settle)
        monkeypatch.setattr(generation_worker, "credits_release", release)
        monkeypatch.setattr(generation_worker, "finish_generation_job", finish)
        monkeypatch.setattr(generation_worker, "_delete_generated_objects", AsyncMock())
        provider = SimpleNamespace(generate=AsyncMock(side_effect=_safety_rejection()))

        await generation_worker.run_generation_once(
            db,
            job.job_id,
            owner="worker-1",
            provider=provider,
            storage=SimpleNamespace(),
        )

        assert provider.generate.await_count == 3
        settle.assert_not_awaited()
        release.assert_awaited_once_with(
            db,
            reservation_id="reservation-1",
            reason="provider_content_safety_rejected",
            commit=False,
        )
        finish.assert_awaited_once()
        assert finish.await_args.args[3] == "failed"
        assert finish.await_args.kwargs["error_type"] == "provider_content_safety_rejected"
        metrics = finish.await_args.kwargs["metrics"]
        assert metrics["content_safety"]["provider_call_count"] == 3
        assert "PRIVATE_REJECTED_PROMPT" not in repr(metrics)

    asyncio.run(scenario())
