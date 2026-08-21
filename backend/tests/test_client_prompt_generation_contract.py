"""生图任务必须使用客户端最终提示词的契约测试。"""

from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.schemas.generation import BatchCreateJobIn, CreateJobIn
from app.services import generation as generation_service
from app.workers import generation as generation_worker


def test_single_generation_requires_client_prompt_and_removes_style_inputs() -> None:
    with pytest.raises(ValidationError):
        CreateJobIn(material_id="material-1", subject_id="subject-1")

    payload = CreateJobIn(
        material_id="material-1",
        subject_id="subject-1",
        prompt="  客户端最终提示词  ",
        negative_prompt="  客户端负向提示词  ",
    )
    assert payload.prompt == "客户端最终提示词"
    assert payload.negative_prompt == "客户端负向提示词"

    with pytest.raises(ValidationError):
        CreateJobIn(
            material_id="material-1",
            subject_id="subject-1",
            prompt="客户端提示词",
            style_id="cinematic",
        )


def test_batch_generation_requires_client_prompt_map_and_removes_style_inputs() -> None:
    with pytest.raises(ValidationError):
        BatchCreateJobIn(material_id="material-1")

    payload = BatchCreateJobIn(
        material_id="material-1",
        subject_ids=["subject-1"],
        prompts={"subject-1": "客户端提示词"},
        negative_prompts={"subject-1": "客户端负向词"},
    )
    assert payload.prompts == {"subject-1": "客户端提示词"}
    assert payload.negative_prompts == {"subject-1": "客户端负向词"}

    with pytest.raises(ValidationError):
        BatchCreateJobIn(
            material_id="material-1",
            subject_ids=["subject-1"],
            prompts={"subject-1": "客户端提示词"},
            style_ids={"subject-1": "cinematic"},
        )


@pytest.mark.asyncio
async def test_generation_job_freezes_client_prompts_without_server_template(monkeypatch) -> None:
    from tests.test_generation import _AsyncSessionAdapter, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw, subject_type="character")
    monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

    job = await generation_service.create_generation_job(
        _AsyncSessionAdapter(raw),
        user=seeded.user,
        payload=CreateJobIn(
            material_id=seeded.material.id,
            subject_id=seeded.subject.id,
            prompt="前端已经完成风格和构图的最终提示词",
            negative_prompt="前端负向提示词",
            model="banana-2",
            aspect="3:4",
        ),
        idempotency_key="client-final-prompt",
    )

    assert job.input_json["prompt"] == "前端已经完成风格和构图的最终提示词"
    assert job.input_json["negative_prompt"] == "前端负向提示词"
    assert "resolved_prompts" not in job.input_json
    assert job.input_json["prompt_source"] == "client"
    assert "style_id" not in job.input_json
    assert "art_style_id" not in job.input_json
    assert "visual_style" not in job.input_json


def test_worker_uses_client_prompt_snapshot_and_ignores_legacy_style_id() -> None:
    assert generation_worker._resolve_job_prompts(
        {
            "prompt_source": "client",
            "prompt": "前端正向提示词",
            "negative_prompt": "前端负向提示词",
            "style_id": "cinematic",
        },
        "旧任务提示词",
    ) == ("前端正向提示词", "前端负向提示词")
