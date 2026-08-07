"""风格目录、提示词优化与 style_id 契约测试。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.api.v1.generation_jobs import _sanitize_input_json
from app.core.errors import ConflictError
from app.schemas.generation import BatchCreateJobIn, CreateJobIn
from app.schemas.materials import (
    SUBJECT_DESIGN_PROMPT_MARKER,
    build_subject_prompt,
)
from app.schemas.prompt_style import PromptOptimizeIn
from app.schemas.video import VideoJobBatchCreateIn, VideoJobCreateIn
from app.services import prompt_style
from app.services.prompt_style import apply_style, optimize_prompt


def test_apply_style_with_placeholder() -> None:
    positive, negative = apply_style("cinematic", "a detective in the rain", "image")

    assert "a detective in the rain" in positive
    assert "{prompt}" not in positive
    assert negative == prompt_style.find_style("cinematic")["negative_prompt"]


def test_apply_style_without_placeholder(monkeypatch) -> None:
    monkeypatch.setattr(
        prompt_style,
        "STYLE_CATALOG",
        [
            {
                "id": "append-only",
                "name_zh": "追加测试",
                "target": ["image"],
                "prompt_template": "dramatic rim light",
                "negative_prompt": "flat lighting",
                "default_prompt": "a studio portrait",
                "best_for": ["测试"],
            }
        ],
    )

    assert apply_style("append-only", "a studio portrait", "image") == (
        "a studio portrait, dramatic rim light",
        "flat lighting",
    )


def test_apply_style_unknown_id() -> None:
    assert apply_style("missing-style", "original prompt", "image") == (
        "original prompt",
        "",
    )


def test_apply_style_uses_asset_safe_cyberpunk_direction_for_subject_design() -> None:
    raw_prompt = build_subject_prompt(
        "老农",
        "character",
        "中国乡村老年男性，穿历史时期粗布衣，使用纯净白底设定图",
    )

    positive, negative = apply_style("cyberpunk", raw_prompt, "image")

    assert raw_prompt in positive
    assert "cyberpunk visual-development asset sheet" in positive
    assert "dense cyberpunk world" not in positive
    assert "preserve every required view" in positive
    assert "clean empty background" not in negative
    assert "historical costume" not in negative
    assert "pastoral daylight" not in negative
    assert "low detail" in negative


@pytest.mark.asyncio
async def test_optimize_prompt_fallback() -> None:
    provider = SimpleNamespace(chat=AsyncMock(side_effect=ConnectionError("provider down")))

    result = await optimize_prompt("a quiet street", None, "image", provider=provider)

    assert result == {
        "optimized_prompt": "a quiet street",
        "negative_prompt": "",
        "style_id": None,
        "style_name": None,
        "original_prompt": "a quiet street",
    }


@pytest.mark.asyncio
async def test_optimize_prompt_success() -> None:
    provider = SimpleNamespace(
        chat=AsyncMock(return_value="a lone traveler crossing a rain-soaked avenue")
    )

    result = await optimize_prompt(
        "traveler in rain",
        "cinematic",
        "image",
        provider=provider,
    )

    assert "a lone traveler crossing a rain-soaked avenue" in result["optimized_prompt"]
    assert result["negative_prompt"] == prompt_style.find_style("cinematic")["negative_prompt"]
    assert result["style_id"] == "cinematic"
    assert result["style_name"] == "电影感"
    assert result["original_prompt"] == "traveler in rain"
    provider.chat.assert_awaited_once()
    call = provider.chat.await_args
    assert call.args[0] == [
        {"role": "user", "content": "请优化以下image生成提示词：\ntraveler in rain"}
    ]
    assert call.kwargs["temperature"] == 0.6
    assert call.kwargs["max_tokens"] == 800
    assert "禁止解释" in call.kwargs["system"]
    assert "禁止使用 Markdown 代码块" in call.kwargs["system"]
    assert "{{var}}" in call.kwargs["system"]


@pytest.mark.asyncio
async def test_optimize_prompt_includes_selected_style_context() -> None:
    provider = SimpleNamespace(
        chat=AsyncMock(return_value="an inked reporter holding a phone in a dynamic panel")
    )

    result = await optimize_prompt(
        "a reporter holding a phone",
        "comic-book",
        "image",
        provider=provider,
    )

    system = provider.chat.await_args.kwargs["system"]
    assert "目标风格：美漫" in system
    assert "dynamic American comic-book panel" in system
    assert "禁止加入这些冲突特征：photorealistic" in system
    assert result["optimized_prompt"].startswith("dynamic American comic-book panel")
    assert result["negative_prompt"].startswith("photorealistic")


@pytest.mark.asyncio
async def test_optimize_subject_design_prompt_preserves_full_contract() -> None:
    raw_prompt = build_subject_prompt(
        "老农",
        "character",
        "老年男性，满脸皱纹，戴草帽，穿粗布衣，手拿烙饼",
    )
    provider = SimpleNamespace(chat=AsyncMock(return_value="不应调用供应商"))

    result = await optimize_prompt(
        raw_prompt,
        "anime",
        "image",
        provider=provider,
    )

    provider.chat.assert_not_awaited()
    assert SUBJECT_DESIGN_PROMPT_MARKER in result["optimized_prompt"]
    assert raw_prompt in result["optimized_prompt"]
    assert "【6 道具与物理关系】" in result["optimized_prompt"]
    assert "真人摄影写实" not in raw_prompt
    assert result["negative_prompt"].startswith("photorealistic")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_output",
    [
        "```text\nfollow {{actor}} through the rain\n```",
        "Prompt: follow {{actor}} through the rain",
        "follow the actor through the rain",
        "follow {{actor}} while {{actor}} crosses the street",
    ],
)
async def test_optimize_prompt_falls_back_for_invalid_provider_output(
    provider_output: str,
) -> None:
    provider = SimpleNamespace(chat=AsyncMock(return_value=provider_output))

    result = await optimize_prompt("follow {{actor}}", None, "image", provider=provider)

    assert result["optimized_prompt"] == "follow {{actor}}"


@pytest.mark.asyncio
async def test_prompt_styles_api(client) -> None:
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True, is_admin=False)

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user

    response = await client.get("/v3api/prompt-styles")
    filtered_response = await client.get("/v3api/prompt-styles", params={"target": "video"})

    assert response.status_code == 200
    assert len(response.json()["styles"]) == 8
    assert response.json()["styles"][0]["id"] == "cinematic"
    assert filtered_response.status_code == 200
    filtered_styles = filtered_response.json()["styles"]
    assert filtered_styles
    assert len(filtered_styles) < len(response.json()["styles"])
    assert all("video" in style["target"] for style in filtered_styles)


def test_generation_schemas_accept_style_id() -> None:
    single = CreateJobIn(material_id="material-1", subject_id="subject-1", style_id="cinematic")
    batch = BatchCreateJobIn(material_id="material-1", style_id="anime")

    assert single.style_id == "cinematic"
    assert batch.style_id == "anime"


def test_video_schemas_accept_style_id() -> None:
    single = VideoJobCreateIn(
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="group-1",
        style_id="cinematic",
    )
    batch = VideoJobBatchCreateIn(
        script_id="script-1",
        episode_id="episode-1",
        shot_group_ids=["group-1"],
        style_id="anime",
    )

    assert single.style_id == "cinematic"
    assert batch.style_id == "anime"


def test_sanitize_input_json_exposes_style_id() -> None:
    assert _sanitize_input_json(
        {
            "prompt": "raw",
            "resolved_prompts": {"final": "resolved"},
            "negative_prompt": "internal negative prompt",
            "style_id": "cinematic",
            "reservation_id": "internal-secret",
        }
    ) == {
        "prompt": "raw",
        "resolved_prompts": {"final": "resolved"},
        "style_id": "cinematic",
    }


@pytest.mark.asyncio
async def test_generation_jobs_persist_style_id_and_include_it_in_idempotency(
    monkeypatch,
) -> None:
    from app.services import generation as generation_service
    from tests.test_generation import _AsyncSessionAdapter, _create_payload, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw)
    db = _AsyncSessionAdapter(raw)

    async def allow_access(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(generation_service, "check_script_access", allow_access)
    monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

    single = await generation_service.create_generation_job(
        db,
        user=seeded.user,
        payload=_create_payload(prompt="single prompt", style_id="cinematic"),
        idempotency_key="style-single",
    )

    assert single.input_json["style_id"] == "cinematic"
    expected_single_prompt, expected_single_negative = apply_style(
        "cinematic", "single prompt", "image"
    )
    expected_single_prompt, expected_single_negative = (
        generation_service._apply_character_sheet_constraints(
            expected_single_prompt,
            expected_single_negative,
            "character",
            "16:9",
        )
    )
    assert single.input_json["resolved_prompts"]["final"] == expected_single_prompt
    assert single.input_json["negative_prompt"] == expected_single_negative
    with pytest.raises(ConflictError):
        await generation_service.create_generation_job(
            db,
            user=seeded.user,
            payload=_create_payload(prompt="single prompt", style_id="anime"),
            idempotency_key="style-single",
        )

    batch_payload = BatchCreateJobIn(
        material_id=seeded.material.id,
        subject_ids=[seeded.subject.id],
        prompts={seeded.subject.id: "batch prompt"},
        style_id="anime",
    )
    created, skipped, failed = await generation_service.create_batch_generation_jobs(
        db,
        user=seeded.user,
        payload=batch_payload,
        idempotency_key="style-batch",
    )

    assert skipped == []
    assert failed == []
    assert created[0].input_json["style_id"] == "anime"
    expected_batch_prompt, expected_batch_negative = apply_style(
        "anime", "batch prompt", "image"
    )
    expected_batch_prompt, expected_batch_negative = (
        generation_service._apply_character_sheet_constraints(
            expected_batch_prompt,
            expected_batch_negative,
            "character",
            "3:4",
        )
    )
    assert created[0].input_json["resolved_prompts"]["final"] == expected_batch_prompt
    assert created[0].input_json["negative_prompt"] == expected_batch_negative
    assert created[0].input_json["aspect"] == "3:4"

    original_find_style = generation_service.find_style
    monkeypatch.setattr(generation_service, "find_style", lambda _style_id: None)
    replayed, replayed_skipped, replayed_failed = (
        await generation_service.create_batch_generation_jobs(
            db,
            user=seeded.user,
            payload=batch_payload,
            idempotency_key="style-batch",
        )
    )
    assert [job.job_id for job in replayed] == [job.job_id for job in created]
    assert replayed_skipped == skipped
    assert replayed_failed == failed
    monkeypatch.setattr(generation_service, "find_style", original_find_style)

    with pytest.raises(ConflictError):
        await generation_service.create_batch_generation_jobs(
            db,
            user=seeded.user,
            payload=batch_payload.model_copy(update={"style_id": "cinematic"}),
            idempotency_key="style-batch",
        )


@pytest.mark.asyncio
async def test_video_job_persists_style_id_and_includes_it_in_idempotency(monkeypatch) -> None:
    from app.models.task_dispatch_outbox import TaskDispatchOutbox
    from app.services import video_generation
    from tests.test_video_generation import _AsyncSessionAdapter, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw)
    db = _AsyncSessionAdapter(raw)

    async def fake_enqueue(db, **kwargs):
        row = TaskDispatchOutbox(
            id="dispatch-style-video",
            task_type=kwargs["task_type"],
            job_id=kwargs["job_id"],
            attempt=kwargs["attempt"],
            payload=kwargs["payload"],
            status="pending",
        )
        db.add(row)
        return row

    monkeypatch.setattr(video_generation, "enqueue_task", fake_enqueue)

    job = await video_generation.create_video_job(
        db,
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        shot_group_id=seeded.shot_group.shot_group_id,
        user_id=seeded.user.user_id,
        idempotency_key="style-video",
        style_id="cinematic",
    )

    assert job.input_json["style_id"] == "cinematic"
    with pytest.raises(ConflictError):
        await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="style-video",
            style_id="anime",
        )

    same_style = await video_generation.create_video_job(
        db,
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        shot_group_id=seeded.shot_group.shot_group_id,
        user_id=seeded.user.user_id,
        idempotency_key="style-video-same-style",
        style_id="cinematic",
    )
    assert same_style.job_id == job.job_id

    with pytest.raises(ConflictError, match="不同风格"):
        await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="style-video-different-style",
            style_id="anime",
        )


@pytest.mark.asyncio
async def test_video_apis_forward_style_id(monkeypatch) -> None:
    from app.api.v1 import video_generation as api
    from tests.test_video_generation import (
        _add_worker_job,
        _AsyncSessionAdapter,
        _make_db,
        _seed,
    )

    _, raw = _make_db()
    seeded = _seed(raw)
    db = _AsyncSessionAdapter(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-style-forward",
        reservation_id="reservation-style-forward",
    )
    create = AsyncMock(return_value=job)
    monkeypatch.setattr("app.services.video_generation.create_video_job", create)
    monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))

    await api.create_video_generation_job(
        VideoJobCreateIn(
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            style_id="cinematic",
        ),
        db=db,
        user=seeded.user,
        idempotency_key="style-forward-single",
    )

    assert create.await_args.kwargs["style_id"] == "cinematic"
    create.reset_mock()

    await api.batch_create_video_generation_jobs(
        VideoJobBatchCreateIn(
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_ids=[seeded.shot_group.shot_group_id],
            style_id="anime",
        ),
        db=db,
        user=seeded.user,
        idempotency_key="style-forward-batch",
    )

    assert create.await_args.kwargs["style_id"] == "anime"


def test_prompt_optimize_in_rejects_oversized_prompt() -> None:
    """PromptOptimizeIn.prompt 超过 2000 字符时必须校验失败。"""
    oversized = "a" * 2001
    with pytest.raises(ValidationError):
        PromptOptimizeIn(prompt=oversized, target="image")


def test_prompt_optimize_in_accepts_max_length_prompt() -> None:
    """正好 2000 字符应当通过校验，防止边界 off-by-one。"""
    payload = PromptOptimizeIn(prompt="a" * 2000, target="image")
    assert len(payload.prompt) == 2000


@pytest.mark.asyncio
async def test_optimize_rate_limit(client) -> None:
    """每用户每分钟 20 次限流：第 21 次必须被 BadRequestError(400) 拒绝。"""
    from app.api.v1 import prompt_styles as api
    from app.core.auth import get_current_user

    api._OPTIMIZE_CALLS.clear()

    async def override_user():
        return SimpleNamespace(user_id="rate-user", enabled=True, is_admin=False)

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user

    async def _fake_optimize_prompt(raw_prompt, style_id, target, **kwargs):
        return {
            "optimized_prompt": raw_prompt or "default",
            "negative_prompt": "",
            "style_id": None,
            "style_name": None,
            "original_prompt": raw_prompt,
        }

    import app.api.v1.prompt_styles as _api_module  # noqa: F811

    original = _api_module.optimize_prompt
    _api_module.optimize_prompt = _fake_optimize_prompt
    try:
        payload = {"prompt": "a quiet street", "target": "image"}
        for _ in range(20):
            resp = await client.post("/v3api/prompt-optimize", json=payload)
            assert resp.status_code == 200, resp.text

        limited = await client.post("/v3api/prompt-optimize", json=payload)
        assert limited.status_code == 400
        assert "频繁" in limited.json()["detail"]
    finally:
        _api_module.optimize_prompt = original
        api._OPTIMIZE_CALLS.clear()
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_optimize_system_prompt_preserves_mention_marker() -> None:
    """系统提示词必须显式要求保留 @数字 主体引用标记。"""
    provider = SimpleNamespace(chat=AsyncMock(return_value="follow @1 through the rain"))
    result = await optimize_prompt("follow @1", None, "image", provider=provider)
    assert "@1" in result["optimized_prompt"]

    video_provider = SimpleNamespace(chat=AsyncMock(return_value="@2 walks in rain"))
    video_result = await optimize_prompt("@2 walks", None, "video", provider=video_provider)
    assert "@2" in video_result["optimized_prompt"]
