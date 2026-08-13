"""风格目录、提示词优化与 style_id 契约测试。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.api.v1.generation_jobs import _sanitize_input_json
from app.core.errors import BadRequestError, ConflictError
from app.schemas.generation import BatchCreateJobIn, CreateJobIn
from app.schemas.materials import build_subject_prompt
from app.schemas.prompt_style import PromptOptimizeIn
from app.schemas.video import VideoJobBatchCreateIn, VideoJobCreateIn
from app.services import prompt_style
from app.services.prompt_style import apply_style, optimize_prompt, strip_image_style


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

    positive, negative = apply_style(
        "cyberpunk",
        raw_prompt,
        "image",
        subject_type="character",
    )

    assert raw_prompt in positive
    assert "unmistakable cyberpunk visual-development rendering" in positive
    assert "dominant cyan-magenta neon color language" in positive
    assert "not a mostly photorealistic image with token neon accents" in positive
    assert "character model sheet" in positive
    assert "consistent turnaround rendering" in positive
    assert "dense cyberpunk world" not in positive
    assert "preserve every required view" in positive
    assert "clean empty background" not in negative
    assert "historical costume" not in negative
    assert "pastoral daylight" not in negative
    assert "low detail" in negative


def test_apply_style_front_loads_comic_book_priority_for_structured_subject_design() -> None:
    raw_prompt = build_subject_prompt(
        "钱王",
        "character",
        "吴越国君主，中年男性，头戴王冠，身穿华贵王袍。"
        "皮肤纹理、发丝、织锦和玉石需要清晰可辨，使用 85mm 人像视角与白色摄影棚背景。",
    )

    positive, negative = apply_style(
        "comic-book",
        raw_prompt,
        "image",
        subject_type="character",
    )

    priority, remaining = positive.split("\n\n", 1)
    assert priority.startswith("【风格优先级】")
    assert "unmistakable 2D American comic-book illustration" in priority
    assert "halftone texture" in priority
    assert "must remain visually dominant" in priority
    assert remaining.startswith(raw_prompt)
    assert positive.count("【风格优先级】") == 1
    assert positive.count("【风格渲染约束】") == 1
    assert strip_image_style(positive) == raw_prompt
    assert "photorealistic" in negative
    assert "live action" in negative
    assert "camera photograph" in negative


def test_apply_style_replaces_existing_style_for_real_scene_and_is_idempotent() -> None:
    raw_prompt = build_subject_prompt(
        "贫困乡村院落",
        "scene",
        "20 世纪末中国北方乡村土坯院落，白天，破旧木门和泥土地面，纪实摄影写实风格",
    )
    prompt_with_old_styles = (
        f"{raw_prompt}\n\n【风格渲染约束】photorealistic studio rendering."
        "\n\n【风格渲染约束】soft watercolor illustration."
    )

    first = apply_style(
        "cyberpunk",
        prompt_with_old_styles,
        "image",
        subject_type="background",
    )
    second = apply_style(
        "cyberpunk",
        first[0],
        "image",
        subject_type="scene",
    )

    assert first == second
    assert first[0].count("【风格渲染约束】") == 1
    style_section = first[0].split("【风格渲染约束】", 1)[1].lower()
    assert "unmistakable cyberpunk visual-development rendering" in style_section
    assert "full-frame environment concept scene" in style_section
    assert "spatial layout" in style_section
    assert "architecture" in style_section
    for character_only_term in ("turnaround", "pose", "clean background"):
        assert character_only_term not in style_section


def test_resolve_image_prompt_retypes_legacy_wrapped_scene_when_switching_style() -> None:
    from app.services import generation as generation_service

    raw_prompt = build_subject_prompt(
        "贫困乡村院落",
        "scene",
        "20 世纪末中国北方乡村土坯院落，保留土坯和木材的真实材质",
    )
    old_style = prompt_style.find_style("photoreal")
    selected_style = prompt_style.find_style("cyberpunk")
    assert old_style is not None
    assert selected_style is not None
    legacy_wrapped = str(old_style["prompt_template"]).replace("{prompt}", raw_prompt)

    positive, negative, style_id = generation_service._resolve_image_prompt(
        legacy_wrapped,
        selected_style,
        subject_type="scene",
    )

    assert style_id == "cyberpunk"
    assert positive.count("【风格渲染约束】") == 1
    assert "photorealistic editorial photograph" not in positive
    assert "dense cyberpunk world" not in positive
    assert raw_prompt in positive
    style_section = positive.split("【风格渲染约束】", 1)[1].lower()
    assert "full-frame environment concept scene" in style_section
    for character_only_term in ("turnaround", "pose", "clean background"):
        assert character_only_term not in style_section
    assert negative


def test_resolve_image_prompt_replaces_legacy_style_for_plain_prompt() -> None:
    from app.services import generation as generation_service

    raw_prompt = "一盏表面锈蚀的旧煤油灯，产品静物构图"
    old_prompt, _ = apply_style("anime", raw_prompt, "image")
    selected_style = prompt_style.find_style("cyberpunk")
    assert selected_style is not None

    positive, negative, style_id = generation_service._resolve_image_prompt(
        old_prompt,
        selected_style,
        subject_type="prop",
    )

    expected = str(selected_style["prompt_template"]).replace("{prompt}", raw_prompt)
    assert positive == expected
    assert "high-quality Japanese anime" not in positive
    assert negative == selected_style["negative_prompt"]
    assert style_id == "cyberpunk"


def test_apply_style_uses_product_direction_for_prop() -> None:
    raw_prompt = build_subject_prompt(
        "旧煤油灯",
        "prop",
        "20 世纪末乡村使用的铁皮煤油灯，表面锈蚀，玻璃灯罩有烟熏痕迹",
    )

    positive, negative = apply_style(
        "anime",
        raw_prompt,
        "image",
        subject_type="prop",
    )

    assert "high-quality Japanese anime rendering" in positive
    assert "product-design reference sheet" in positive
    assert "multi-view object rendering" in positive
    assert "character model sheet" not in positive
    assert negative.startswith("photorealistic")


def test_strip_image_style_removes_legacy_wrapper_and_style_section() -> None:
    raw_prompt = build_subject_prompt(
        "旧煤油灯",
        "prop",
        "20 世纪末乡村使用的铁皮煤油灯，表面锈蚀",
    )
    styled_prompt, _ = apply_style(
        "anime",
        raw_prompt,
        "image",
        subject_type="prop",
    )
    legacy_style = prompt_style.find_style("photoreal")
    assert legacy_style is not None
    legacy_wrapped = str(legacy_style["prompt_template"]).replace(
        "{prompt}", styled_prompt
    )

    assert strip_image_style(legacy_wrapped) == raw_prompt


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
    assert "unmistakable 2D American comic-book illustration" in system
    assert "禁止加入这些冲突特征：photorealistic" in system
    assert result["optimized_prompt"].startswith("unmistakable 2D American comic-book illustration")
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
    assert result["optimized_prompt"] == raw_prompt
    assert result["original_prompt"] == raw_prompt
    assert "【4 体态与道具】" in result["optimized_prompt"]
    assert "真人摄影写实" not in raw_prompt
    assert result["negative_prompt"] == ""
    assert result["style_id"] == "anime"
    assert result["style_name"] == "日式动漫"


@pytest.mark.asyncio
async def test_optimize_subject_design_prompt_removes_baked_style_section() -> None:
    raw_prompt = build_subject_prompt("老农", "character", "中国乡村老年男性")
    styled_prompt, _ = apply_style(
        "photoreal",
        raw_prompt,
        "image",
        subject_type="character",
    )
    provider = SimpleNamespace(chat=AsyncMock(return_value="不应调用供应商"))

    result = await optimize_prompt(
        styled_prompt,
        "cyberpunk",
        "image",
        provider=provider,
    )

    provider.chat.assert_not_awaited()
    assert result["optimized_prompt"] == raw_prompt
    assert result["original_prompt"] == raw_prompt
    assert "【风格渲染约束】" not in result["optimized_prompt"]
    assert result["negative_prompt"] == ""
    assert result["style_id"] == "cyberpunk"


@pytest.mark.asyncio
async def test_structured_style_handles_mixed_legacy_wrapper_and_preserves_next_constraint() -> None:
    raw_prompt = build_subject_prompt(
        "贫困乡村院落",
        "scene",
        "20 世纪末中国北方乡村土坯院落",
    )
    mixed_inner = (
        f"{raw_prompt}\n\n【风格渲染约束】旧版写实摄影风格。"
        "\n【额外硬约束】必须保留土坯墙和破旧木门。"
    )
    legacy_style = prompt_style.find_style("photoreal")
    assert legacy_style is not None
    mixed_prompt = str(legacy_style["prompt_template"]).replace("{prompt}", mixed_inner)

    positive, _ = apply_style(
        "cyberpunk",
        mixed_prompt,
        "image",
        subject_type="scene",
    )
    optimized = await optimize_prompt(mixed_prompt, "cyberpunk", "image")

    assert positive.count("【风格渲染约束】") == 1
    assert "photorealistic editorial photograph" not in positive
    assert "【额外硬约束】必须保留土坯墙和破旧木门。" in positive
    assert "【风格渲染约束】" not in optimized["optimized_prompt"]
    assert "photorealistic editorial photograph" not in optimized["optimized_prompt"]
    assert "【额外硬约束】必须保留土坯墙和破旧木门。" in optimized["optimized_prompt"]
    assert optimized["original_prompt"] == optimized["optimized_prompt"]


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
    batch = BatchCreateJobIn(
        material_id="material-1",
        style_id="anime",
        style_ids={"subject-1": "cyberpunk", "subject-2": None},
    )

    assert single.style_id == "cinematic"
    assert batch.style_id == "anime"
    assert batch.style_ids == {"subject-1": "cyberpunk", "subject-2": None}


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
async def test_optimized_structured_prompt_keeps_comic_book_priority_until_job_creation(monkeypatch) -> None:
    from app.services import generation as generation_service
    from tests.test_generation import _AsyncSessionAdapter, _create_payload, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw)
    db = _AsyncSessionAdapter(raw)
    monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())
    raw_prompt = build_subject_prompt(
        "钱王",
        "character",
        "吴越国君主，中年男性，头戴王冠，身穿华贵王袍。"
        "需要保留皮肤纹理、发丝、织锦和玉石的细节，使用白色摄影棚背景。",
    )

    optimized = await optimize_prompt(raw_prompt, "comic-book", "image")
    assert optimized["optimized_prompt"] == raw_prompt
    assert optimized["style_id"] == "comic-book"

    job = await generation_service.create_generation_job(
        db,
        user=seeded.user,
        payload=_create_payload(
            prompt=optimized["optimized_prompt"],
            style_id=optimized["style_id"],
            model="qwen-image-2.0",
            aspect="3:4",
            count=1,
        ),
        idempotency_key="optimized-structured-comic-book",
    )

    final_prompt = job.input_json["resolved_prompts"]["final"]
    priority, _ = final_prompt.split("\n\n", 1)
    assert priority.startswith("【风格优先级】")
    assert "unmistakable 2D American comic-book illustration" in priority
    assert "must remain visually dominant" in priority
    assert "【人物设定图构图硬约束 v2】" in final_prompt
    assert job.input_json["style_id"] == "comic-book"
    assert "photorealistic" in job.input_json["negative_prompt"]
    assert "live action" in job.input_json["negative_prompt"]


@pytest.mark.asyncio
async def test_single_generation_applies_scene_specific_cyberpunk_style(monkeypatch) -> None:
    from app.services import generation as generation_service
    from tests.test_generation import _AsyncSessionAdapter, _create_payload, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw, subject_type="scene")
    monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())
    scene_prompt = build_subject_prompt(
        "贫困乡村院落",
        "scene",
        "20 世纪末中国北方乡村土坯院落，白天，破旧木门和泥土地面，纪实摄影写实风格",
    )

    job = await generation_service.create_generation_job(
        _AsyncSessionAdapter(raw),
        user=seeded.user,
        payload=_create_payload(
            prompt=scene_prompt,
            style_id="cyberpunk",
            aspect="auto",
        ),
        idempotency_key="single-scene-cyberpunk",
    )

    final_prompt = job.input_json["resolved_prompts"]["final"]
    style_section = final_prompt.split("【风格渲染约束】", 1)[1].lower()
    assert final_prompt.count("【风格渲染约束】") == 1
    assert "full-frame environment concept scene" in style_section
    assert "unmistakable cyberpunk visual-development rendering" in style_section
    for character_only_term in ("turnaround", "pose", "clean background"):
        assert character_only_term not in style_section
    assert job.input_json["style_id"] == "cyberpunk"
    assert job.input_json["negative_prompt"]
    assert job.input_json["aspect"] == "16:9"


@pytest.mark.asyncio
async def test_batch_generation_applies_per_subject_style_mapping_and_is_idempotent(
    monkeypatch,
) -> None:
    from app.models.subject import Subject
    from app.services import generation as generation_service
    from tests.test_generation import _AsyncSessionAdapter, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw)
    extra_subjects = [
        Subject(
            id="subject-scene",
            script_id=seeded.script.id,
            type="scene",
            name="乡村院落",
            description="20 世纪末中国北方乡村土坯院落",
            episode_nos=[1],
            review_status="not_designed",
        ),
        Subject(
            id="subject-prop-none",
            script_id=seeded.script.id,
            type="prop",
            name="旧煤油灯",
            description="铁皮煤油灯，表面锈蚀",
            episode_nos=[1],
            review_status="not_designed",
        ),
        Subject(
            id="subject-prop-global",
            script_id=seeded.script.id,
            type="prop",
            name="旧瓷碗",
            description="有缺口和使用痕迹的白瓷碗",
            episode_nos=[1],
            review_status="not_designed",
        ),
    ]
    raw.add_all(extra_subjects)
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

    no_style_base_prompt = build_subject_prompt(
        "旧煤油灯",
        "prop",
        "铁皮煤油灯，表面锈蚀",
    )
    no_style_styled_prompt, _ = apply_style(
        "anime",
        no_style_base_prompt,
        "image",
        subject_type="prop",
    )
    legacy_style = prompt_style.find_style("photoreal")
    assert legacy_style is not None
    no_style_legacy_prompt = str(legacy_style["prompt_template"]).replace(
        "{prompt}", no_style_styled_prompt
    )
    prompts = {
        seeded.subject.id: build_subject_prompt("老农", "character", "中国乡村老年男性"),
        "subject-scene": build_subject_prompt(
            "乡村院落",
            "scene",
            "20 世纪末中国北方乡村土坯院落",
        ),
        "subject-prop-none": no_style_legacy_prompt,
        "subject-prop-global": build_subject_prompt(
            "旧瓷碗",
            "prop",
            "有缺口和使用痕迹的白瓷碗",
        ),
    }
    payload = BatchCreateJobIn(
        material_id=seeded.material.id,
        subject_ids=list(prompts),
        prompts=prompts,
        style_id="cinematic",
        style_ids={
            seeded.subject.id: "anime",
            "subject-scene": "cyberpunk",
            "subject-prop-none": None,
        },
        count=1,
    )

    created, skipped, failed = await generation_service.create_batch_generation_jobs(
        db,
        user=seeded.user,
        payload=payload,
        idempotency_key="batch-subject-style-map",
    )

    assert skipped == []
    assert failed == []
    by_subject = {job.subject_id: job for job in created}
    assert by_subject[seeded.subject.id].input_json["style_id"] == "anime"
    assert "character model sheet" in by_subject[seeded.subject.id].input_json[
        "resolved_prompts"
    ]["final"]
    scene_job = by_subject["subject-scene"]
    assert scene_job.input_json["style_id"] == "cyberpunk"
    assert "full-frame environment concept scene" in scene_job.input_json[
        "resolved_prompts"
    ]["final"]
    no_style_job = by_subject["subject-prop-none"]
    assert no_style_job.input_json["style_id"] is None
    no_style_final_prompt = no_style_job.input_json["resolved_prompts"]["final"]
    assert no_style_final_prompt == no_style_base_prompt
    assert "【风格渲染约束】" not in no_style_final_prompt
    assert "photorealistic editorial photograph" not in no_style_final_prompt
    fallback_job = by_subject["subject-prop-global"]
    assert fallback_job.input_json["style_id"] == "cinematic"
    assert "product-design reference sheet" in fallback_job.input_json[
        "resolved_prompts"
    ]["final"]

    original_find_style = generation_service.find_style
    monkeypatch.setattr(generation_service, "find_style", lambda _style_id: None)
    replayed, replayed_skipped, replayed_failed = (
        await generation_service.create_batch_generation_jobs(
            db,
            user=seeded.user,
            payload=payload,
            idempotency_key="batch-subject-style-map",
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
            payload=payload.model_copy(
                update={
                    "style_ids": {
                        **(payload.style_ids or {}),
                        "subject-scene": "watercolor",
                    }
                }
            ),
            idempotency_key="batch-subject-style-map",
        )


@pytest.mark.asyncio
async def test_batch_generation_api_returns_per_subject_styles(client, monkeypatch) -> None:
    import json

    from app.core.auth import get_current_user
    from app.core.database import get_db
    from app.models.subject import Subject
    from app.services import generation as generation_service
    from tests.test_generation import _AsyncSessionAdapter, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw)
    no_style_subject = Subject(
        id="subject-api-no-style",
        script_id=seeded.script.id,
        type="prop",
        name="旧煤油灯",
        description="铁皮煤油灯，表面锈蚀",
        episode_nos=[1],
        review_status="not_designed",
    )
    raw.add(no_style_subject)
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

    async def override_db():
        yield db

    async def override_user():
        yield seeded.user

    client._app.dependency_overrides[get_db] = override_db
    client._app.dependency_overrides[get_current_user] = override_user

    response = await client.post(
        "/v3api/generation-jobs/batch",
        headers={"Idempotency-Key": "batch-style-api-contract"},
        json={
            "material_id": seeded.material.id,
            "subject_ids": [seeded.subject.id, no_style_subject.id],
            "prompts": {
                seeded.subject.id: build_subject_prompt(
                    "老农",
                    "character",
                    "中国乡村老年男性",
                ),
                no_style_subject.id: build_subject_prompt(
                    "旧煤油灯",
                    "prop",
                    "铁皮煤油灯，表面锈蚀",
                ),
            },
            "style_id": "cinematic",
            "style_ids": {
                seeded.subject.id: "anime",
                no_style_subject.id: None,
            },
        },
    )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["skipped"] == []
    assert body["failed"] == []
    by_subject = {job["subject_id"]: job for job in body["created"]}
    styled_input = json.loads(by_subject[seeded.subject.id]["input_json"])
    assert styled_input["style_id"] == "anime"
    assert "【风格渲染约束】" in styled_input["resolved_prompts"]["final"]
    no_style_input = json.loads(by_subject[no_style_subject.id]["input_json"])
    assert no_style_input["style_id"] is None
    assert "【风格渲染约束】" not in no_style_input["resolved_prompts"]["final"]


@pytest.mark.asyncio
async def test_batch_generation_rejects_invalid_style_mapping_before_creating_jobs() -> None:
    from sqlalchemy import select

    from app.models.generation_batch_request import GenerationBatchRequest
    from app.models.generation_job import GenerationJob
    from app.services import generation as generation_service
    from tests.test_generation import _AsyncSessionAdapter, _make_db, _seed

    _, raw = _make_db()
    seeded = _seed(raw)
    db = _AsyncSessionAdapter(raw)

    with pytest.raises(BadRequestError, match="style_ids 包含非目标主体"):
        await generation_service.create_batch_generation_jobs(
            db,
            user=seeded.user,
            payload=BatchCreateJobIn(
                material_id=seeded.material.id,
                subject_ids=[seeded.subject.id],
                style_ids={"subject-other": "anime"},
            ),
            idempotency_key="batch-style-map-unknown-subject",
        )

    with pytest.raises(BadRequestError, match="未知风格"):
        await generation_service.create_batch_generation_jobs(
            db,
            user=seeded.user,
            payload=BatchCreateJobIn(
                material_id=seeded.material.id,
                subject_ids=[seeded.subject.id],
                style_ids={seeded.subject.id: "missing-style"},
            ),
            idempotency_key="batch-style-map-unknown-style",
        )

    assert raw.scalars(select(GenerationJob)).all() == []
    assert raw.scalars(select(GenerationBatchRequest)).all() == []


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
