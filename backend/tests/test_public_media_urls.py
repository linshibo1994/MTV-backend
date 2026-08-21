"""公开 CDN 媒体 URL 的 API 输出契约。"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.schemas.asset import AssetEntryOut, CandidateOut
from app.schemas.generation import GenerationJobOut
from app.schemas.materials import CanvasNodeOut, ImageAssetOut
from app.schemas.script import EpisodeOut, ScriptUploadOut
from app.schemas.shot import CoverOut, ReferenceFramesOut
from app.schemas.timeline import RenderJobOut, RenderStatusOut
from app.schemas.video import VideoJobOut


@pytest.fixture
def public_oss(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.providers import storage as storage_module

    monkeypatch.setattr(
        storage_module,
        "settings",
        SimpleNamespace(
            storage_driver="oss",
            aliyun_oss_public_url="https://cdn-ai-public.commind.com",
        ),
    )


def test_resource_output_models_expose_cdn_urls(public_oss) -> None:
    now = datetime.now(UTC)
    image_key = "MTV/upload_images/material-1/portrait.png"
    video_key = "MTV/generate_images/video/job-1.mp4"

    entry = AssetEntryOut(
        asset_id="asset-1",
        storage_key=image_key,
        mime_type="image/png",
        size_bytes=1,
        source="upload_images",
        kind="image",
    )
    candidate = CandidateOut(
        candidate_id="candidate-1",
        storage_key=image_key,
        mime_type="image/png",
        size_bytes=1,
        source="upload_images",
        kind="image",
    )
    image = ImageAssetOut(
        image_id="asset-1",
        storage_key=f"/{image_key}",
        mime_type="image/png",
        size_bytes=1,
        source="upload_images",
    )
    script_upload = ScriptUploadOut(
        script_id="script-1",
        source_episode_id="episode-1",
        filename="story.txt",
        script_file_storage_key="MTV/upload_scripts/script-1/story.txt",
        auto=True,
        status="queued",
    )
    episode = EpisodeOut(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_file_storage_key="MTV/upload_scripts/script-1/story.txt",
        schema_version=1,
        episode_version=1,
        client_save_seq=0,
        created_at=now,
        updated_at=now,
    )
    render_status = RenderStatusOut(
        job_id="render-1",
        status="succeeded",
        storage_key=video_key,
        output_storage_key=video_key,
    )
    render_job = RenderJobOut(
        job_id="render-1",
        status="starting",
        output_storage_key=video_key,
    )
    frames = ReferenceFramesOut(first_frame=image_key, last_frame=image_key)
    cover = CoverOut(script_id="script-1", cover=image_key, mime_type="image/png")

    assert entry.public_url == f"https://cdn-ai-public.commind.com/{image_key}"
    assert candidate.public_url == f"https://cdn-ai-public.commind.com/{image_key}"
    assert image.public_url == f"https://cdn-ai-public.commind.com/{image_key}"
    assert script_upload.script_file_public_url == (
        "https://cdn-ai-public.commind.com/MTV/upload_scripts/script-1/story.txt"
    )
    assert episode.script_file_public_url == script_upload.script_file_public_url
    assert render_status.public_url == f"https://cdn-ai-public.commind.com/{video_key}"
    assert render_status.output_public_url == render_status.public_url
    assert render_job.output_public_url == render_status.public_url
    assert frames.first_frame_public_url == f"https://cdn-ai-public.commind.com/{image_key}"
    assert frames.last_frame_public_url == f"https://cdn-ai-public.commind.com/{image_key}"
    assert cover.public_url == f"https://cdn-ai-public.commind.com/{image_key}"


def test_job_and_canvas_json_outputs_append_cdn_urls(public_oss) -> None:
    image_key = "MTV/generate_images/material-1/portrait.png"
    video_key = "MTV/generate_images/video/job-1.mp4"
    node = CanvasNodeOut(
        id="node-1",
        type="imageUpload",
        data={
            "prompt": "人物设定图",
            "imageOutputs": [
                {"status": "succeeded", "storageKey": image_key},
            ],
        },
    )
    generation = GenerationJobOut(
        job_id="generation-1",
        subject_id="subject-1",
        status="succeeded",
        progress=100,
        output_json={
            "imageOutputs": [
                {"status": "succeeded", "storageKey": image_key},
            ],
        },
    )
    video = VideoJobOut(
        job_id="video-1",
        script_id="script-1",
        episode_id="episode-1",
        scene_no=1,
        unit_id="group:1",
        unit_runtime_key="runtime:1",
        provider_key="neolink",
        job_type="all_reference",
        status="succeeded",
        progress=100,
        output_json={"video_storage_key": video_key},
    )

    assert node.data["imageOutputs"][0]["publicUrl"] == (
        f"https://cdn-ai-public.commind.com/{image_key}"
    )
    generation_output = json.loads(generation.model_dump(mode="json")["output_json"])
    assert generation_output["imageOutputs"][0]["publicUrl"] == (
        f"https://cdn-ai-public.commind.com/{image_key}"
    )
    assert video.output_json["video_public_url"] == (
        f"https://cdn-ai-public.commind.com/{video_key}"
    )
