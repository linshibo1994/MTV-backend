"""NeoLink Seedance 视频供应商适配测试。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from app.core.config import Settings
from app.providers.registry import ProviderConfigurationError
from app.providers.video_gen import (
    VideoCancellationOutcome,
    VideoGenerationRemoteTerminalError,
    VideoGenerationRequest,
)
from app.providers.video_gen_neolink import NeolinkVideoGenProvider
from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

_DOCUMENTED_VIDEO_URL = (
    "https://ark-acg-cn-beijing.tos-cn-beijing.volces.com/"
    "doubao-seedance-2-0/generated.mp4?X-Tos-Expires=86400"
)


def _settings(**kwargs) -> Settings:
    values = {
        "video_gen_provider": "neolink",
        "neolink_api_key": "test-neolink-key",
        "neolink_base_url": "https://neolink.com/api",
    }
    values.update(kwargs)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


def _patch_settings(monkeypatch, configured: Settings | None = None) -> Settings:
    from app.providers import video_gen_neolink

    configured = configured or _settings()
    monkeypatch.setattr(video_gen_neolink, "settings", configured)
    return configured


def test_neolink_reuses_volcengine_ark_generation_chain() -> None:
    provider = NeolinkVideoGenProvider()

    assert isinstance(provider, VolcengineVideoGenProvider)
    assert provider.provider_key == "neolink"


@pytest.mark.parametrize(
    ("configured_url", "expected_url"),
    [
        ("https://neolink.com/api", "https://neolink.com/api/v1"),
        ("https://neolink.com/api/", "https://neolink.com/api/v1"),
        ("https://neolink.com/api/v1", "https://neolink.com/api/v1"),
    ],
)
def test_neolink_resolves_dedicated_key_and_normalizes_ark_base_url(
    monkeypatch,
    configured_url: str,
    expected_url: str,
) -> None:
    _patch_settings(monkeypatch, _settings(neolink_base_url=configured_url))
    provider = NeolinkVideoGenProvider()

    assert provider._resolve_ark_api_key() == "test-neolink-key"
    assert provider._resolve_ark_base_url() == expected_url


def test_neolink_dedicated_pair_wins_over_legacy_generic_video_config(monkeypatch) -> None:
    _patch_settings(
        monkeypatch,
        _settings(
            video_gen_api_key="legacy-provider-key",
            video_gen_base_url="https://legacy-provider.example/api/v3",
        ),
    )
    provider = NeolinkVideoGenProvider()

    assert provider._resolve_ark_api_key() == "test-neolink-key"
    assert provider._resolve_ark_base_url() == "https://neolink.com/api/v1"


def test_neolink_missing_dedicated_key_fails_before_sdk_call(monkeypatch) -> None:
    _patch_settings(
        monkeypatch,
        _settings(neolink_api_key="", video_gen_api_key=""),
    )
    provider = NeolinkVideoGenProvider()
    provider._build_client = AsyncMock(side_effect=AssertionError("缺 Key 时不得创建 Ark 客户端"))

    with pytest.raises(RuntimeError, match="NeoLink.*API Key"):
        provider._check_keys()

    provider._build_client.assert_not_called()


@pytest.mark.asyncio
async def test_neolink_cancel_reports_unsupported_without_building_client(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    provider = NeolinkVideoGenProvider()
    provider._build_client = AsyncMock(side_effect=AssertionError("NeoLink 取消不得创建 Ark 客户端"))

    result = await provider.cancel("neolink-task-1")

    assert result.provider_task_id == "neolink-task-1"
    assert result.outcome is VideoCancellationOutcome.UNSUPPORTED
    assert result.detail
    provider._build_client.assert_not_called()


@pytest.mark.asyncio
async def test_neolink_remote_cancel_never_calls_ark_delete() -> None:
    tasks = SimpleNamespace(
        delete=AsyncMock(side_effect=AssertionError("NeoLink 不支持 Ark tasks.delete"))
    )

    result = await NeolinkVideoGenProvider()._cancel_remote_task(tasks, "neolink-task-2")

    assert result.provider_task_id == "neolink-task-2"
    assert result.outcome is VideoCancellationOutcome.UNSUPPORTED
    tasks.delete.assert_not_awaited()


def test_neolink_extracts_video_url_from_documented_dict_content() -> None:
    response = {
        "id": "neolink-task-succeeded",
        "status": "succeeded",
        "content": {"video_url": _DOCUMENTED_VIDEO_URL},
    }

    assert NeolinkVideoGenProvider._extract_video_url(response) == _DOCUMENTED_VIDEO_URL


@pytest.mark.asyncio
async def test_neolink_submit_uses_documented_create_contract(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    tasks = SimpleNamespace(
        create=AsyncMock(return_value=SimpleNamespace(id="neolink-task-created")),
        get=AsyncMock(
            return_value={
                "id": "neolink-task-created",
                "status": "succeeded",
                "content": {"video_url": _DOCUMENTED_VIDEO_URL},
            }
        ),
    )
    client = SimpleNamespace(content_generation=SimpleNamespace(tasks=tasks))
    provider = NeolinkVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_close_client_safely", AsyncMock())
    on_task_created = AsyncMock()

    video_url, metadata = await provider._submit_and_poll(
        "doubao-seedance-2-0-260128",
        "雨后的竹林，镜头缓慢向前移动",
        ["https://storage.example/reference/character.png?signature=image"],
        ["https://storage.example/reference/voice.mp3?signature=audio"],
        "https://storage.example/reference/first-frame.png?signature=frame",
        lambda *_args: None,
        video_urls=["https://storage.example/reference/motion.mp4?signature=video"],
        duration_seconds=8,
        on_task_created=on_task_created,
    )

    assert video_url == _DOCUMENTED_VIDEO_URL
    assert metadata["task_id"] == "neolink-task-created"
    tasks.create.assert_awaited_once_with(
        model="doubao-seedance-2-0-260128",
        content=[
            {"type": "text", "text": "雨后的竹林，镜头缓慢向前移动"},
            {
                "type": "image_url",
                "image_url": {
                    "url": "https://storage.example/reference/first-frame.png?signature=frame"
                },
                "role": "first_frame",
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "https://storage.example/reference/character.png?signature=image"
                },
                "role": "reference_image",
            },
            {
                "type": "audio_url",
                "audio_url": {
                    "url": "https://storage.example/reference/voice.mp3?signature=audio"
                },
                "role": "reference_audio",
            },
            {
                "type": "video_url",
                "video_url": {
                    "url": "https://storage.example/reference/motion.mp4?signature=video"
                },
                "role": "reference_video",
            },
        ],
        generate_audio=True,
        ratio="16:9",
        duration=8,
        watermark=False,
    )
    on_task_created.assert_awaited_once_with("neolink-task-created")


@pytest.mark.asyncio
async def test_neolink_reference_media_use_external_https_urls(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(
            side_effect=[
                "https://storage.example/reference/character.png?signature=image",
                "https://storage.example/reference/voice.mp3?signature=audio",
                "https://storage.example/reference/first-frame.png?signature=frame",
            ]
        )
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    provider = NeolinkVideoGenProvider()

    image_urls = await provider._encode_storage_refs(
        [{"storage_key": "upload_images/material/character.png"}],
        "image/png",
    )
    audio_urls = await provider._encode_storage_refs(
        [{"storage_key": "upload_images/material/voice.mp3"}],
        "audio/mpeg",
    )
    first_frame_url = await provider._encode_single(
        "generate_images/material/first-frame.png"
    )

    assert image_urls == [
        "https://storage.example/reference/character.png?signature=image"
    ]
    assert audio_urls == ["https://storage.example/reference/voice.mp3?signature=audio"]
    assert first_frame_url == (
        "https://storage.example/reference/first-frame.png?signature=frame"
    )
    assert storage.external_read_url.await_args_list == [
        call(
            "upload_images/material/character.png",
            expires_seconds=900,
        ),
        call(
            "upload_images/material/voice.mp3",
            expires_seconds=900,
        ),
        call(
            "generate_images/material/first-frame.png",
            expires_seconds=900,
        ),
    ]


@pytest.mark.asyncio
async def test_neolink_generate_passes_external_urls_to_ark_create(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        get_size=AsyncMock(return_value=1024),
        external_read_url=AsyncMock(
            side_effect=[
                "https://storage.example/reference/character.png?signature=image",
                "https://storage.example/reference/voice.mp3?signature=audio",
                "https://storage.example/reference/first-frame.png?signature=frame",
            ]
        ),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    tasks = SimpleNamespace(
        create=AsyncMock(return_value=SimpleNamespace(id="neolink-task-generate")),
        get=AsyncMock(
            return_value={
                "id": "neolink-task-generate",
                "status": "succeeded",
                "content": {"video_url": _DOCUMENTED_VIDEO_URL},
            }
        ),
    )
    client = SimpleNamespace(content_generation=SimpleNamespace(tasks=tasks))
    provider = NeolinkVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_close_client_safely", AsyncMock())
    monkeypatch.setattr(provider, "_download_to_file", AsyncMock())
    monkeypatch.setattr(
        provider,
        "_store_video_file",
        AsyncMock(return_value="generate_images/video/job-neolink-generate.mp4"),
    )

    result = await provider.generate(
        VideoGenerationRequest(
            prompt="雨后的竹林",
            reference_images=[
                {
                    "storage_key": "upload_images/material/character.png",
                    "mime_type": "image/png",
                }
            ],
            reference_image_names=["角色参考图"],
            reference_audios=[
                {
                    "storage_key": "upload_images/material/voice.mp3",
                    "mime_type": "audio/mpeg",
                }
            ],
            reference_audio_names=["角色声音"],
            model="doubao-seedance-2-0-260128",
            job_id="job-neolink-generate",
            first_frame="generate_images/material/first-frame.png",
            duration_seconds=8,
            on_task_created=AsyncMock(),
        )
    )

    assert result.video_storage_key == "generate_images/video/job-neolink-generate.mp4"
    create_kwargs = tasks.create.await_args.kwargs
    assert create_kwargs["content"][1:] == [
        {
            "type": "image_url",
            "image_url": {
                "url": "https://storage.example/reference/first-frame.png?signature=frame"
            },
            "role": "first_frame",
        },
        {
            "type": "image_url",
            "image_url": {
                "url": "https://storage.example/reference/character.png?signature=image"
            },
            "role": "reference_image",
        },
        {
            "type": "audio_url",
            "audio_url": {
                "url": "https://storage.example/reference/voice.mp3?signature=audio"
            },
            "role": "reference_audio",
        },
    ]
    assert all(
        item[item["type"]]["url"].startswith("https://")
        for item in create_kwargs["content"][1:]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "external_url",
    [
        None,
        "/api/x/cctv/zhihui/flow-app/media/image.png",
        "http://storage.example/reference/image.png",
        "https://localhost/reference/image.png",
        "https://127.0.0.1/reference/image.png",
        "https://10.0.0.1/reference/image.png",
        "https://user:password@storage.example/reference/image.png",
        "https://storage.example:8443/reference/image.png",
        "https://storage.example/reference/image.png#fragment",
    ],
)
async def test_neolink_rejects_unusable_external_reference_url(
    external_url: str | None,
) -> None:
    storage_key = "upload_images/private-material/secret-reference.png"
    storage = SimpleNamespace(external_read_url=AsyncMock(return_value=external_url))

    with pytest.raises(ProviderConfigurationError) as exc_info:
        await NeolinkVideoGenProvider._external_reference_url(storage, storage_key)

    assert storage_key not in str(exc_info.value)
    assert "signature" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_neolink_missing_external_url_fails_before_sdk_submit(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage_key = "upload_images/private-material/secret-reference.png"
    storage = SimpleNamespace(
        get_size=AsyncMock(return_value=1024),
        external_read_url=AsyncMock(return_value=None),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    provider = NeolinkVideoGenProvider()
    submit = AsyncMock(side_effect=AssertionError("无外链时不得调用 NeoLink SDK"))
    monkeypatch.setattr(provider, "_submit_and_poll", submit)

    with pytest.raises(ProviderConfigurationError) as exc_info:
        await provider.generate(
            VideoGenerationRequest(
                prompt="雨后的竹林",
                reference_images=[
                    {
                        "storage_key": storage_key,
                        "mime_type": "image/png",
                    }
                ],
                reference_image_names=["角色参考图"],
                reference_audios=[],
                reference_audio_names=[],
                model="doubao-seedance-2-0-260128",
                job_id="job-neolink-no-external-url",
            )
        )

    assert storage_key not in str(exc_info.value)
    submit.assert_not_awaited()


def test_neolink_download_host_only_allows_documented_ark_bucket(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    provider = NeolinkVideoGenProvider()

    provider._validate_download_host(_DOCUMENTED_VIDEO_URL)
    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        provider._validate_download_host("https://cdn.example.com/generated/video.mp4")


@pytest.mark.asyncio
async def test_neolink_expired_status_is_typed_terminal_error(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    tasks = SimpleNamespace(
        create=AsyncMock(side_effect=AssertionError("恢复任务不得重复提交")),
        get=AsyncMock(
            return_value={
                "id": "neolink-task-expired",
                "status": "expired",
            }
        ),
        delete=AsyncMock(side_effect=AssertionError("NeoLink 不支持 Ark tasks.delete")),
    )
    client = SimpleNamespace(content_generation=SimpleNamespace(tasks=tasks))
    provider = NeolinkVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_close_client_safely", AsyncMock())

    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await provider._submit_and_poll(
            "doubao-seedance-2-0-260128",
            "雨后的竹林，镜头缓慢向前移动",
            [],
            [],
            None,
            lambda *_args: None,
            provider_task_id="neolink-task-expired",
        )

    assert exc_info.value.provider_task_id == "neolink-task-expired"
    assert exc_info.value.terminal_status == "failed"
    tasks.create.assert_not_awaited()
    tasks.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_neolink_unknown_status_fails_fast_without_remote_cancel(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    tasks = SimpleNamespace(
        create=AsyncMock(side_effect=AssertionError("恢复任务不得重复提交")),
        get=AsyncMock(return_value={"id": "neolink-task-unknown", "status": "mystery"}),
        delete=AsyncMock(side_effect=AssertionError("未知状态不得触发远端取消")),
    )
    client = SimpleNamespace(content_generation=SimpleNamespace(tasks=tasks))
    provider = NeolinkVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_close_client_safely", AsyncMock())

    with pytest.raises(RuntimeError, match="neolink_video_status_unknown"):
        await provider._submit_and_poll(
            "doubao-seedance-2-0-260128",
            "雨后的竹林，镜头缓慢向前移动",
            [],
            [],
            None,
            lambda *_args: None,
            provider_task_id="neolink-task-unknown",
        )

    tasks.create.assert_not_awaited()
    tasks.delete.assert_not_awaited()
