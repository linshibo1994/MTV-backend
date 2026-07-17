"""T8 视频生成专项行为测试。

只使用 SQLite / fake provider / fake storage，不连接真实 DB、Redis、AI 或网络。
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy import CheckConstraint, UniqueConstraint, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import BadRequestError, ConflictError, ForbiddenError
from app.models.asset_entry import AssetEntry
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.models.video_provider import VideoProviderConfig


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncNested:
    def __init__(self, transaction) -> None:
        self.transaction = transaction

    async def __aenter__(self):
        return self.transaction.__enter__()

    async def __aexit__(self, exc_type, exc, tb):
        return self.transaction.__exit__(exc_type, exc, tb)


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.commit_count = 0

    async def execute(self, statement):
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.commit_count += 1
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    def add(self, row) -> None:
        self.session.add(row)

    def begin_nested(self):
        return _AsyncNested(self.session.begin_nested())


def _make_db() -> tuple[object, Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        User.__table__,
        Script.__table__,
        Episode.__table__,
        ShotGroup.__table__,
        Shot.__table__,
        Subject.__table__,
        AssetEntry.__table__,
        CreditAccount.__table__,
        CreditReservation.__table__,
        CreditLog.__table__,
        VideoProviderConfig.__table__,
        VideoGenerationJob.__table__,
        TaskDispatchOutbox.__table__,
    ):
        table.create(engine)
    raw = Session(engine, expire_on_commit=False)
    return engine, raw


def _seed(raw: Session) -> SimpleNamespace:
    user = User(
        user_id="user-1",
        username="alice",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    script = Script(
        id="script-1",
        title="剧本",
        scope="personal",
        owner_user_id=user.user_id,
        created_by_user_id=user.user_id,
    )
    episode = Episode(
        episode_id="episode-1",
        script_id=script.id,
        episode_no=1,
        title="第一集",
        episode_version=7,
    )
    current = ShotGroup(
        shot_group_id="sg-10",
        script_id=script.id,
        episode_id=episode.episode_id,
        scene_no=1,
        group_index=10,
        group_no="G10",
        prompt="镜头组提示词",
        duration_seconds=6,
        materials={
            "character": [{"subject_id": "subject-c", "name": "阿明", "asset_entry_id": "asset-img"}],
            "background": [],
            "prop": [],
            "other": [],
        },
        audio_elements={"characters": ["阿明"], "sounds": ["门响"]},
        unit_runtime_key="episode-1:1:G10",
    )
    previous_g2 = ShotGroup(
        shot_group_id="sg-2",
        script_id=script.id,
        episode_id=episode.episode_id,
        scene_no=1,
        group_index=2,
        group_no="G2",
        prompt="上一组",
        reference_frames={"last_frame": "/frames/g2-last.png"},
        materials={},
    )
    later_g11 = ShotGroup(
        shot_group_id="sg-11",
        script_id=script.id,
        episode_id=episode.episode_id,
        scene_no=1,
        group_index=11,
        group_no="G11",
        prompt="后一组",
        reference_frames={"last_frame": "/frames/g11-last.png"},
        materials={},
    )
    character = Subject(
        id="subject-c",
        script_id=script.id,
        type="character",
        name="阿明",
        description="角色",
        episode_nos=[1],
        review_status="approved",
        image_entry_id="asset-img",
    )
    sound = Subject(
        id="subject-s",
        script_id=script.id,
        type="sound",
        name="门响",
        description="声音",
        episode_nos=[1],
        review_status="approved",
    )
    image = AssetEntry(
        id="asset-img",
        script_id=script.id,
        subject_id=character.id,
        kind="image",
        storage_key="/generate_images/material-1/image.png",
        mime_type="image/png",
        size_bytes=123,
        source="generate_images",
        sha256="a" * 64,
        status="approved",
    )
    audio = AssetEntry(
        id="asset-audio",
        script_id=script.id,
        subject_id=sound.id,
        kind="audio",
        storage_key="/audio/door.wav",
        mime_type="audio/wav",
        size_bytes=456,
        source="upload_images",
        sha256="b" * 64,
        status="approved",
    )
    character_audio = AssetEntry(
        id="asset-character-audio",
        script_id=script.id,
        subject_id=character.id,
        kind="audio",
        storage_key="/audio/aming.wav",
        mime_type="audio/wav",
        size_bytes=456,
        source="upload_images",
        sha256="c" * 64,
        status="approved",
    )
    account = CreditAccount(
        id="account-1",
        owner_type="user",
        owner_id=user.user_id,
        balance_points=5000,
        frozen_points=0,
        status="active",
    )
    provider = VideoProviderConfig(
        id="provider-1",
        provider_key="modelsai",
        job_type="all_reference",
        model="doubao-seedance-2-0-260128",
        is_active=True,
    )
    raw.add_all([
        user,
        script,
        episode,
        current,
        previous_g2,
        later_g11,
        character,
        sound,
        image,
        audio,
        character_audio,
        account,
        provider,
    ])
    raw.commit()
    return SimpleNamespace(user=user, script=script, episode=episode, shot_group=current)


def _add_worker_job(
    raw: Session,
    seeded: SimpleNamespace,
    *,
    job_id: str,
    reservation_id: str,
    unit_id: str = "group:sg-10",
    attempt: int = 1,
) -> VideoGenerationJob:
    reservation = CreditReservation(
        id=reservation_id,
        account_id="account-1",
        source_type="generation_job",
        source_id=job_id,
        reserved_points=210,
        status="reserved",
    )
    account = raw.get(CreditAccount, "account-1")
    account.frozen_points += 210
    job = VideoGenerationJob(
        job_id=job_id,
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        shot_group_id=seeded.shot_group.shot_group_id,
        owner_user_id=seeded.user.user_id,
        scene_no=1,
        unit_id=unit_id,
        unit_runtime_key="episode-1:1:G10",
        provider_key="modelsai",
        job_type="all_reference",
        idempotency_key=f"{job_id}-key",
        request_hash="h",
        input_version=7,
        attempt=attempt,
        reservation_id=reservation.id,
        status="starting",
        input_json={
            "model": "doubao-seedance-2-0-260128",
            "prompt": "提示词",
            "reference_images": [],
            "reference_image_names": [],
            "reference_audios": [],
            "reference_audio_names": [],
            "reservation_id": reservation.id,
            "shot_group_version": {"episode_version": 7},
        },
    )
    raw.add_all([reservation, job])
    raw.commit()
    return job


def test_video_job_model_declares_reliable_state_and_constraints() -> None:
    columns = set(VideoGenerationJob.__table__.columns.keys())
    assert {
        "owner_user_id",
        "shot_group_id",
        "idempotency_key",
        "request_hash",
        "input_version",
        "attempt",
        "reservation_id",
        "dispatch_id",
        "lease_owner",
        "lease_token",
        "lease_until",
        "heartbeat_at",
        "started_at",
        "finished_at",
        "canceled_at",
        "error_type",
        "retryable",
    }.issubset(columns)

    unique_columns = [
        {column.name for column in constraint.columns}
        for constraint in VideoGenerationJob.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert {"script_id", "idempotency_key"} in unique_columns
    assert {"unit_id", "attempt"} in unique_columns

    check_names = {
        constraint.name
        for constraint in VideoGenerationJob.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert {
        "ck_video_jobs_status",
        "ck_video_jobs_progress_range",
        "ck_video_jobs_attempt_positive",
    }.issubset(check_names)


def test_video_static_routes_are_registered_before_dynamic_job_route() -> None:
    from app.api.v1.video_generation import router

    paths = [getattr(route, "path", "") for route in router.routes]
    active_idx = paths.index("/video-generation/active-provider")
    check_idx = paths.index("/video-generation/check-materials")
    dynamic_idx = paths.index("/video-generation/{job_id}")
    assert active_idx < dynamic_idx
    assert check_idx < dynamic_idx


def test_reference_aggregation_is_fail_closed_and_keeps_image_audio_names_aligned() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)

        from app.services.reference_aggregator import aggregate_references

        aggregated = await aggregate_references(db, "sg-10")

        assert aggregated.reference_image_names == ["阿明"]
        assert [item["storage_key"] for item in aggregated.reference_images] == [
            "/generate_images/material-1/image.png"
        ]
        assert aggregated.reference_audio_names == ["阿明", "门响"]
        assert [item["storage_key"] for item in aggregated.reference_audios] == [
            "/audio/aming.wav",
            "/audio/door.wav",
        ]

        with pytest.raises(HTTPException) as exc_info:
            await aggregate_references(db, "missing")
        assert exc_info.value.status_code == 404

    asyncio.run(scenario())


def test_create_video_job_requires_idempotency_key_and_uses_transactional_outbox(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        enqueued: list[dict] = []

        async def fake_enqueue(db, **kwargs):
            enqueued.append(kwargs)
            row = TaskDispatchOutbox(
                id="dispatch-1",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload={**kwargs["payload"], "dispatch_id": "dispatch-1", "run_id": kwargs["job_id"]},
                status="pending",
            )
            db.add(row)
            return row

        monkeypatch.setattr("app.services.video_generation.enqueue_task", fake_enqueue)

        from app.services.video_generation import create_video_job

        with pytest.raises(BadRequestError):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key=None,
            )

        job = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="idem-1",
        )
        same = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="idem-1",
        )
        assert same.job_id == job.job_id
        assert len(raw.scalars(select(VideoGenerationJob)).all()) == 1
        assert len(enqueued) == 1
        assert enqueued[0]["task_type"] == "video.generate"
        assert raw.scalar(select(TaskDispatchOutbox)).job_id == job.job_id
        assert job.reservation_id
        assert job.dispatch_id == "dispatch-1"
        assert job.input_version == seeded.episode.episode_version
        assert job.input_json["shot_group_version"]["episode_version"] == seeded.episode.episode_version

        with pytest.raises(ConflictError):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id="sg-2",
                user_id=seeded.user.user_id,
                idempotency_key="idem-1",
            )

    asyncio.run(scenario())


def test_create_video_job_after_terminal_retry_creates_next_immutable_attempt(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        previous = VideoGenerationJob(
            job_id="job-attempt-1",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="retry-old",
            request_hash="h",
            input_version=7,
            attempt=1,
            status="failed",
            retryable=True,
            input_json={},
        )
        raw.add(previous)
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        enqueued: list[dict] = []

        async def fake_enqueue(db, **kwargs):
            enqueued.append(kwargs)
            row = TaskDispatchOutbox(
                id=f"dispatch-{kwargs['attempt']}",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload=kwargs["payload"],
                status="pending",
            )
            db.add(row)
            return row

        monkeypatch.setattr("app.services.video_generation.enqueue_task", fake_enqueue)

        from app.services.video_generation import create_video_job

        retry = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="retry-new",
        )

        assert retry.job_id != previous.job_id
        assert retry.attempt == 2
        assert previous.attempt == 1
        assert enqueued[0]["attempt"] == 2
        assert [row.attempt for row in raw.scalars(
            select(VideoGenerationJob).where(VideoGenerationJob.unit_id == "group:sg-10").order_by(VideoGenerationJob.attempt)
        ).all()] == [1, 2]

    asyncio.run(scenario())


def test_first_frame_uses_natural_g2_before_g10_order() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)

        from app.services.video_generation import _resolve_first_frame

        assert await _resolve_first_frame(db, seeded.shot_group) == "/frames/g2-last.png"

    asyncio.run(scenario())


def test_video_claim_heartbeat_finish_are_token_guarded() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = VideoGenerationJob(
            job_id="job-lease",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="lease-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id="reservation-1",
            status="starting",
            input_json={},
        )
        raw.add(job)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.crud.video import claim_video_job, finish_video_job, heartbeat_video_job

        claim = await claim_video_job(db, "job-lease", owner="worker-a", now=datetime(2026, 7, 17, tzinfo=UTC))
        assert claim is not None
        assert await heartbeat_video_job(db, "job-lease", claim.lease_token, now=datetime(2026, 7, 17, 0, 1, tzinfo=UTC))
        assert not await finish_video_job(db, "job-lease", "old-token", "succeeded")
        assert await finish_video_job(
            db,
            "job-lease",
            claim.lease_token,
            "succeeded",
            output_json={"video_storage_key": "generate_images/video/job-lease.mp4"},
        )
        assert raw.get(VideoGenerationJob, "job-lease").status == "succeeded"

    asyncio.run(scenario())


def test_typed_provider_request_and_private_url_rejection() -> None:
    from app.providers.video_gen import VideoGenerationProgress, VideoGenerationRequest
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    request = VideoGenerationRequest(
        prompt="提示词",
        reference_images=[{"kind": "storage", "storage_key": "/img.png"}],
        reference_image_names=["阿明"],
        reference_audios=[],
        reference_audio_names=[],
        model="doubao-seedance-2-0-260128",
        job_id="job-1",
    )
    progress = VideoGenerationProgress(stage="submitting", message="提交", progress=10)
    assert request.reference_image_names == ["阿明"]
    assert progress.progress == 10

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    with pytest.raises(ValueError):
        asyncio.run(provider._download("http://127.0.0.1/private.mp4"))


def test_volcengine_provider_builds_async_ark_client(monkeypatch) -> None:
    from app.core.config import settings
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    captured: dict = {}
    sentinel = object()

    def fake_async_ark(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(settings, "volcengine_api_key", "test-ark-key")
    monkeypatch.setattr("volcenginesdkarkruntime.AsyncArk", fake_async_ark)

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    assert provider._build_client() is sentinel
    assert captured == {
        "api_key": "test-ark-key",
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
    }


def test_volcengine_provider_prefers_explicit_video_gen_api_key(monkeypatch) -> None:
    """显式 VIDEO_GEN_API_KEY 优先于 VOLCENGINE_API_KEY 回退。"""
    from app.core.config import Settings
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    explicit = Settings(
        _env_file=None,
        video_gen_api_key="explicit-ark-key",
        volcengine_api_key="fallback-ark-key",
    )
    monkeypatch.setattr("app.providers.video_gen_volcengine.settings", explicit)
    monkeypatch.setattr("volcenginesdkarkruntime.AsyncArk", lambda **kw: kw)

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    client = provider._build_client()
    assert client["api_key"] == "explicit-ark-key"


def test_volcengine_provider_explicit_base_url_overrides_default(monkeypatch) -> None:
    """显式 VIDEO_GEN_BASE_URL 覆盖 Ark 默认端点。"""
    from app.core.config import Settings
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    explicit = Settings(
        _env_file=None,
        volcengine_api_key="ark-key",
        video_gen_base_url="https://ark.example.com/api/v3",
    )
    monkeypatch.setattr("app.providers.video_gen_volcengine.settings", explicit)
    monkeypatch.setattr("volcenginesdkarkruntime.AsyncArk", lambda **kw: kw)

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    client = provider._build_client()
    assert client["base_url"] == "https://ark.example.com/api/v3"
    assert client["api_key"] == "ark-key"


def test_volcengine_provider_check_keys_raises_when_missing(monkeypatch) -> None:
    """Ark API Key 均缺失时 _check_keys 抛 RuntimeError。"""
    from app.core.config import Settings
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    empty = Settings(_env_file=None)  # video_gen_api_key 与 volcengine_api_key 均为空
    monkeypatch.setattr("app.providers.video_gen_volcengine.settings", empty)

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    with pytest.raises(RuntimeError, match="缺少火山方舟 Ark API Key"):
        provider._check_keys()


def test_volcengine_provider_builds_ark_content_with_private_media() -> None:
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    content = provider._build_content(
        "镜头提示词",
        ["data:image/png;base64,aW1hZ2U="],
        ["data:audio/wav;base64,YXVkaW8="],
        "data:image/jpeg;base64,ZnJhbWU=",
    )

    assert content == [
        {"type": "text", "text": "镜头提示词"},
        {
            "type": "image_url",
            "image_url": {"url": "data:image/jpeg;base64,ZnJhbWU="},
            "role": "first_frame",
        },
        {
            "type": "image_url",
            "image_url": {"url": "data:image/png;base64,aW1hZ2U="},
            "role": "reference_image",
        },
        {
            "type": "audio_url",
            "audio_url": {"url": "data:audio/wav;base64,YXVkaW8="},
            "role": "reference_audio",
        },
    ]


def test_volcengine_provider_uses_ark_tasks_and_closes_client(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        calls: dict = {}

        class FakeTasks:
            async def create(self, **kwargs):
                calls["create"] = kwargs
                return SimpleNamespace(id="provider-task-1")

            async def get(self, *, task_id: str):
                calls.setdefault("get", []).append(task_id)
                return SimpleNamespace(
                    id=task_id,
                    status="succeeded",
                    content=SimpleNamespace(
                        video_url="https://cdn.example.com/video.mp4",
                        last_frame_url="https://cdn.example.com/frame.jpg",
                    ),
                    error=None,
                )

        class FakeClient:
            def __init__(self) -> None:
                self.content_generation = SimpleNamespace(tasks=FakeTasks())
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        client = FakeClient()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_build_client", lambda: client)
        monkeypatch.setattr(provider, "_download", AsyncMock(return_value=b"video"))

        notifications: list[tuple] = []
        video, metadata = await provider._submit_and_poll(
            "doubao-seedance-2-0-260128",
            "镜头提示词",
            ["data:image/png;base64,aW1hZ2U="],
            ["data:audio/wav;base64,YXVkaW8="],
            "data:image/jpeg;base64,ZnJhbWU=",
            lambda *args: notifications.append(args),
            duration_seconds=8,
            job_id="job-1",
        )

        assert video == b"video"
        assert calls["create"] == {
            "model": "doubao-seedance-2-0-260128",
            "content": provider._build_content(
                "镜头提示词",
                ["data:image/png;base64,aW1hZ2U="],
                ["data:audio/wav;base64,YXVkaW8="],
                "data:image/jpeg;base64,ZnJhbWU=",
            ),
            "duration": 8,
            "generate_audio": True,
            "return_last_frame": True,
        }
        assert calls["get"] == ["provider-task-1"]
        assert metadata == {
            "task_id": "provider-task-1",
            "volcengine_status": "succeeded",
            "video_url": "https://cdn.example.com/video.mp4",
            "last_frame_url": "https://cdn.example.com/frame.jpg",
            "job_id": "job-1",
        }
        assert client.closed is True
        assert notifications[0][:3] == ("submitted", "视频生成任务已提交", 15)

    asyncio.run(scenario())


def test_video_provider_factory_routes_supported_vendors() -> None:
    from app.providers.video_gen import get_video_provider
    from app.providers.video_gen_bailian import BailianVideoGenProvider
    from app.providers.video_gen_openai import OpenAIVideoGenProvider
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    assert isinstance(get_video_provider("modelsai"), VolcengineVideoGenProvider)
    assert isinstance(get_video_provider("bailian"), BailianVideoGenProvider)
    assert isinstance(get_video_provider("openai"), OpenAIVideoGenProvider)


def test_openai_video_provider_sends_first_frame_and_reads_async_content(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoGenerationRequest
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        calls: dict = {}

        class Content:
            closed = False

            async def aread(self):
                return b"video-bytes"

            async def aclose(self):
                self.closed = True

        content = Content()

        class Videos:
            async def create(self, **kwargs):
                calls["create"] = kwargs
                return SimpleNamespace(id="sora-task-1")

            async def retrieve(self, video_id):
                calls.setdefault("retrieve", []).append(video_id)
                return SimpleNamespace(status="completed")

            async def download_content(self, video_id):
                calls["download"] = video_id
                return content

        class Client:
            def __init__(self):
                self.videos = Videos()
                self.closed = False

            async def close(self):
                self.closed = True

        class Storage:
            def __init__(self):
                self.puts: list[tuple[str, bytes]] = []

            async def get(self, key):
                assert key == "frames/first.png"
                return b"frame-bytes"

            async def put(self, key, data):
                self.puts.append((key, data))

        client = Client()
        storage = Storage()
        provider = OpenAIVideoGenProvider()
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda: client)
        monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
        request = VideoGenerationRequest(
            prompt="镜头提示词",
            reference_images=[],
            reference_image_names=[],
            reference_audios=[],
            reference_audio_names=[],
            model="sora-2",
            job_id="job-sora",
            first_frame="frames/first.png",
            duration_seconds=7,
        )

        result = await provider.generate(request)

        assert calls["create"]["input_reference"] == {
            "image_url": "data:image/png;base64,ZnJhbWUtYnl0ZXM="
        }
        assert calls["create"]["seconds"] == "8"
        assert calls["retrieve"] == ["sora-task-1"]
        assert calls["download"] == "sora-task-1"
        assert storage.puts == [
            ("generate_images/video/job-sora.mp4", b"video-bytes")
        ]
        assert result.provider_job_id == "sora-task-1"
        assert content.closed is True
        assert client.closed is True

    asyncio.run(scenario())


def test_video_providers_resume_persisted_provider_tasks(monkeypatch) -> None:
    async def scenario() -> None:
        from app.core.config import settings
        from app.providers.video_gen import VideoGenerationRequest
        from app.providers.video_gen_bailian import BailianVideoGenProvider
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        class HttpClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            async def post(self, *args, **kwargs):
                raise AssertionError("恢复任务不得重复提交")

        request = VideoGenerationRequest(
            prompt="提示词",
            reference_images=[],
            reference_image_names=[],
            reference_audios=[],
            reference_audio_names=[],
            model="wanx2.1-t2v-turbo",
            job_id="job-resume",
            metadata={"provider_task_id": "provider-existing"},
        )
        monkeypatch.setattr(settings, "dashscope_api_key", "test-key")
        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: HttpClient())
        bailian = BailianVideoGenProvider()
        monkeypatch.setattr(
            bailian,
            "_poll",
            AsyncMock(return_value="https://result.aliyuncs.com/video.mp4"),
        )
        monkeypatch.setattr(bailian, "_download", AsyncMock(return_value=b"video"))
        monkeypatch.setattr(
            bailian,
            "_store_video",
            AsyncMock(return_value="generate_images/video/job-resume.mp4"),
        )

        bailian_result = await bailian.generate(request)
        assert bailian_result.provider_job_id == "provider-existing"

        class Videos:
            async def create(self, **kwargs):
                raise AssertionError("恢复任务不得重复提交")

        class OpenAIClient:
            videos = Videos()

            async def close(self):
                return None

        openai = OpenAIVideoGenProvider()
        monkeypatch.setattr(openai, "_check_keys", lambda: None)
        monkeypatch.setattr(openai, "_build_client", lambda: OpenAIClient())
        monkeypatch.setattr(
            openai,
            "_poll_and_download",
            AsyncMock(return_value=b"video"),
        )
        monkeypatch.setattr(
            openai,
            "_store_video",
            AsyncMock(return_value="generate_images/video/job-resume.mp4"),
        )

        openai_result = await openai.generate(request)
        assert openai_result.provider_job_id == "provider-existing"

    asyncio.run(scenario())


def test_video_provider_seed_includes_all_routable_vendors() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        raw.query(VideoProviderConfig).delete()
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.crud.video import ensure_provider_configs

        await ensure_provider_configs(db)
        await db.commit()

        rows = raw.scalars(select(VideoProviderConfig)).all()
        assert {row.provider_key for row in rows} == {
            "jianyu",
            "modelsai",
            "bailian",
            "openai",
        }
        assert sum(row.is_active for row in rows) == 1

    asyncio.run(scenario())


def test_worker_settle_failure_marks_financial_cleanup_and_deletes_video(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        _add_worker_job(
            raw,
            seeded,
            job_id="job-worker",
            reservation_id="reservation-1",
        )
        db = _AsyncSessionAdapter(raw)
        deleted: list[str] = []

        class Provider:
            async def generate(self, request, on_progress=None):
                return SimpleNamespace(
                    video_storage_key="generate_images/video/job-worker.mp4",
                    provider_job_id="provider-job",
                    metrics={},
                )

        class Storage:
            async def delete(self, key):
                deleted.append(key)

        async def failing_settle(*args, **kwargs):
            raise RuntimeError("settle failed: https://example.invalid/body")

        monkeypatch.setattr("app.workers.video_generation._credits_settle", failing_settle)
        monkeypatch.setattr("app.workers.video_generation._credits_release", AsyncMock())

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            "job-worker",
            owner="worker",
            provider=Provider(),
            extract_last_frame=lambda *_: (_ for _ in ()).throw(RuntimeError("ffmpeg failed")),
            storage=Storage(),
        )

        refreshed = raw.get(VideoGenerationJob, "job-worker")
        assert refreshed.status == "failed"
        assert refreshed.error_type == "financial_cleanup"
        assert refreshed.retryable is True
        assert "https://" not in (refreshed.error_message or "")
        assert refreshed.output_json is None
        assert deleted == ["generate_images/video/job-worker.mp4"]

    asyncio.run(scenario())


def test_worker_provider_timeout_releases_real_reservation_and_sanitizes_error() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-provider-timeout",
            reservation_id="reservation-provider-timeout",
        )
        db = _AsyncSessionAdapter(raw)

        class Provider:
            async def generate(self, request, on_progress=None):
                raise TimeoutError("private provider endpoint timed out")

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
        )

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, "reservation-provider-timeout")
        account = raw.get(CreditAccount, "account-1")
        assert refreshed.status == "failed"
        assert refreshed.error_type == "provider_timeout"
        assert refreshed.error_message == "provider_timeout"
        assert reservation.status == "released"
        assert account.frozen_points == 0
        assert account.balance_points == 5000

    asyncio.run(scenario())


def test_top_level_video_failure_marks_financial_cleanup_when_release_fails(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-top-level-failure",
            reservation_id="reservation-top-level-failure",
        )
        db = _AsyncSessionAdapter(raw)

        async def failing_release(*args, **kwargs):
            raise RuntimeError("private database endpoint")

        monkeypatch.setattr(
            "app.workers.video_generation._credits_release", failing_release
        )

        from app.workers.video_generation import _mark_failed_and_release_once

        await _mark_failed_and_release_once(db, job.job_id, "worker_internal_error")

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(
            CreditReservation, "reservation-top-level-failure"
        )
        assert refreshed.status == "failed"
        assert refreshed.error_type == "financial_cleanup"
        assert refreshed.error_message == "financial_cleanup"
        assert refreshed.retryable is True
        assert refreshed.lease_token is None
        assert reservation.status == "reserved"

    asyncio.run(scenario())


def test_video_task_wrapper_never_returns_raw_internal_error(monkeypatch) -> None:
    from app.workers import video_generation as worker

    async def failing_run(job_id: str):
        raise RuntimeError("postgresql://user:secret@private-db/internal")

    marked: list[tuple[str, str]] = []

    async def mark_failed(job_id: str, error_type: str) -> None:
        marked.append((job_id, error_type))

    monkeypatch.setattr(worker, "_run_video_generation_async", failing_run)
    monkeypatch.setattr(worker, "_mark_failed_and_release", mark_failed)

    result = worker.run_video_generation.run("job-wrapper-failure")

    assert result == {
        "job_id": "job-wrapper-failure",
        "status": "failed",
        "error": "worker_internal_error",
    }
    assert marked == [("job-wrapper-failure", "worker_internal_error")]


def test_worker_frame_failure_rolls_back_settle_releases_and_deletes_video() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-frame-failure",
            reservation_id="reservation-frame-failure",
        )
        db = _AsyncSessionAdapter(raw)
        deleted: list[str] = []

        class Provider:
            async def generate(self, request, on_progress=None):
                return SimpleNamespace(
                    video_storage_key="generate_images/video/job-frame-failure.mp4",
                    provider_job_id="provider-job",
                    metrics={},
                )

        class Storage:
            async def delete(self, key):
                deleted.append(key)

        async def fail_frame(*_args):
            raise RuntimeError("ffmpeg private failure")

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=fail_frame,
            storage=Storage(),
        )

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, "reservation-frame-failure")
        account = raw.get(CreditAccount, "account-1")
        assert refreshed.status == "failed"
        assert refreshed.error_type == "frame_extraction_failed"
        assert refreshed.error_message == "frame_extraction_failed"
        assert reservation.status == "released"
        assert account.frozen_points == 0
        assert account.balance_points == 5000
        assert deleted == ["generate_images/video/job-frame-failure.mp4"]

    asyncio.run(scenario())


def test_check_materials_rejects_cross_script_before_aggregation(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api

        _, raw = _make_db()
        seeded = _seed(raw)
        other = Script(
            id="script-2",
            title="其他",
            scope="personal",
            owner_user_id=seeded.user.user_id,
            created_by_user_id=seeded.user.user_id,
        )
        raw.add(other)
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=other))

        with pytest.raises(ForbiddenError):
            await api.check_materials(
                shot_group_id=seeded.shot_group.shot_group_id,
                script_id=other.id,
                db=db,
                user=seeded.user,
            )

    asyncio.run(scenario())


async def test_video_generation_route_requires_login(client) -> None:
    response = await client.get("/v3api/video-generation/active-provider")
    assert response.status_code == 401
    assert response.json() == {"detail": "未登录"}


def test_cancel_terminal_wins_over_worker_success_and_releases_credit(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        reservation = CreditReservation(
            id="reservation-cancel",
            account_id="account-1",
            source_type="generation_job",
            source_id="job-cancel",
            reserved_points=210,
            status="reserved",
        )
        job = VideoGenerationJob(
            job_id="job-cancel",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="cancel-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id=reservation.id,
            status="starting",
            input_json={"reservation_id": reservation.id},
        )
        raw.add_all([reservation, job])
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.crud.video import claim_video_job, finish_video_job
        from app.services.video_generation import cancel_video_job

        claim = await claim_video_job(db, "job-cancel", owner="worker")
        await db.commit()
        release = AsyncMock()
        monkeypatch.setattr("app.services.video_generation._credits_reserve", object())
        monkeypatch.setattr("app.services.credits.release", release)

        canceled = await cancel_video_job(db, script_id=seeded.script.id, job_id="job-cancel")
        assert canceled.status == "canceled"
        assert release.await_count == 1
        assert not await finish_video_job(
            db,
            "job-cancel",
            claim.lease_token,
            "succeeded",
            output_json={"video_storage_key": "generate_images/video/job-cancel.mp4"},
        )
        assert raw.get(VideoGenerationJob, "job-cancel").status == "canceled"

    asyncio.run(scenario())


def test_video_financial_cleanup_reconciler_releases_and_clears_retryable(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        reservation = CreditReservation(
            id="reservation-cleanup",
            account_id="account-1",
            source_type="generation_job",
            source_id="job-cleanup",
            reserved_points=210,
            status="reserved",
        )
        job = VideoGenerationJob(
            job_id="job-cleanup",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="cleanup-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id=reservation.id,
            status="failed",
            error_type="financial_cleanup",
            retryable=True,
            input_json={"reservation_id": reservation.id},
        )
        raw.add_all([reservation, job])
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        release = AsyncMock()
        monkeypatch.setattr("app.workers.video_generation._credits_release", release)

        from app.workers.video_generation import reconcile_video_financial_cleanup

        assert await reconcile_video_financial_cleanup(db) == 1
        refreshed = raw.get(VideoGenerationJob, "job-cleanup")
        assert refreshed.retryable is False
        release.assert_awaited_once()

    asyncio.run(scenario())


def test_fail_with_stale_lease_does_not_release_credit_or_commit(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        reservation = CreditReservation(
            id="reservation-stale",
            account_id="account-1",
            source_type="generation_job",
            source_id="job-stale",
            reserved_points=210,
            status="reserved",
        )
        job = VideoGenerationJob(
            job_id="job-stale",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="stale-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id=reservation.id,
            status="processing",
            lease_token="fresh-token",
            input_json={"reservation_id": reservation.id},
        )
        raw.add_all([reservation, job])
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        release = AsyncMock()
        monkeypatch.setattr("app.workers.video_generation._credits_release", release)

        from app.workers.video_generation import _fail_video_with_release

        await _fail_video_with_release(
            db,
            "job-stale",
            "stale-token",
            reservation.id,
            error_type="provider_or_storage_error",
            retryable=False,
        )

        release.assert_not_awaited()
        assert db.commit_count == 0
        assert raw.get(VideoGenerationJob, "job-stale").status == "processing"

    asyncio.run(scenario())


def test_cancel_release_failure_does_not_commit_canceled(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        reservation = CreditReservation(
            id="reservation-cancel-fail",
            account_id="account-1",
            source_type="generation_job",
            source_id="job-cancel-fail",
            reserved_points=210,
            status="reserved",
        )
        job = VideoGenerationJob(
            job_id="job-cancel-fail",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="cancel-fail-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id=reservation.id,
            status="processing",
            lease_token="cancel-token",
            input_json={"reservation_id": reservation.id},
        )
        raw.add_all([reservation, job])
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def failing_release(*args, **kwargs):
            raise RuntimeError("release failed")

        monkeypatch.setattr("app.services.video_generation._credits_reserve", object())
        monkeypatch.setattr("app.services.credits.release", failing_release)

        from app.services.video_generation import cancel_video_job

        with pytest.raises(RuntimeError, match="release failed"):
            await cancel_video_job(db, script_id=seeded.script.id, job_id="job-cancel-fail")

        assert db.commit_count == 0
        assert raw.get(VideoGenerationJob, "job-cancel-fail").status == "processing"

    asyncio.run(scenario())


def test_worker_resolves_first_frame_at_execution_and_persists_progress_and_provider_task(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        raw.get(ShotGroup, "sg-2").reference_frames = {"last_frame": "/frames/original.png"}
        reservation = CreditReservation(
            id="reservation-progress",
            account_id="account-1",
            source_type="generation_job",
            source_id="job-progress",
            reserved_points=210,
            status="reserved",
        )
        job = VideoGenerationJob(
            job_id="job-progress",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="progress-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id=reservation.id,
            status="starting",
            input_json={
                "model": "doubao-seedance-2-0-260128",
                "prompt": "提示词",
                "reference_images": [],
                "reference_image_names": [],
                "reference_audios": [],
                "reference_audio_names": [],
                "shot_group_version": {"episode_version": 7},
            },
        )
        raw.add_all([reservation, job])
        raw.commit()
        raw.get(ShotGroup, "sg-2").reference_frames = {"last_frame": "/frames/latest.png"}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        seen_requests = []

        class Provider:
            async def generate(self, request, on_progress=None):
                seen_requests.append(request)
                on_progress(SimpleNamespace(
                    stage="submitted",
                    message="provider task submitted",
                    progress=25,
                    provider_task_id="provider-task-1",
                ))
                await asyncio.sleep(0)
                assert raw.get(VideoGenerationJob, "job-progress").output_json == {
                    "provider_task_id": "provider-task-1"
                }
                return SimpleNamespace(
                    video_storage_key="generate_images/video/job-progress.mp4",
                    provider_job_id="provider-task-1",
                    metrics={"elapsed_ms": 10},
                )

        monkeypatch.setattr("app.workers.video_generation._credits_settle", AsyncMock())

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            "job-progress",
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

        refreshed = raw.get(VideoGenerationJob, "job-progress")
        assert seen_requests[0].first_frame == "/frames/latest.png"
        assert refreshed.output_json["provider_task_id"] == "provider-task-1"
        assert refreshed.metrics["provider_task_id"] == "provider-task-1"
        assert refreshed.progress == 100

    asyncio.run(scenario())


def test_worker_serializes_multiple_progress_updates_on_one_session() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-progress-serialized",
            reservation_id="reservation-progress-serialized",
        )

        class GuardedSession(_AsyncSessionAdapter):
            def __init__(self, session: Session) -> None:
                super().__init__(session)
                self.executing = False

            async def execute(self, statement):
                if self.executing:
                    raise AssertionError("AsyncSession 被并发使用")
                self.executing = True
                try:
                    await asyncio.sleep(0)
                    return self.session.execute(statement)
                finally:
                    self.executing = False

        class Provider:
            async def generate(self, request, on_progress=None):
                on_progress(
                    SimpleNamespace(
                        stage="submitted",
                        message="已提交",
                        progress=15,
                        provider_task_id="provider-serialized",
                    )
                )
                on_progress(
                    SimpleNamespace(
                        stage="polling",
                        message="生成中",
                        progress=60,
                        provider_task_id="provider-serialized",
                    )
                )
                await asyncio.sleep(0.01)
                return SimpleNamespace(
                    video_storage_key="generate_images/video/job-progress-serialized.mp4",
                    provider_job_id="provider-serialized",
                    metrics={},
                )

        db = GuardedSession(raw)

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(
            CreditReservation, "reservation-progress-serialized"
        )
        assert refreshed.status == "succeeded"
        assert refreshed.output_json["provider_task_id"] == "provider-serialized"
        assert reservation.status == "captured"

    asyncio.run(scenario())


def test_worker_reuses_persisted_provider_task_id_on_recovery(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        reservation = CreditReservation(
            id="reservation-recovery",
            account_id="account-1",
            source_type="generation_job",
            source_id="job-recovery",
            reserved_points=210,
            status="reserved",
        )
        job = VideoGenerationJob(
            job_id="job-recovery",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="recovery-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            reservation_id=reservation.id,
            status="processing",
            lease_token="expired-token",
            lease_until=datetime(2026, 7, 16, tzinfo=UTC),
            output_json={"provider_task_id": "provider-existing"},
            input_json={
                "model": "doubao-seedance-2-0-260128",
                "prompt": "提示词",
                "reference_images": [],
                "reference_image_names": [],
                "reference_audios": [],
                "reference_audio_names": [],
                "shot_group_version": {"episode_version": 7},
            },
        )
        raw.add_all([reservation, job])
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        seen_metadata = []

        class Provider:
            async def generate(self, request, on_progress=None):
                seen_metadata.append(request.metadata)
                return SimpleNamespace(
                    video_storage_key="generate_images/video/job-recovery.mp4",
                    provider_job_id="provider-existing",
                    metrics={},
                )

        monkeypatch.setattr("app.workers.video_generation._credits_settle", AsyncMock())

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            "job-recovery",
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

        assert seen_metadata == [{"provider_task_id": "provider-existing"}]

    asyncio.run(scenario())


def test_stale_video_reconciler_restores_dispatches_once_and_skips_active_jobs() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        stale_starting = _add_worker_job(
            raw,
            seeded,
            job_id="job-stale-starting",
            reservation_id="reservation-stale-starting",
            unit_id="group:stale-starting",
        )
        stale_processing = _add_worker_job(
            raw,
            seeded,
            job_id="job-stale-processing",
            reservation_id="reservation-stale-processing",
            unit_id="group:stale-processing",
        )
        active_processing = _add_worker_job(
            raw,
            seeded,
            job_id="job-active-processing",
            reservation_id="reservation-active-processing",
            unit_id="group:active-processing",
        )
        recent_starting = _add_worker_job(
            raw,
            seeded,
            job_id="job-recent-starting",
            reservation_id="reservation-recent-starting",
            unit_id="group:recent-starting",
        )
        terminal = _add_worker_job(
            raw,
            seeded,
            job_id="job-terminal",
            reservation_id="reservation-terminal",
            unit_id="group:terminal",
        )
        now = datetime.now(UTC)
        stale_starting.updated_at = now - timedelta(hours=2)
        stale_processing.status = "processing"
        stale_processing.lease_owner = "dead-worker"
        stale_processing.lease_token = "expired-token"
        stale_processing.lease_until = now - timedelta(minutes=1)
        stale_processing.updated_at = now - timedelta(hours=2)
        active_processing.status = "processing"
        active_processing.lease_owner = "active-worker"
        active_processing.lease_token = "active-token"
        active_processing.lease_until = now + timedelta(hours=1)
        recent_starting.updated_at = now
        terminal.status = "succeeded"
        terminal.finished_at = now
        outbox = TaskDispatchOutbox(
            id="dispatch-stale-processing",
            task_type="video.generate",
            job_id=stale_processing.job_id,
            attempt=stale_processing.attempt,
            payload={"job_id": stale_processing.job_id},
            status="published",
            published_at=now - timedelta(hours=1),
        )
        stale_processing.dispatch_id = outbox.id
        raw.add(outbox)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.workers.video_generation import reconcile_stale_video_jobs

        recovered = await reconcile_stale_video_jobs(db)

        assert set(recovered) == {stale_starting.job_id, stale_processing.job_id}
        starting_outbox = raw.scalar(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.job_id == stale_starting.job_id
            )
        )
        assert starting_outbox is not None
        assert starting_outbox.status == "pending"
        assert stale_starting.dispatch_id == starting_outbox.id
        assert stale_processing.status == "starting"
        assert stale_processing.lease_owner is None
        assert stale_processing.lease_token is None
        assert stale_processing.lease_until is None
        assert outbox.status == "pending"
        assert outbox.published_at is None
        assert active_processing.status == "processing"
        assert active_processing.lease_token == "active-token"
        assert recent_starting.status == "starting"
        assert terminal.status == "succeeded"
        assert await reconcile_stale_video_jobs(db) == []

    asyncio.run(scenario())


def test_reference_aggregation_fail_closed_on_missing_sha_and_disambiguates_typed_materials() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        prop = Subject(
            id="subject-prop",
            script_id=seeded.script.id,
            type="prop",
            name="阿明",
            description="同名道具",
            episode_nos=[1],
            review_status="approved",
            image_entry_id="asset-prop",
        )
        prop_image = AssetEntry(
            id="asset-prop",
            script_id=seeded.script.id,
            subject_id=prop.id,
            kind="image",
            storage_key="/generate_images/material-2/image.png",
            mime_type="image/png",
            size_bytes=234,
            source="generate_images",
            sha256="d" * 64,
            status="approved",
        )
        raw.add_all([prop, prop_image])
        raw.get(AssetEntry, "asset-img").sha256 = None
        seeded.shot_group.materials = {
            "character": [{"subject_id": "subject-c", "name": "阿明", "asset_entry_id": "asset-img"}],
            "prop": [{"subject_id": "subject-prop", "name": "阿明", "asset_entry_id": "asset-prop"}],
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.services.reference_aggregator import aggregate_references

        aggregated = await aggregate_references(db, seeded.shot_group.shot_group_id)

        assert aggregated.missing_subjects == ["character:阿明"]
        assert aggregated.reference_image_names == ["阿明"]
        assert aggregated.reference_image_categories == ["prop"]
        assert aggregated.subject_id_map == {"prop:阿明": "subject-prop"}

    asyncio.run(scenario())


def test_reference_audio_uses_stable_subject_id_after_sound_rename() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        sound = raw.get(Subject, "subject-s")
        sound.name = "门响-新版"
        seeded.shot_group.audio_elements = {
            "sounds": [{
                "subject_id": "subject-s",
                "name": "门响",
                "asset_entry_id": "asset-audio",
                "type": "sound",
            }]
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.services.reference_aggregator import aggregate_references

        aggregated = await aggregate_references(db, seeded.shot_group.shot_group_id)

        assert aggregated.reference_audio_names == ["门响"]
        assert [item["storage_key"] for item in aggregated.reference_audios] == ["/audio/door.wav"]
        assert aggregated.missing_audio == []

    asyncio.run(scenario())


def test_batch_create_requires_idempotency_key_before_creating_any_job(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoJobBatchCreateIn

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        create = AsyncMock(side_effect=AssertionError("不应创建任务"))
        monkeypatch.setattr("app.services.video_generation.create_video_job", create)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))

        with pytest.raises(BadRequestError):
            await api.batch_create_video_generation_jobs(
                VideoJobBatchCreateIn(
                    script_id=seeded.script.id,
                    episode_id=seeded.episode.episode_id,
                    shot_group_ids=[seeded.shot_group.shot_group_id],
                ),
                db=db,
                user=seeded.user,
                idempotency_key=None,
            )

        create.assert_not_awaited()

    asyncio.run(scenario())


def test_active_provider_initial_seed_is_committed() -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api

        engine, raw = _make_db()
        seeded = _seed(raw)
        raw.query(VideoProviderConfig).delete()
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        out = await api.get_active_provider(db=db, user=seeded.user)

        assert out.provider_key == "modelsai"
        assert db.commit_count == 1
        assert raw.scalar(select(VideoProviderConfig).where(VideoProviderConfig.is_active.is_(True))) is not None
        engine.dispose()

    asyncio.run(scenario())


def test_video_provider_and_job_type_constraints_are_declared() -> None:
    job_checks = {
        constraint.name
        for constraint in VideoGenerationJob.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    provider_checks = {
        constraint.name
        for constraint in VideoProviderConfig.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "ck_video_jobs_provider_key" in job_checks
    assert "ck_video_jobs_job_type" in job_checks
    assert "ck_video_provider_configs_provider_key" in provider_checks
    assert "ck_video_provider_configs_job_type" in provider_checks


def test_financial_cleanup_has_schedulable_celery_task() -> None:
    from app.workers import video_generation as worker

    assert worker.reconcile_video_financial_cleanup_task.name == (
        "app.workers.video_generation.reconcile_video_financial_cleanup"
    )
    assert callable(worker.reconcile_video_financial_cleanup_task.delay)


def test_first_frame_reference_read_failure_is_fail_closed(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        class Storage:
            async def get(self, key):
                raise FileNotFoundError(key)

        monkeypatch.setattr("app.providers.storage.get_storage", lambda: Storage())

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        with pytest.raises(RuntimeError, match="reference_read_failed"):
            await provider._encode_single("/frames/missing.png")

    asyncio.run(scenario())


async def test_video_generation_http_denies_cross_user_create_list_get_cancel(client, monkeypatch) -> None:
    from app.core.auth import get_current_user
    from app.core.database import get_db

    engine, raw = _make_db()
    seeded = _seed(raw)
    other = User(
        user_id="user-2",
        username="bob",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    job = VideoGenerationJob(
        job_id="job-http",
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        shot_group_id=seeded.shot_group.shot_group_id,
        owner_user_id=seeded.user.user_id,
        scene_no=1,
        unit_id="group:sg-10",
        unit_runtime_key="episode-1:1:G10",
        provider_key="modelsai",
        job_type="all_reference",
        idempotency_key="http-key",
        request_hash="h",
        input_version=7,
        attempt=1,
        status="processing",
        input_json={},
    )
    raw.add_all([other, job])
    raw.commit()
    db = _AsyncSessionAdapter(raw)

    async def override_db():
        yield db

    async def override_user():
        return other

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_current_user] = override_user
    monkeypatch.setattr("app.services.video_generation.create_video_job", AsyncMock())

    try:
        create_response = await client.post(
            "/v3api/video-generation-jobs",
            headers={"Idempotency-Key": "http-create"},
            json={
                "script_id": seeded.script.id,
                "episode_id": seeded.episode.episode_id,
                "shot_group_id": seeded.shot_group.shot_group_id,
            },
        )
        list_response = await client.get(
            f"/v3api/video-generation-jobs?script_id={seeded.script.id}"
        )
        get_response = await client.get("/v3api/video-generation/job-http")
        cancel_response = await client.post("/v3api/video-generation/job-http/cancel")
    finally:
        engine.dispose()

    assert create_response.status_code == 403
    assert list_response.status_code == 403
    assert get_response.status_code == 403
    assert cancel_response.status_code == 403
