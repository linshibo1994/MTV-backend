"""T8 视频生成专项行为测试。

只使用 SQLite / fake provider / fake storage，不连接真实 DB、Redis、AI 或网络。
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, UniqueConstraint, create_engine, inspect, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import BadRequestError, ConflictError, ForbiddenError
from app.models.asset_entry import AssetEntry
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.media_storage_job import MediaStorageJob
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.models.video_generation_job import (
    VideoGenerationIdempotencyRequest,
    VideoGenerationJob,
)
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
        self.rollback_count = 0
        self.executed_statements: list[object] = []

    async def execute(self, statement):
        self.executed_statements.append(statement)
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.commit_count += 1
        self.session.commit()

    async def rollback(self) -> None:
        self.rollback_count += 1
        self.session.rollback()

    async def refresh(self, instance, attribute_names=None) -> None:
        self.session.refresh(instance, attribute_names=attribute_names)

    def add(self, row) -> None:
        self.session.add(row)

    def begin_nested(self):
        return _AsyncNested(self.session.begin_nested())


class _DetachOnRollbackSessionAdapter(_AsyncSessionAdapter):
    """模拟 AsyncSession 回滚后旧 ORM 实例不可再隐式加载。"""

    async def rollback(self) -> None:
        await super().rollback()
        self.session.expunge_all()


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
        VideoGenerationIdempotencyRequest.__table__,
        TaskDispatchOutbox.__table__,
        MediaStorageJob.__table__,
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
    # 该夹具的既有视频创建用例关注生成编排，不覆盖人工审核流程；显式构造一份
    # 与最终普通参考图绑定的已通过审核快照。
    from app.services.reference_aggregator import AggregatedReferences
    from app.services.reference_image_audit import reference_image_hash

    audit_references = AggregatedReferences(
        reference_images=[
            {
                "kind": "storage",
                "storage_key": image.storage_key.lstrip("/"),
                "mime_type": image.mime_type,
                "size_bytes": image.size_bytes,
                "sha256": image.sha256,
            }
        ]
    )
    current.reference_image_audit = {
        "status": "approved",
        "reference_hash": reference_image_hash(audit_references),
        "image_count": len(audit_references.reference_images),
        "results": [],
    }
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


def test_create_video_job_blocks_when_reference_image_audit_is_pending() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.shot_group.reference_image_audit = {}
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.services.video_generation import create_video_job

        with pytest.raises(ConflictError, match="尚未审核"):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="audit-pending",
            )
        assert raw.scalar(select(VideoGenerationJob)) is None

    asyncio.run(scenario())


def test_create_video_job_rejects_reference_images_over_normal_limit(monkeypatch) -> None:
    """禁用自动首帧后，普通参考图仍必须遵守供应商限额。"""

    async def scenario() -> None:
        from app.services import video_generation
        from app.services.reference_aggregator import AggregatedReferences

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(
            video_generation,
            "aggregate_references",
            AsyncMock(
                return_value=AggregatedReferences(
                    reference_images=[
                        {
                            "kind": "storage",
                            "storage_key": f"generate_images/reference-{index}.png",
                            "mime_type": "image/png",
                            "sha256": f"{index:064x}",
                        }
                        for index in range(10)
                    ]
                )
            ),
        )

        with pytest.raises(BadRequestError, match="参考图最多支持 9 个"):
            await video_generation.create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="first-frame-reference-limit",
            )

        assert raw.scalar(select(VideoGenerationJob)) is None

    asyncio.run(scenario())


def test_create_bailian_i2v_rejects_reserved_but_unavailable_first_frame() -> None:
    """普通后续组的容量预约不能让图生模型在无实际输入图时创建任务。"""

    async def scenario() -> None:
        from app.services.video_generation import create_video_job

        _, raw = _make_db()
        seeded = _seed(raw)
        provider = raw.scalar(
            select(VideoProviderConfig).where(VideoProviderConfig.is_active.is_(True))
        )
        provider.provider_key = "bailian"
        provider.model = "wanx2.1-kf2v"
        seeded.shot_group.materials = {
            "character": [],
            "background": [],
            "prop": [],
            "other": [],
            "video": [],
        }
        seeded.shot_group.audio_elements = {"characters": [], "sounds": []}
        raw.get(ShotGroup, "sg-2").reference_frames = {}
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        with pytest.raises(BadRequestError, match="至少需要 1 张参考图片或首帧"):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="bailian-reserved-frame-unavailable",
            )

        assert raw.scalar(select(VideoGenerationJob)) is None

    asyncio.run(scenario())


def test_create_video_job_does_not_use_mgrouter_first_frame_quota() -> None:
    """MGRouter 任务不再为同场景前驱尾帧预留独占首帧槽。"""

    async def scenario() -> None:
        from app.services.video_generation import create_video_job

        _, raw = _make_db()
        seeded = _seed(raw)
        provider = raw.scalar(
            select(VideoProviderConfig).where(VideoProviderConfig.is_active.is_(True))
        )
        provider.provider_key = "mgrouter"
        provider.model = "AICC-Doubao-Seedance-2.0"
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        job = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="mgrouter-without-first-frame",
        )

        assert job.status == "starting"
        assert job.input_json["first_frame"] is None
        assert job.input_json["awaits_predecessor_frame"] is False

    asyncio.run(scenario())


def test_create_video_job_rejects_reference_video_for_unsupported_provider() -> None:
    async def scenario() -> None:
        from app.services.video_generation import create_video_job

        _, raw = _make_db()
        seeded = _seed(raw)
        provider = raw.scalar(
            select(VideoProviderConfig).where(VideoProviderConfig.is_active.is_(True))
        )
        provider.provider_key = "bailian"
        provider.model = "wan2.6-i2v"
        reference_job = VideoGenerationJob(
            job_id="reference-video-job",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:reference-video-source",
            unit_runtime_key="episode-1:1:reference-video-source",
            provider_key="modelsai",
            job_type="all_reference",
            input_version=seeded.episode.episode_version,
            attempt=1,
            status="succeeded",
            output_json={"video_storage_key": "generate_videos/source/video.mp4"},
        )
        seeded.shot_group.materials["video"] = [
            {"name": "上一镜头视频", "video_job_id": reference_job.job_id}
        ]
        raw.add(reference_job)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        with pytest.raises(BadRequestError, match="当前视频模型不支持参考视频"):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="unsupported-reference-video",
            )

        assert raw.scalar(
            select(VideoGenerationJob).where(
                VideoGenerationJob.job_id != reference_job.job_id
            )
        ) is None

    asyncio.run(scenario())


def test_create_video_job_skips_reference_image_audit_for_database_bailian(monkeypatch) -> None:
    """实际激活供应商为百炼时，即使存在参考图也不能套用火山审核门禁。"""
    async def scenario() -> None:
        from app.core.config import Settings
        from app.services import video_generation
        from app.services.reference_aggregator import AggregatedReferences

        _, raw = _make_db()
        seeded = _seed(raw)
        provider = raw.scalar(select(VideoProviderConfig).where(VideoProviderConfig.is_active.is_(True)))
        provider.provider_key = "bailian"
        provider.model = "wan2.6-i2v"
        # 百炼 I2V 仅消费一张首帧和一段驱动音频；此用例只验证审核能力，
        # 因此移除夹具中的额外主体/声音引用，避免掩盖动态限额门禁。
        seeded.shot_group.materials = {
            "character": [],
            "background": [],
            "prop": [],
            "other": [],
            "video": [],
        }
        seeded.shot_group.audio_elements = {"characters": [], "sounds": []}
        seeded.shot_group.reference_image_audit = {}
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-bailian",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload={**kwargs["payload"], "dispatch_id": "dispatch-bailian"},
                status="pending",
            )
            db.add(row)
            return row

        monkeypatch.setattr(video_generation, "settings", Settings(_env_file=None))
        monkeypatch.setattr(video_generation, "_credits_reserve", None)
        monkeypatch.setattr(video_generation, "enqueue_task", fake_enqueue)
        monkeypatch.setattr(
            video_generation,
            "aggregate_references",
            AsyncMock(return_value=AggregatedReferences(reference_images=[{"storage_key": "input.png"}])),
        )

        job = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="bailian-skips-audit",
        )

        assert job.provider_key == "bailian"
        assert job.input_json["reference_image_audit"]["status"] == "not_required"

    asyncio.run(scenario())


def test_create_text_to_video_job_does_not_freeze_or_pass_previous_tail_frame(monkeypatch) -> None:
    """纯文生模型不能因为镜头组序号或续帧开关意外接收首帧。"""

    async def scenario() -> None:
        from app.core.config import Settings
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        provider = raw.scalar(select(VideoProviderConfig).where(VideoProviderConfig.is_active.is_(True)))
        provider.provider_key = "bailian"
        provider.model = "wan2.1-t2v"
        seeded.shot_group.is_continuation = True
        seeded.shot_group.materials = {
            "character": [],
            "background": [],
            "prop": [],
            "other": [],
            "video": [],
        }
        seeded.shot_group.audio_elements = {"characters": [], "sounds": []}
        seeded.shot_group.reference_image_audit = {}
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-bailian-t2v",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload={**kwargs["payload"], "dispatch_id": "dispatch-bailian-t2v"},
                status="pending",
            )
            db.add(row)
            return row

        monkeypatch.setattr(video_generation, "settings", Settings(_env_file=None))
        monkeypatch.setattr(video_generation, "_credits_reserve", None)
        monkeypatch.setattr(video_generation, "enqueue_task", fake_enqueue)

        job = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="bailian-t2v-no-tail-frame",
        )

        assert job.status == "starting"
        assert job.input_json["first_frame_supported"] is False
        assert job.input_json["first_frame"] is None
        assert job.input_json["continuation_dependency"] is None

    asyncio.run(scenario())


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

    assert set(VideoGenerationIdempotencyRequest.__table__.primary_key.columns.keys()) == {
        "script_id",
        "idempotency_key",
    }
    assert {
        "script_id",
        "idempotency_key",
        "request_hash",
        "job_id",
    } <= set(VideoGenerationIdempotencyRequest.__table__.columns.keys())


def test_video_job_create_schema_accepts_optional_trimmed_prompt_with_length_limit() -> None:
    from app.schemas.video import VideoJobCreateIn

    without_prompt = VideoJobCreateIn(
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="sg-1",
    )
    assert without_prompt.prompt is None

    with_prompt = VideoJobCreateIn(
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="sg-1",
        prompt="  客户端视频提示词\n",
    )
    assert with_prompt.prompt == "客户端视频提示词"
    assert VideoJobCreateIn(
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="sg-1",
        prompt="镜" * 32000,
    ).prompt == "镜" * 32000

    for invalid_prompt in ("", " \n\t ", "镜" * 32001):
        with pytest.raises(ValidationError):
            VideoJobCreateIn(
                script_id="script-1",
                episode_id="episode-1",
                shot_group_id="sg-1",
                prompt=invalid_prompt,
            )


def test_video_json_merge_uses_locked_debt_state_without_resurrecting_stale_debt() -> None:
    from app.crud.video import _merge_latest_video_json

    stale_debt = {
        "attempt_output_cleanup": {
            "stale-attempt": {"retryable": True},
        },
        "provider_cancellation": {
            "provider_task_id": "stale-provider-task",
            "retryable": True,
        },
        "provider_task_id": "provider-new",
    }
    assert _merge_latest_video_json(
        {"provider_task_id": "provider-current"},
        stale_debt,
    ) == {"provider_task_id": "provider-new"}


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
        assert aggregated.reference_image_refs == [
            {
                "token": "图1",
                "ref": None,
                "name": "阿明",
                "category": "character",
                "subject_id": "subject-c",
                "asset_entry_id": "asset-img",
            }
        ]
        # 持久化对象引用统一使用 canonical 相对 key。
        assert [item["storage_key"] for item in aggregated.reference_images] == [
            "generate_images/material-1/image.png"
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
        with pytest.raises(BadRequestError):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="   ",
            )

        job = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="idem-1",
        )
        assert "updated_at" not in inspect(job).expired_attributes

        from app.schemas.video import VideoJobOut

        response = VideoJobOut.model_validate(job)
        assert response.updated_at is not None

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

        with pytest.raises(ConflictError) as exc_info:
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id="sg-2",
                user_id=seeded.user.user_id,
                idempotency_key="idem-1",
            )
        assert exc_info.value.detail == "幂等键已用于不同请求载荷"

    asyncio.run(scenario())


def test_create_video_job_persists_explicit_prompt_without_video_template(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-explicit-prompt",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload=kwargs["payload"],
                status="pending",
            )
            db.add(row)
            return row

        monkeypatch.setattr(video_generation, "enqueue_task", fake_enqueue)
        template_resolver = AsyncMock(
            side_effect=AssertionError("视频创建不应解析 video.constraint_lock")
        )
        template_renderer = Mock(
            side_effect=AssertionError("视频创建不应渲染 video.constraint_lock")
        )
        monkeypatch.setattr(
            video_generation,
            "resolve_prompt_template",
            template_resolver,
            raising=False,
        )
        monkeypatch.setattr(
            video_generation,
            "render_prompt_template_with_fallback",
            template_renderer,
            raising=False,
        )

        job = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="explicit-video-prompt",
            prompt="  客户端自定义运镜  ",
        )

        expected = (
            "## 图片内容说明\n"
            "图1: 阿明（短引用 无；类型 character）\n"
            "## 图片内容说明结束\n"
            "客户端自定义运镜"
        )
        assert job.input_json["prompt"] == expected
        assert job.input_json["base_prompt"] == "客户端自定义运镜"
        assert job.input_json["source_prompt"] == expected
        assert job.input_json["adapted_prompt"] == expected
        assert job.input_json["adapted_prompt_sha256"] == hashlib.sha256(
            expected.encode("utf-8")
        ).hexdigest()
        assert job.input_json["adapter_version"] == "video-prompt-adapter.v1"
        assert job.input_json["prompt_adapter"]["version"] == "video-prompt-adapter.v1"
        assert "constraint_prompt_template" not in job.input_json
        assert job.input_json["reference_token_map"] == {
            "图1": {
                "ref": None,
                "name": "阿明",
                "category": "character",
                "subject_id": "subject-c",
                "asset_entry_id": "asset-img",
                "sha256": "a" * 64,
            },
        }
        template_resolver.assert_not_awaited()
        template_renderer.assert_not_called()

        from app.schemas.video import VideoJobOut

        response = VideoJobOut.model_validate(job)
        assert "constraint_prompt_template" not in response.input_json

    asyncio.run(scenario())


def test_create_video_job_enhances_structured_prompt_before_adapter(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        raw.add_all(
            [
                Shot(
                    shot_id="shot-structured-1",
                    shot_group_id=seeded.shot_group.shot_group_id,
                    scene_no=1,
                    group_no="G10",
                    shot_no="2x2x2x2x2x",
                    display_no=1,
                    content="阿明推门进入",
                    description="镜头一",
                    shot_line="镜头一",
                    start_seconds=0,
                    end_seconds=3,
                    characters=["阿明"],
                    structured_data={
                        "schema_version": "storyboard.shot-structure.v1",
                        "display_no": 1,
                        "start_seconds": 0,
                        "end_seconds": 3,
                        "visual": {
                            "shot_size": "全景",
                            "composition": ["三分法"],
                            "camera_angle": "平视",
                            "camera_movement": "推镜",
                            "lighting": "室内暖光",
                            "frame_description": "阿明站在门边",
                            "action": "推门进入",
                            "emotion": "谨慎",
                        },
                        "narrative_purpose": "展现阿明推门进入时的谨慎状态",
                        "character_refs": ["C01"],
                        "prop_refs": [],
                        "sound_refs": [],
                        "sound_design": "门轴轻响",
                        "dialogue": None,
                        "source_block_refs": ["B01"],
                    },
                ),
                Shot(
                    shot_id="shot-structured-2",
                    shot_group_id=seeded.shot_group.shot_group_id,
                    scene_no=1,
                    group_no="G10",
                    shot_no="3x3x3x3x3x",
                    display_no=2,
                    content="阿明停下回头",
                    description="镜头二",
                    shot_line="镜头二",
                    start_seconds=3,
                    end_seconds=6,
                    characters=["阿明"],
                    structured_data={
                        "schema_version": "storyboard.shot-structure.v1",
                        "display_no": 2,
                        "start_seconds": 3,
                        "end_seconds": 6,
                        "visual": {
                            "shot_size": "中景",
                            "composition": ["前景遮挡"],
                            "camera_angle": "侧拍",
                            "camera_movement": "固定",
                            "lighting": "门外冷光照入室内",
                            "frame_description": "阿明停在室内回望门口",
                            "action": "停下并回头",
                            "emotion": "警觉",
                        },
                        "narrative_purpose": "展现阿明回望门口时的警觉状态",
                        "character_refs": ["C01"],
                        "prop_refs": [],
                        "sound_refs": [],
                        "sound_design": "无",
                        "dialogue": None,
                        "source_block_refs": ["B02"],
                    },
                ),
            ]
        )
        seeded.shot_group.reference_frames = {
            "boundary_projections": {
                "start": {"prompt_fragment": "起始边界：阿明位于门外，右手刚触及门把。"},
                "end": {"prompt_fragment": "结束边界：阿明已进入室内并回望门口。"},
            }
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-structured-prompt",
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
            idempotency_key="structured-video-prompt",
        )

        source = job.input_json["source_prompt"]
        assert "Camera " not in source
        assert "【镜头1:0s～3s】全景·平视，镜头缓慢向主体推进。" in source
        assert "【镜头2:3s～6s】中景·侧拍，镜头保持固定。" in source
        assert source.index("## 图片内容说明") < source.index("镜头组提示词")
        assert source.index("【镜头2:3s～6s】") < source.index("【冻结边界事实 boundary-facts】")
        assert "起始边界：阿明位于门外，右手刚触及门把。" in source
        assert "结束边界：阿明已进入室内并回望门口。" in source
        assert source.endswith("画面不出现任何可读文字、字幕、Logo 或水印。")
        assert "constraint_prompt_template" not in job.input_json
        assert job.input_json["prompt"] == source
        assert job.input_json["boundary_prompt_fragments"] == [
            "起始边界：阿明位于门外，右手刚触及门把。",
            "结束边界：阿明已进入室内并回望门口。",
        ]

    asyncio.run(scenario())


def test_create_video_job_replaces_stored_legacy_group_prompt_before_adapter(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.shot_group.prompt = (
            "【镜头1:0s～3s】老姜独自在电脑前加班，构图是【三分法】，"
            "声音设计是【远处车流声】\n"
            "【角色】老姜、电脑、茶杯\n"
            "【叙事目的】展现深夜工作状态\n"
            "【约束】人物特征高度一致，禁止出现任何形式的字幕。"
        )
        raw.add(
            Shot(
                shot_id="shot-legacy-prompt-1",
                shot_group_id=seeded.shot_group.shot_group_id,
                scene_no=1,
                group_no="G10",
                shot_no="4x4x4x4x4x",
                display_no=1,
                content="老姜独自在电脑前加班，专注处理屏幕上的工作。",
                description="深夜办公",
                shot_line="【镜头1:0s～3s】旧式镜头行",
                start_seconds=0,
                end_seconds=3,
                characters=["老姜"],
                props=["电脑", "茶杯"],
                structured_data={},
            )
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-legacy-prompt",
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
            idempotency_key="legacy-group-prompt",
        )

        source = job.input_json["source_prompt"]
        assert "构图是【" not in source
        assert "声音设计是【" not in source
        assert "【角色】" not in source
        assert "【叙事目的】" not in source
        assert "老姜独自在电脑前加班，专注处理屏幕上的工作。" in source
        assert source.index("## 图片内容说明") < source.index("【镜头1:0s～3s】")
        assert source.endswith("【约束】人物特征高度一致，禁止出现任何形式的字幕。")

    asyncio.run(scenario())


def test_create_video_job_same_idempotency_key_rejects_changed_explicit_prompt(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-same-key-prompt",
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

        first = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="same-key-prompt",
            prompt="第一版提示词",
        )
        replay = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="same-key-prompt",
            prompt="第一版提示词",
        )

        assert replay.job_id == first.job_id
        with pytest.raises(ConflictError) as exc_info:
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="same-key-prompt",
                prompt="第二版提示词",
            )
        assert exc_info.value.detail == "幂等键已用于不同请求载荷"

    asyncio.run(scenario())


def test_create_video_job_reuses_active_unit_across_different_idempotency_keys(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        enqueued: list[dict] = []

        async def fake_enqueue(db, **kwargs):
            enqueued.append(kwargs)
            row = TaskDispatchOutbox(
                id=f"dispatch-{len(enqueued)}",
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

        first = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="active-key-1",
            prompt="相同的客户端视频提示词",
        )
        replay = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="active-key-2",
            prompt="相同的客户端视频提示词",
        )

        assert replay.job_id == first.job_id
        assert len(raw.scalars(select(VideoGenerationJob)).all()) == 1
        assert len(raw.scalars(select(CreditReservation)).all()) == 1
        assert len(raw.scalars(select(TaskDispatchOutbox)).all()) == 1
        bindings = list(
            raw.scalars(
                select(VideoGenerationIdempotencyRequest).order_by(
                    VideoGenerationIdempotencyRequest.idempotency_key
                )
            )
        )
        assert [binding.idempotency_key for binding in bindings] == [
            "active-key-1",
            "active-key-2",
        ]
        assert {binding.job_id for binding in bindings} == {first.job_id}
        assert len(enqueued) == 1

        first.status = "succeeded"
        raw.commit()
        terminal_replay = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="active-key-2",
            prompt="相同的客户端视频提示词",
        )

        assert terminal_replay.job_id == first.job_id
        assert len(raw.scalars(select(VideoGenerationJob)).all()) == 1
        assert len(enqueued) == 1

    asyncio.run(scenario())


def test_create_video_job_rejects_different_prompt_for_active_unit_without_side_effects(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        enqueued: list[dict] = []

        async def fake_enqueue(db, **kwargs):
            enqueued.append(kwargs)
            row = TaskDispatchOutbox(
                id="dispatch-active-prompt",
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

        first = await create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="active-prompt-key-1",
            prompt="活动任务第一版提示词",
        )
        counts_before = (
            len(raw.scalars(select(VideoGenerationJob)).all()),
            len(raw.scalars(select(CreditReservation)).all()),
            len(raw.scalars(select(TaskDispatchOutbox)).all()),
            len(raw.scalars(select(VideoGenerationIdempotencyRequest)).all()),
        )

        with pytest.raises(ConflictError) as exc_info:
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="active-prompt-key-2",
                prompt="活动任务第二版提示词",
            )

        assert exc_info.value.detail == (
            "该镜头组已有使用不同提示词的生成任务，请等待完成或取消后重试"
        )
        assert (
            len(raw.scalars(select(VideoGenerationJob)).all()),
            len(raw.scalars(select(CreditReservation)).all()),
            len(raw.scalars(select(TaskDispatchOutbox)).all()),
            len(raw.scalars(select(VideoGenerationIdempotencyRequest)).all()),
        ) == counts_before
        assert raw.scalar(select(VideoGenerationJob)).job_id == first.job_id
        bindings = raw.scalars(select(VideoGenerationIdempotencyRequest)).all()
        assert [binding.idempotency_key for binding in bindings] == ["active-prompt-key-1"]
        assert len(enqueued) == 1

    asyncio.run(scenario())


def test_manual_retry_reuses_unresolved_submission_without_new_attempt(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-manual-retry-blocked",
            reservation_id="reservation-manual-retry-blocked",
        )
        job.status = "failed"
        job.progress_stage = "manual_review"
        job.error_type = "provider_submission_uncertain"
        job.retryable = False
        job.finished_at = datetime.now(UTC)
        submission = {
            "state": "submission_uncertain",
            "provider_key": "modelsai",
            "attempt": 1,
        }
        job.output_json = {"provider_submission": submission}
        job.metrics = {"provider_submission": submission}
        raw.commit()
        enqueue = AsyncMock(side_effect=AssertionError("未决提交不得创建新 outbox"))
        monkeypatch.setattr("app.services.video_generation.enqueue_task", enqueue)

        from app.services.video_generation import create_video_job

        replay = await create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="manual-retry-new-key",
        )

        assert replay.job_id == job.job_id
        assert [
            row.attempt
            for row in raw.scalars(
                select(VideoGenerationJob).where(
                    VideoGenerationJob.unit_id == job.unit_id
                )
            ).all()
        ] == [1]
        enqueue.assert_not_awaited()

    asyncio.run(scenario())


def test_video_job_scopes_shot_group_lock_before_ownership_check() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        other_script = Script(
            id="script-other",
            title="其他剧本",
            scope="personal",
            owner_user_id=seeded.user.user_id,
            created_by_user_id=seeded.user.user_id,
        )
        other_episode = Episode(
            episode_id="episode-other",
            script_id=other_script.id,
            episode_no=1,
            title="其他分集",
        )
        foreign_group = ShotGroup(
            shot_group_id="sg-foreign",
            script_id=other_script.id,
            episode_id=other_episode.episode_id,
            scene_no=1,
            group_index=1,
            group_no="G1",
            prompt="其他镜头组",
        )
        raw.add_all([other_script, other_episode, foreign_group])
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.services.video_generation import create_video_job

        with pytest.raises(ForbiddenError):
            await create_video_job(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=foreign_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="foreign-lock",
            )

        lock_statements = []
        for statement in db.executed_statements:
            sql = " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
            if "FROM shot_groups" in sql and "FOR UPDATE" in sql:
                lock_statements.append(sql)
        assert len(lock_statements) == 1
        lock_predicate = lock_statements[0].split(" WHERE ", 1)[1].split(" FOR UPDATE", 1)[0]
        assert "shot_groups.shot_group_id =" in lock_predicate
        assert "shot_groups.script_id =" in lock_predicate
        assert "shot_groups.episode_id =" in lock_predicate

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


def test_stale_claim_refuses_unresolved_provider_submission() -> None:
    from app.crud.video import claim_video_job

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-unresolved-claim",
        reservation_id="reservation-unresolved-claim",
    )
    job.status = "processing"
    job.lease_owner = "dead-worker"
    job.lease_token = "expired-token"
    job.lease_until = datetime.now(UTC) - timedelta(seconds=1)
    submission = {
        "state": "submit_intent",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    job.output_json = {"provider_submission": submission}
    job.metrics = {"provider_submission": submission}
    raw.commit()

    async def run() -> None:
        claim = await claim_video_job(
            _AsyncSessionAdapter(raw),
            job.job_id,
            owner="replacement-worker",
        )
        assert claim is None

    asyncio.run(run())
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token == "expired-token"


def test_stale_claim_allows_resume_when_submission_has_authoritative_task_id() -> None:
    from app.crud.video import claim_video_job

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-submission-with-task-id",
        reservation_id="reservation-submission-with-task-id",
    )
    job.status = "processing"
    job.lease_owner = "dead-worker"
    job.lease_token = "expired-token"
    job.lease_until = datetime.now(UTC) - timedelta(seconds=1)
    submission = {
        "state": "submit_intent",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    job.output_json = {
        "provider_submission": submission,
        "provider_task_id": "provider-resumable",
    }
    job.metrics = {
        "provider_submission": submission,
        "provider_task_id": "provider-resumable",
    }
    raw.commit()

    async def run() -> None:
        claim = await claim_video_job(
            _AsyncSessionAdapter(raw),
            job.job_id,
            owner="replacement-worker",
        )
        assert claim is not None
        assert claim.lease_token != "expired-token"

    asyncio.run(run())


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


def test_failed_video_terminal_overrides_stale_success_progress() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = VideoGenerationJob(
            job_id="job-failed-terminal-progress",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="failed-terminal-progress-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            status="processing",
            progress=100,
            progress_stage="succeeded",
            progress_message="视频生成完成",
            lease_owner="worker",
            lease_token="lease-token",
            input_json={},
        )
        raw.add(job)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.crud.video import finish_video_job

        assert await finish_video_job(
            db,
            job.job_id,
            "lease-token",
            "failed",
            error_type="frame_extraction_failed",
            error_message="frame_extraction_failed",
            retryable=False,
        )

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved.status == "failed"
        assert saved.progress == 100
        assert saved.progress_stage == "failed"
        assert saved.progress_message == "视频生成失败"
        assert saved.error_type == "frame_extraction_failed"
        assert saved.error_message == "frame_extraction_failed"
        assert saved.retryable is False

    asyncio.run(scenario())


def test_canceled_video_terminal_overrides_stale_progress_message() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = VideoGenerationJob(
            job_id="job-canceled-terminal-progress",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id="group:sg-10",
            unit_runtime_key="episode-1:1:G10",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key="canceled-terminal-progress-key",
            request_hash="h",
            input_version=7,
            attempt=1,
            status="processing",
            progress=73,
            progress_stage="generating",
            progress_message="视频生成中",
            lease_owner="worker",
            lease_token="lease-token",
            input_json={},
        )
        raw.add(job)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.crud.video import finish_video_job

        assert await finish_video_job(
            db,
            job.job_id,
            "lease-token",
            "canceled",
        )

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved.status == "canceled"
        assert saved.progress == 73
        assert saved.progress_stage == "canceled"
        assert saved.progress_message == "用户取消"
        assert saved.finished_at is not None
        assert saved.canceled_at is not None
        assert saved.lease_owner is None
        assert saved.lease_token is None
        assert saved.lease_until is None

    asyncio.run(scenario())


def test_typed_provider_request_and_private_url_rejection(tmp_path: Path) -> None:
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
        asyncio.run(
            provider._download_to_file(
                "http://127.0.0.1/private.mp4",
                tmp_path / "private.mp4",
            )
        )


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
        "max_retries": 0,
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


@pytest.mark.parametrize(
    "audio_urls",
    [[], ["data:audio/wav;base64,YXVkaW8="]],
    ids=["generated-audio", "reference-audio"],
)
def test_volcengine_provider_uses_ark_tasks_and_closes_client(
    monkeypatch,
    audio_urls: list[str],
) -> None:
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
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

        notifications: list[tuple] = []
        video_url, metadata = await provider._submit_and_poll(
            "doubao-seedance-2-0-260128",
            "镜头提示词",
            ["data:image/png;base64,aW1hZ2U="],
            audio_urls,
            "data:image/jpeg;base64,ZnJhbWU=",
            lambda *args: notifications.append(args),
            duration_seconds=8,
            job_id="job-1",
        )

        assert video_url == "https://cdn.example.com/video.mp4"
        assert calls["create"] == {
            "model": "doubao-seedance-2-0-260128",
            "content": provider._build_content(
                "镜头提示词",
                ["data:image/png;base64,aW1hZ2U="],
                audio_urls,
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


def test_openai_video_provider_sends_first_frame_and_uses_safe_download(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoGenerationRequest
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        calls: dict = {}

        class Videos:
            def __init__(self) -> None:
                self.with_streaming_response = self

            async def create(self, **kwargs):
                calls["create"] = kwargs
                return SimpleNamespace(id="sora-task-1")

            async def retrieve(self, video_id):
                calls.setdefault("retrieve", []).append(video_id)
                return SimpleNamespace(status="completed")

            def download_content(self, video_id):
                calls["sdk_download"] = video_id
                raise AssertionError("禁止 SDK download_content 绕过安全下载器")

        class Client:
            def __init__(self):
                self.videos = Videos()
                self.closed = False

            async def close(self):
                self.closed = True

        class Storage:
            def __init__(self):
                self.objects: dict[str, bytes] = {}

            async def get(self, key):
                assert key == "frames/first.png"
                return b"frame-bytes"

            async def put_file(self, key, path):
                self.objects[key] = Path(path).read_bytes()
                return key

            async def promote(self, source_key, final_key):
                self.objects[final_key] = self.objects.pop(source_key)
                return final_key

            async def delete(self, key):
                self.objects.pop(key, None)

            async def exists(self, key):
                return key in self.objects

        client = Client()
        storage = Storage()
        provider = OpenAIVideoGenProvider()
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda: client)
        monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

        async def download(video_id: str, path: str | Path, **kwargs) -> int:
            calls["safe_download"] = video_id
            assert callable(kwargs["cancellation_probe"])
            await kwargs["cancellation_probe"]()
            Path(path).write_bytes(b"video-bytes")
            return len(b"video-bytes")

        monkeypatch.setattr(provider, "_download_to_file", download)
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
        assert calls["safe_download"] == "sora-task-1"
        assert "sdk_download" not in calls
        assert storage.objects == {
            "generate_images/video/job-sora.mp4": b"video-bytes"
        }
        assert result.provider_job_id == "sora-task-1"
        assert client.closed is True

    asyncio.run(scenario())


def test_openai_video_provider_rejects_multiple_reference_images_before_submit(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoGenerationRequest, VideoGenerationSubmitRejected
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        provider = OpenAIVideoGenProvider()
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(
            provider,
            "_build_client",
            lambda: (_ for _ in ()).throw(AssertionError("超限请求不得创建客户端")),
        )
        request = VideoGenerationRequest(
            prompt="镜头提示词",
            reference_images=[
                {"storage_key": "frames/one.png", "mime_type": "image/png"},
                {"storage_key": "frames/two.png", "mime_type": "image/png"},
            ],
            reference_image_names=[],
            reference_audios=[],
            reference_audio_names=[],
            model="sora-2",
            job_id="job-sora-over-limit",
        )

        with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
            await provider.generate(request)

        assert exc_info.value.diagnostics == {"code": "reference_images_limit_exceeded"}

    asyncio.run(scenario())


def test_video_providers_resume_persisted_provider_tasks(monkeypatch) -> None:
    async def scenario() -> None:
        from app.core.config import settings
        from app.providers.video_gen import VideoGenerationRequest
        from app.providers.video_gen_bailian import BailianVideoGenProvider
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        class HttpClient:
            async def aclose(self) -> None:
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
        monkeypatch.setattr(
            bailian,
            "_download_to_file",
            AsyncMock(return_value=len(b"video")),
        )
        monkeypatch.setattr(
            bailian,
            "_store_video_file",
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
            "_poll_and_download_to_file",
            AsyncMock(return_value=len(b"video")),
        )
        monkeypatch.setattr(
            openai,
            "_store_video_file",
            AsyncMock(return_value="generate_images/video/job-resume.mp4"),
        )

        openai_result = await openai.generate(request)
        assert openai_result.provider_job_id == "provider-existing"

    asyncio.run(scenario())


def test_video_provider_seed_includes_all_routable_vendors(monkeypatch) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        raw.query(VideoProviderConfig).delete()
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        monkeypatch.setattr(
            "app.core.config.settings",
            SimpleNamespace(
                seedance_model="seedance-test",
                dashscope_video_model="wan-test",
                openai_video_model="sora-test",
                google_video_model="veo-google-test",
                mgrouter_video_model="AICC-Seedance-Test",
                video_default_provider="modelsai",
            ),
        )

        from app.crud.video import ensure_provider_configs

        await ensure_provider_configs(db)
        await ensure_provider_configs(db)
        await db.commit()

        rows = raw.scalars(select(VideoProviderConfig)).all()
        assert {row.provider_key for row in rows} == {
            "jianyu",
            "modelsai",
            "bailian",
            "openai",
                "gemini",
                "mgrouter",
                "neolink",
            }
        gemini = next(row for row in rows if row.provider_key == "gemini")
        assert gemini.model == "veo-google-test"
        mgrouter = next(row for row in rows if row.provider_key == "mgrouter")
        assert mgrouter.model == "AICC-Seedance-Test"
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
        release_log = raw.scalar(
            select(CreditLog).where(
                CreditLog.log_type == "release",
                CreditLog.reservation_id == reservation.id,
            )
        )
        assert refreshed.status == "failed"
        assert refreshed.error_type == "provider_timeout"
        assert refreshed.error_message == "provider_timeout"
        assert reservation.status == "released"
        assert account.frozen_points == 0
        assert account.balance_points == 5000
        assert release_log is not None
        assert release_log.source_id == job.job_id
        assert release_log.actor_user_id == job.owner_user_id

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "provider_error",
    [
        "download_too_large",
        "video_download_too_large",
        "video_inline_too_large",
        "reference_image_too_large",
        "reference_audio_too_large",
        "reference_media_too_large",
        "reference_media_total_too_large",
    ],
)
def test_worker_maps_provider_media_limit_errors_to_stable_contract(provider_error: str) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id=f"job-media-limit-{provider_error}",
            reservation_id=f"reservation-media-limit-{provider_error}",
        )
        db = _AsyncSessionAdapter(raw)

        class Provider:
            async def generate(self, request, on_progress=None):
                raise RuntimeError(provider_error)

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
        )

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, job.reservation_id)
        assert refreshed.status == "failed"
        assert refreshed.error_type == "provider_media_invalid"
        assert refreshed.error_message == "provider_media_invalid"
        assert refreshed.retryable is False
        assert reservation.status == "released"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "provider_error",
    [
        pytest.param(RuntimeError("ordinary provider failure"), id="provider"),
        pytest.param(OSError("storage backend unavailable"), id="storage"),
        pytest.param(ValueError("download_too_large"), id="wrong-exception-type"),
        pytest.param(RuntimeError("download_too_large: extra"), id="non-exact-message"),
    ],
)
def test_worker_does_not_misclassify_ordinary_provider_or_storage_errors(
    provider_error: Exception,
) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-ordinary-provider-storage-error",
            reservation_id="reservation-ordinary-provider-storage-error",
        )
        db = _AsyncSessionAdapter(raw)

        class Provider:
            async def generate(self, request, on_progress=None):
                raise provider_error

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
        )

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        assert refreshed.status == "failed"
        assert refreshed.error_type == "provider_or_storage_error"
        assert refreshed.error_message == "provider_or_storage_error"

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

        from app.crud.video import claim_video_job
        from app.workers.video_generation import _mark_failed_and_release_once

        claim = await claim_video_job(db, job.job_id, owner="worker")
        assert claim is not None
        await db.commit()
        await _mark_failed_and_release_once(
            db,
            job.job_id,
            claim.lease_token,
            "worker_internal_error",
        )

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

    async def mark_failed(job_id: str, lease_token: str, error_type: str) -> None:
        marked.append((job_id, error_type))

    monkeypatch.setattr(worker, "_run_video_generation_async", failing_run)
    monkeypatch.setattr(worker, "_mark_failed_and_release", mark_failed)

    result = worker.run_video_generation.run("job-wrapper-failure")

    assert result == {
        "job_id": "job-wrapper-failure",
        "status": "failed",
        "error": "worker_internal_error",
    }
    assert marked == []


@pytest.mark.asyncio
async def test_update_last_frame_logs_ffmpeg_diagnostics(monkeypatch, caplog) -> None:
    from app.providers import storage as storage_module
    from app.workers import video_generation as worker

    class Storage:
        pass

    async def download_file(_storage, _storage_key: str, _local_path: str) -> None:
        return None

    def fake_subprocess_run(command, **_kwargs):
        if command[0] == "ffprobe":
            return SimpleNamespace(stdout="15.0", returncode=0, stderr="")
        return SimpleNamespace(
            returncode=1,
            stderr=b"Invalid data found when processing input",
        )

    monkeypatch.setattr(storage_module, "get_storage", Storage)
    monkeypatch.setattr(worker, "_download_storage_file", download_file)
    monkeypatch.setattr(worker.subprocess, "run", fake_subprocess_run)

    with pytest.raises(RuntimeError, match="frame_extraction_failed"):
        await worker._update_last_frame(
            SimpleNamespace(),
            "group:sg-10",
            "generate_images/video/test.mp4",
            "attempt-1",
        )

    assert "ffmpeg_frame_extraction_failed returncode=1" in caplog.text
    assert "Invalid data found when processing input" in caplog.text


def test_worker_frame_failure_keeps_video_and_settles_reservation() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-frame-failure",
            reservation_id="reservation-frame-failure",
        )
        db = _DetachOnRollbackSessionAdapter(raw)
        job_id = job.job_id
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
            job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=fail_frame,
            storage=Storage(),
        )

        refreshed = raw.get(VideoGenerationJob, job_id)
        reservation = raw.get(CreditReservation, "reservation-frame-failure")
        account = raw.get(CreditAccount, "account-1")
        assert refreshed.status == "succeeded"
        assert refreshed.output_json["video_storage_key"] == (
            "generate_images/video/job-frame-failure.mp4"
        )
        assert refreshed.error_type is None
        assert refreshed.error_message is None
        assert reservation.status == "captured"
        assert account.frozen_points == 0
        assert deleted == []

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


@pytest.mark.parametrize(
    ("missing_subjects", "expected_ready"),
    [([], True), (["角色:阿明"], False)],
)
def test_check_materials_treats_missing_audio_as_optional_fallback(
    monkeypatch,
    missing_subjects: list[str],
    expected_ready: bool,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(
            api,
            "_load_and_check_script",
            AsyncMock(return_value=seeded.script),
        )
        from app.services.reference_aggregator import AggregatedReferences

        monkeypatch.setattr(
            "app.services.reference_aggregator.aggregate_references",
            AsyncMock(
                return_value=AggregatedReferences(
                    missing_subjects=missing_subjects,
                    missing_audio=["阿明", "门响"],
                )
            ),
        )
        monkeypatch.setattr(
            "app.services.video_generation._resolve_first_frame",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            "app.services.video_generation._reserves_first_frame",
            AsyncMock(return_value=False),
        )

        result = await api.check_materials(
            shot_group_id=seeded.shot_group.shot_group_id,
            script_id=seeded.script.id,
            db=db,
            user=seeded.user,
        )

        assert result == {
            "missing_subjects": missing_subjects,
            "missing_audio": ["阿明", "门响"],
            "missing_videos": [],
            "effective_reference_counts": {
                "images": 0,
                "audios": 0,
                "videos": 0,
                "total": 0,
                "reserves_first_frame": False,
            },
            "reference_violations": [],
            "ready": expected_ready,
        }

    asyncio.run(scenario())


def test_check_materials_reports_all_reference_violations_as_objects(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoMaterialCheckOut
        from app.services.reference_aggregator import AggregatedReferences
        from app.services.runtime_models import RuntimeVideoModel

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(
            api,
            "_load_and_check_script",
            AsyncMock(return_value=seeded.script),
        )
        monkeypatch.setattr(
            "app.services.reference_aggregator.aggregate_references",
            AsyncMock(
                return_value=AggregatedReferences(
                    reference_images=[{"storage_key": f"image-{index}"} for index in range(10)],
                    reference_audios=[{"storage_key": f"audio-{index}"} for index in range(4)],
                    reference_videos=[{"storage_key": f"video-{index}"} for index in range(4)],
                )
            ),
        )
        monkeypatch.setattr(
            "app.services.video_generation._resolve_first_frame",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            "app.services.video_generation._reserves_first_frame",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            api,
            "resolve_video_model",
            AsyncMock(
                return_value=RuntimeVideoModel(
                    provider="bailian",
                    model="wan2.6-i2v",
                    reference_image_audit_required=False,
                    task_provider_key="bailian",
                )
            ),
        )

        result = await api.check_materials(
            shot_group_id=seeded.shot_group.shot_group_id,
            script_id=seeded.script.id,
            db=db,
            user=seeded.user,
        )

        assert result["effective_reference_counts"] == {
            "images": 10,
            "audios": 4,
            "videos": 4,
            "total": 18,
            "reserves_first_frame": False,
        }
        assert [item["code"] for item in result["reference_violations"]] == [
            "reference_images_limit_exceeded",
            "reference_audios_limit_exceeded",
            "reference_videos_limit_exceeded",
            "reference_media_total_limit_exceeded",
            "reference_video_unsupported",
        ]
        assert all(set(item) == {"code", "detail"} for item in result["reference_violations"])
        assert VideoMaterialCheckOut.model_validate(result).model_dump() == {
            **result,
            "can_wait_for_predecessor": False,
        }
        assert result["ready"] is False

    asyncio.run(scenario())


def test_check_materials_requires_explicit_bailian_i2v_reference_image(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.services.reference_aggregator import AggregatedReferences
        from app.services.runtime_models import RuntimeVideoModel

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(
            api,
            "_load_and_check_script",
            AsyncMock(return_value=seeded.script),
        )
        monkeypatch.setattr(
            "app.services.reference_aggregator.aggregate_references",
            AsyncMock(return_value=AggregatedReferences()),
        )
        monkeypatch.setattr(
            api,
            "resolve_video_model",
            AsyncMock(
                return_value=RuntimeVideoModel(
                    provider="bailian",
                    model="wanx2.1-kf2v",
                    reference_image_audit_required=False,
                    task_provider_key="bailian",
                )
            ),
        )

        result = await api.check_materials(
            shot_group_id=seeded.shot_group.shot_group_id,
            script_id=seeded.script.id,
            db=db,
            user=seeded.user,
        )

        assert result["effective_reference_counts"]["images"] == 0
        assert [item["code"] for item in result["reference_violations"]] == [
            "reference_input_image_required"
        ]
        assert result["ready"] is False

    asyncio.run(scenario())


def test_check_materials_does_not_reserve_mgrouter_first_frame(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.services.reference_aggregator import AggregatedReferences
        from app.services.runtime_models import RuntimeVideoModel

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(
            api,
            "_load_and_check_script",
            AsyncMock(return_value=seeded.script),
        )
        monkeypatch.setattr(
            "app.services.reference_aggregator.aggregate_references",
            AsyncMock(
                return_value=AggregatedReferences(
                    reference_images=[{"storage_key": "reference/character.png"}],
                    reference_audios=[{"storage_key": "reference/voice.mp3"}],
                )
            ),
        )
        monkeypatch.setattr(
            api,
            "resolve_video_model",
            AsyncMock(
                return_value=RuntimeVideoModel(
                    provider="mgrouter",
                    model="AICC-Doubao-Seedance-2.0",
                    reference_image_audit_required=False,
                    task_provider_key="mgrouter",
                )
            ),
        )

        result = await api.check_materials(
            shot_group_id=seeded.shot_group.shot_group_id,
            script_id=seeded.script.id,
            db=db,
            user=seeded.user,
        )

        assert result["effective_reference_counts"] == {
            "images": 1,
            "audios": 1,
            "videos": 0,
            "total": 2,
            "reserves_first_frame": False,
        }
        assert result["reference_violations"] == []
        assert result["ready"] is True

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
        account = raw.get(CreditAccount, "account-1")
        account.frozen_points += reservation.reserved_points
        active_outbox = TaskDispatchOutbox(
            id="dispatch-video-cancel",
            task_type="video.generate",
            job_id=job.job_id,
            attempt=job.attempt,
            payload={"job_id": job.job_id},
            status="running",
            lease_owner="dispatcher",
            lease_token="dispatch-token",
            lease_until=datetime.now(UTC) + timedelta(minutes=1),
        )
        wrong_attempt = TaskDispatchOutbox(
            id="dispatch-video-wrong-attempt",
            task_type="video.generate",
            job_id=job.job_id,
            attempt=job.attempt + 1,
            payload={"job_id": job.job_id},
            status="published",
            published_at=datetime.now(UTC),
        )
        wrong_task_type = TaskDispatchOutbox(
            id="dispatch-video-wrong-type",
            task_type="timeline.render",
            job_id=job.job_id,
            attempt=job.attempt,
            payload={"job_id": job.job_id},
            status="pending",
        )
        raw.add_all(
            [reservation, job, active_outbox, wrong_attempt, wrong_task_type]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.crud.video import claim_video_job, finish_video_job
        from app.services.video_generation import cancel_video_job

        claim = await claim_video_job(db, "job-cancel", owner="worker")
        await db.commit()
        monkeypatch.setattr("app.services.video_generation._credits_reserve", object())

        canceled = await cancel_video_job(
            db,
            script_id=seeded.script.id,
            job_id="job-cancel",
            actor_user_id=seeded.user.user_id,
        )
        raw.expire_all()
        active_outbox = raw.get(TaskDispatchOutbox, "dispatch-video-cancel")
        wrong_attempt = raw.get(TaskDispatchOutbox, "dispatch-video-wrong-attempt")
        wrong_task_type = raw.get(TaskDispatchOutbox, "dispatch-video-wrong-type")
        assert active_outbox is not None
        assert wrong_attempt is not None
        assert wrong_task_type is not None
        assert canceled.status == "canceled"
        assert active_outbox.status == "canceled"
        assert active_outbox.lease_owner is None
        assert active_outbox.lease_token is None
        assert active_outbox.lease_until is None
        assert wrong_attempt.status == "published"
        assert wrong_task_type.status == "pending"
        release_log = raw.scalar(
            select(CreditLog).where(
                CreditLog.log_type == "release",
                CreditLog.reservation_id == reservation.id,
            )
        )
        assert release_log is not None
        assert release_log.source_id == job.job_id
        assert release_log.actor_user_id == seeded.user.user_id
        assert not await finish_video_job(
            db,
            "job-cancel",
            claim.lease_token,
            "succeeded",
            output_json={"video_storage_key": "generate_images/video/job-cancel.mp4"},
        )
        assert raw.get(VideoGenerationJob, "job-cancel").status == "canceled"

    asyncio.run(scenario())


def test_cancel_route_forwards_authenticated_actor(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-route-cancel",
            reservation_id="reservation-route-cancel",
        )
        db = _AsyncSessionAdapter(raw)
        allow = AsyncMock(return_value=seeded.script)
        cancel = AsyncMock(return_value=job)
        monkeypatch.setattr(api, "_load_and_check_script", allow)
        monkeypatch.setattr(service, "cancel_video_job", cancel)

        result = await api.cancel_video_generation_job(
            job.job_id,
            db=db,
            user=seeded.user,
        )

        assert result.job_id == job.job_id
        cancel.assert_awaited_once_with(
            db,
            script_id=seeded.script.id,
            job_id=job.job_id,
            actor_user_id=seeded.user.user_id,
        )

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

        async def release_reservation(
            release_db,
            *,
            reservation_id: str,
            **_kwargs,
        ) -> None:
            authoritative = release_db.session.get(
                CreditReservation,
                reservation_id,
            )
            assert authoritative is not None
            authoritative.status = "released"

        release = AsyncMock(side_effect=release_reservation)
        monkeypatch.setattr("app.workers.video_generation._credits_release", release)

        from app.workers.video_generation import reconcile_video_financial_cleanup

        assert await reconcile_video_financial_cleanup(db) == 1
        refreshed = raw.get(VideoGenerationJob, "job-cleanup")
        assert refreshed.retryable is False
        release.assert_awaited_once()
        assert release.await_args.kwargs["actor_user_id"] == job.owner_user_id

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
            actor_user_id=seeded.user.user_id,
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
            await cancel_video_job(
                db,
                script_id=seeded.script.id,
                job_id="job-cancel-fail",
                actor_user_id=seeded.user.user_id,
            )

        assert db.commit_count == 0
        assert raw.get(VideoGenerationJob, "job-cancel-fail").status == "processing"

    asyncio.run(scenario())


def test_worker_ignores_legacy_first_frame_at_execution_and_persists_progress(monkeypatch) -> None:
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
        assert seen_requests[0].first_frame is None
        assert refreshed.output_json["provider_task_id"] == "provider-task-1"
        assert refreshed.metrics["provider_task_id"] == "provider-task-1"
        assert refreshed.progress == 100

    asyncio.run(scenario())


def test_worker_honors_frozen_no_first_frame_capability(monkeypatch) -> None:
    """执行时不得为纯文生任务重新解析同场景上一组尾帧。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        raw.get(ShotGroup, "sg-2").reference_frames = {"last_frame": "/frames/latest.png"}
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-no-first-frame",
            reservation_id="reservation-no-first-frame",
        )
        job.input_json = {**job.input_json, "first_frame_supported": False}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        seen_requests = []

        class Provider:
            async def generate(self, request, on_progress=None):
                seen_requests.append(request)
                return SimpleNamespace(
                    video_storage_key="generate_images/video/job-no-first-frame.mp4",
                    provider_job_id="provider-no-first-frame",
                    metrics={},
                )

        monkeypatch.setattr("app.workers.video_generation._credits_settle", AsyncMock())

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

        assert seen_requests[0].first_frame is None

    asyncio.run(scenario())


def test_worker_ignores_legacy_mgrouter_first_frame_references() -> None:
    """历史任务也不能再将首帧与普通参考图一同提交。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-legacy-mgrouter-first-frame",
            reservation_id="reservation-legacy-mgrouter-first-frame",
        )
        job.provider_key = "mgrouter"
        job.input_json = {
            **job.input_json,
            "model": "AICC-Doubao-Seedance-2.0",
            "reference_images": [
                {
                    "storage_key": "generate_images/reference/character.png",
                    "mime_type": "image/png",
                }
            ],
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        provider_called = False

        class Provider:
            async def generate(self, request, on_progress=None):
                nonlocal provider_called
                provider_called = True
                assert request.first_frame is None
                return SimpleNamespace(
                    video_storage_key="generate_images/video/legacy-mgrouter.mp4",
                    provider_job_id="legacy-mgrouter-job",
                    metrics={},
                )

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
        )

        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, job.reservation_id)
        assert provider_called is True
        assert refreshed.status == "succeeded"
        assert refreshed.error_type is None
        assert reservation.status == "captured"

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
        settle_log = raw.scalar(
            select(CreditLog).where(
                CreditLog.log_type == "settle",
                CreditLog.reservation_id == reservation.id,
            )
        )
        assert refreshed.status == "succeeded"
        assert refreshed.output_json["provider_task_id"] == "provider-serialized"
        assert reservation.status == "captured"
        assert settle_log is not None
        assert settle_log.source_id == job.job_id
        assert settle_log.actor_user_id == job.owner_user_id

    asyncio.run(scenario())


def test_video_commit_after_apply_preserves_authoritative_succeeded_outputs(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-commit-after-apply",
            reservation_id="reservation-commit-after-apply",
        )
        disconnect_raised = False
        rollback_disconnect_raised = False

        class CommitAfterApplyAdapter(_AsyncSessionAdapter):
            async def commit(self) -> None:
                nonlocal disconnect_raised
                self.session.commit()
                saved = self.session.get(VideoGenerationJob, job.job_id)
                if saved is not None and saved.status == "succeeded" and not disconnect_raised:
                    disconnect_raised = True
                    raise ConnectionError("commit response lost")

            async def rollback(self) -> None:
                nonlocal rollback_disconnect_raised
                self.session.rollback()
                if not rollback_disconnect_raised:
                    rollback_disconnect_raised = True
                    raise ConnectionError("rollback response lost")

        class FakeSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                self.raw = Session(raw.get_bind(), expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        db = CommitAfterApplyAdapter(raw)
        stored: dict[str, bytes] = {}
        deleted: list[str] = []
        output_keys: dict[str, str] = {}

        class Provider:
            async def generate(self, request, on_progress=None):
                attempt_id = request.metadata["storage_attempt_id"]
                video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
                stored[video_key] = b"video"
                output_keys["video"] = video_key
                return SimpleNamespace(
                    video_storage_key=video_key,
                    provider_job_id="provider-commit-after-apply",
                    metrics={},
                )

        class Storage:
            async def delete(self, key: str) -> None:
                deleted.append(key)
                stored.pop(key, None)

        async def extract_last_frame(db, _unit_id, video_key: str, _lease_token: str) -> str:
            frame_key = video_key.removesuffix(".mp4") + "-last.png"
            stored[frame_key] = b"frame"
            output_keys["frame"] = frame_key
            seeded.shot_group.reference_frames = {"last_frame": frame_key}
            await db.flush()
            return frame_key

        monkeypatch.setattr("app.workers.video_generation._credits_settle", AsyncMock())
        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            FakeSessionFactory(),
        )

        from app.workers.video_generation import run_video_generation_once

        await run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=extract_last_frame,
            storage=Storage(),
        )

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        shot_group = raw.get(ShotGroup, seeded.shot_group.shot_group_id)
        assert disconnect_raised is True
        assert rollback_disconnect_raised is True
        assert saved is not None and saved.status == "succeeded"
        assert saved.output_json["video_storage_key"] == output_keys["video"]
        assert shot_group.reference_frames["last_frame"] == output_keys["frame"]
        assert deleted == []
        assert stored == {
            output_keys["video"]: b"video",
            output_keys["frame"]: b"frame",
        }

    asyncio.run(scenario())


def test_authoritative_video_success_requires_exact_attempt_key_and_fails_closed(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-authoritative-video-read",
            reservation_id="reservation-authoritative-video-read",
        )
        attempt_key = "generate_images/video/job-authoritative-video-read-attempt.mp4"
        job.status = "succeeded"
        job.output_json = {"video_storage_key": attempt_key}
        raw.commit()

        class FakeSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                self.raw = Session(raw.get_bind(), expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            FakeSessionFactory(),
        )
        assert await worker._authoritative_video_attempt_succeeded(
            job.job_id,
            attempt_key,
        ) is True
        assert await worker._authoritative_video_attempt_succeeded(
            job.job_id,
            "generate_images/video/other-attempt.mp4",
        ) is False

        class FailingSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                raise ConnectionError("authoritative read unavailable")

            async def __aexit__(self, exc_type, exc, tb):
                return None

        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            FailingSessionFactory(),
        )
        assert await worker._authoritative_video_attempt_succeeded(
            job.job_id,
            attempt_key,
        ) is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    (
        "authoritative",
        "frame_authoritative",
        "expected_status",
        "failure_expected",
    ),
    [
        (None, None, "processing", False),
        (False, False, "failed", True),
        (False, True, "failed", True),
    ],
)
def test_video_commit_failure_only_cleans_after_authoritative_false(
    monkeypatch,
    authoritative: bool | None,
    frame_authoritative: bool | None,
    expected_status: str,
    failure_expected: bool,
) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id=f"job-authoritative-{authoritative}",
            reservation_id=f"reservation-authoritative-{authoritative}",
        )

        class CommitBeforeApplyAdapter(_AsyncSessionAdapter):
            def __init__(self, session: Session) -> None:
                super().__init__(session)
                self.commit_calls = 0

            async def commit(self) -> None:
                self.commit_calls += 1
                if self.commit_calls == 2:
                    raise ConnectionError("commit outcome unavailable")
                self.session.commit()

        db = CommitBeforeApplyAdapter(raw)
        stored: dict[str, bytes] = {}
        deleted: list[str] = []
        output_keys: dict[str, str] = {}

        class Provider:
            async def generate(self, request, on_progress=None):
                attempt_id = request.metadata["storage_attempt_id"]
                video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
                stored[video_key] = b"video"
                output_keys["video"] = video_key
                return SimpleNamespace(
                    video_storage_key=video_key,
                    provider_job_id="provider-authoritative-check",
                    metrics={},
                )

        class Storage:
            async def delete(self, key: str) -> None:
                deleted.append(key)
                stored.pop(key, None)

        async def extract_last_frame(db, _unit_id, video_key: str, _lease_token: str) -> str:
            frame_key = video_key.removesuffix(".mp4") + "-last.png"
            stored[frame_key] = b"frame"
            output_keys["frame"] = frame_key
            await db.flush()
            return frame_key

        authoritative_read = AsyncMock(return_value=authoritative)
        monkeypatch.setattr(worker, "_credits_settle", AsyncMock())
        monkeypatch.setattr(
            worker,
            "_authoritative_video_attempt_succeeded",
            authoritative_read,
            raising=False,
        )
        frame_authoritative_read = AsyncMock(return_value=frame_authoritative)
        monkeypatch.setattr(
            worker,
            "_authoritative_last_frame_referenced",
            frame_authoritative_read,
            raising=False,
        )

        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=extract_last_frame,
            storage=Storage(),
        )

        raw.expire_all()
        refreshed = raw.get(VideoGenerationJob, job.job_id)
        assert refreshed.status == expected_status
        assert refreshed.error_type == (
            "provider_or_storage_error" if failure_expected else None
        )
        authoritative_read.assert_awaited_once_with(job.job_id, output_keys["video"])
        if authoritative is False:
            frame_authoritative_read.assert_awaited_once_with(
                seeded.shot_group.shot_group_id,
                output_keys["frame"],
            )
            if frame_authoritative is True:
                assert deleted == [output_keys["video"]]
                assert stored == {output_keys["frame"]: b"frame"}
            else:
                assert deleted == [output_keys["video"], output_keys["frame"]]
                assert stored == {}
            assert raw.get(CreditReservation, job.reservation_id).status == "released"
        else:
            frame_authoritative_read.assert_not_awaited()
            assert deleted == []
            assert set(stored) == {output_keys["video"], output_keys["frame"]}
            assert raw.get(CreditReservation, job.reservation_id).status == "reserved"

    asyncio.run(scenario())


@pytest.mark.parametrize("authoritative", [False, True])
def test_persisted_attempt_cleanup_debt_is_periodically_reconciled(
    monkeypatch,
    authoritative: bool,
) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id=f"job-attempt-debt-{authoritative}",
            reservation_id=f"reservation-attempt-debt-{authoritative}",
        )
        attempt_id = "0123456789abcdef"
        video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
        frame_key = (
            "generate_images/video/frames/"
            f"{seeded.shot_group.shot_group_id}_{attempt_id}_last.png"
        )
        record = worker._attempt_cleanup_record(
            shot_group_id=seeded.shot_group.shot_group_id,
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=frame_key,
        )
        job.status = "succeeded" if authoritative else "failed"
        job.output_json = {
            **({"video_storage_key": video_key} if authoritative else {}),
            worker._ATTEMPT_CLEANUP_DEBT_KEY: {attempt_id: record},
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        deleted: list[str] = []

        class Storage:
            async def delete(self, key: str) -> None:
                deleted.append(key)

        monkeypatch.setattr("app.providers.storage.get_storage", lambda: Storage())
        authoritative_read = AsyncMock(return_value=authoritative)
        monkeypatch.setattr(
            worker,
            "_authoritative_video_attempt_succeeded",
            authoritative_read,
        )
        frame_authoritative_read = AsyncMock(return_value=False)
        monkeypatch.setattr(
            worker,
            "_authoritative_last_frame_referenced",
            frame_authoritative_read,
        )

        assert await worker.reconcile_video_attempt_cleanup_debts(db) == 1

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert worker._ATTEMPT_CLEANUP_DEBT_KEY not in (saved.output_json or {})
        if authoritative:
            assert saved.output_json["video_storage_key"] == video_key
            assert deleted == []
        else:
            assert deleted == [video_key, frame_key]
            frame_authoritative_read.assert_awaited_once_with(
                seeded.shot_group.shot_group_id,
                frame_key,
            )
        authoritative_read.assert_awaited_once_with(job.job_id, video_key)

    asyncio.run(scenario())


def test_persisted_attempt_cleanup_debt_survives_unrecoverable_delete(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-attempt-debt-delete-failed",
            reservation_id="reservation-attempt-debt-delete-failed",
        )
        attempt_id = "fedcba9876543210"
        video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
        frame_key = (
            "generate_images/video/frames/"
            f"{seeded.shot_group.shot_group_id}_{attempt_id}_last.png"
        )
        record = worker._attempt_cleanup_record(
            shot_group_id=seeded.shot_group.shot_group_id,
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=frame_key,
        )
        job.status = "failed"
        job.output_json = {
            worker._ATTEMPT_CLEANUP_DEBT_KEY: {attempt_id: record},
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(
            worker,
            "_authoritative_video_attempt_succeeded",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            worker,
            "_authoritative_last_frame_referenced",
            AsyncMock(return_value=False),
        )
        delete_output = AsyncMock(return_value=False)
        monkeypatch.setattr(worker, "_delete_generated_output", delete_output)

        assert await worker.reconcile_video_attempt_cleanup_debts(db) == 0

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert (saved.output_json or {})[worker._ATTEMPT_CLEANUP_DEBT_KEY] == {
            attempt_id: record,
        }
        assert delete_output.await_count == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("publish_fails", [False, True])
def test_attempt_cleanup_schedule_persists_real_database_debt(
    monkeypatch,
    publish_fails: bool,
) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-attempt-debt-publish-failed",
            reservation_id="reservation-attempt-debt-publish-failed",
        )
        attempt_id = "0011223344556677"
        video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
        frame_key = (
            "generate_images/video/frames/"
            f"{seeded.shot_group.shot_group_id}_{attempt_id}_last.png"
        )

        class FakeSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                self.raw = Session(raw.get_bind(), expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            FakeSessionFactory(),
        )
        publish = Mock(
            side_effect=(
                ConnectionError("redis unavailable")
                if publish_fails
                else None
            )
        )
        monkeypatch.setattr(
            worker.reconcile_video_attempt_outputs_task,
            "apply_async",
            publish,
        )

        assert await worker._schedule_video_attempt_cleanup(
            job_id=job.job_id,
            script_id=seeded.script.id,
            shot_group_id=seeded.shot_group.shot_group_id,
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=frame_key,
        ) is True
        publish.assert_called_once()

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert (saved.output_json or {})[worker._ATTEMPT_CLEANUP_DEBT_KEY] == {
            attempt_id: {
                "storage_attempt_id": attempt_id,
                "shot_group_id": seeded.shot_group.shot_group_id,
                "video_storage_key": video_key,
                "last_frame_storage_key": frame_key,
                "retryable": True,
            }
        }

    asyncio.run(scenario())


def test_attempt_cleanup_publish_failure_after_job_delete_enqueues_storage_debt(
    monkeypatch,
) -> None:
    """生命周期删除先胜出时，Redis 故障也不能让已知 attempt 对象失去清理事实。"""

    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-attempt-deleted-before-debt",
            reservation_id="reservation-attempt-deleted-before-debt",
        )
        script_id = seeded.script.id
        shot_group_id = seeded.shot_group.shot_group_id
        attempt_id = "89abcdef01234567"
        video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
        frame_key = (
            "generate_images/video/frames/"
            f"{shot_group_id}_{attempt_id}_last.png"
        )

        # 模拟 episode 删除已提交：父 Script 仍在，VideoJob 与 ShotGroup 已不存在。
        raw.delete(job)
        raw.delete(seeded.shot_group)
        raw.commit()

        class FakeSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                self.raw = Session(raw.get_bind(), expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            FakeSessionFactory(),
        )
        monkeypatch.setattr(
            worker.reconcile_video_attempt_outputs_task,
            "apply_async",
            Mock(side_effect=ConnectionError("redis unavailable")),
        )

        assert await worker._schedule_video_attempt_cleanup(
            job_id=job.job_id,
            script_id=script_id,
            shot_group_id=shot_group_id,
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=frame_key,
        ) is True

        raw.expire_all()
        queued = list(raw.scalars(select(MediaStorageJob)).all())
        assert {row.source_key for row in queued} == {video_key, frame_key}
        assert all(row.job_type == "delete" and row.status == "pending" for row in queued)
        assert raw.get(VideoGenerationJob, job.job_id) is None

    asyncio.run(scenario())


def test_missing_job_cleanup_debt_preserves_authoritative_last_frame(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-deleted-frame-authoritative",
            reservation_id="reservation-deleted-frame-authoritative",
        )
        attempt_id = "76543210fedcba98"
        video_key = f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
        frame_key = (
            "generate_images/video/frames/"
            f"{seeded.shot_group.shot_group_id}_{attempt_id}_last.png"
        )
        seeded.shot_group.reference_frames = {"last_frame": frame_key}
        raw.delete(job)
        raw.commit()

        class FakeSessionFactory:
            def __call__(self):
                return self

            async def __aenter__(self):
                self.raw = Session(raw.get_bind(), expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            FakeSessionFactory(),
        )
        monkeypatch.setattr(
            worker.reconcile_video_attempt_outputs_task,
            "apply_async",
            Mock(side_effect=ConnectionError("redis unavailable")),
        )

        assert await worker._schedule_video_attempt_cleanup(
            job_id=job.job_id,
            script_id=seeded.script.id,
            shot_group_id=seeded.shot_group.shot_group_id,
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=frame_key,
        ) is True

        raw.expire_all()
        queued = list(raw.scalars(select(MediaStorageJob)).all())
        assert [row.source_key for row in queued] == [video_key]
        shot_group = raw.get(ShotGroup, seeded.shot_group.shot_group_id)
        assert shot_group is not None
        assert shot_group.reference_frames["last_frame"] == frame_key

    asyncio.run(scenario())


def test_provider_progress_preserves_attempt_cleanup_debt() -> None:
    async def scenario() -> None:
        from app.crud.video import claim_video_job
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-progress-preserves-attempt-debt",
            reservation_id="reservation-progress-preserves-attempt-debt",
        )
        db = _AsyncSessionAdapter(raw)
        claim = await claim_video_job(db, job.job_id, owner="worker")
        assert claim is not None
        await db.commit()
        attempt_id = "8899aabbccddeeff"
        assert worker._ATTEMPT_CLEANUP_DEBT_KEY not in (job.output_json or {})
        with Session(raw.get_bind(), expire_on_commit=False) as debt_raw:
            debt_db = _AsyncSessionAdapter(debt_raw)
            assert await worker._upsert_video_attempt_cleanup_debt(
                debt_db,
                job_id=job.job_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                storage_attempt_id=attempt_id,
                video_storage_key=(
                    f"generate_images/video/{job.job_id}-{attempt_id}.mp4"
                ),
                last_frame_storage_key=None,
            )
            await debt_db.commit()

        await worker._persist_provider_progress(
            db,
            job.job_id,
            claim.lease_token,
            SimpleNamespace(
                progress=35,
                stage="polling",
                message="生成中",
                provider_task_id="provider-progress-debt",
            ),
        )

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved.output_json["provider_task_id"] == "provider-progress-debt"
        assert attempt_id in saved.output_json[worker._ATTEMPT_CLEANUP_DEBT_KEY]

    asyncio.run(scenario())


def test_finish_preserves_attempt_cleanup_debt_from_independent_session() -> None:
    async def scenario() -> None:
        from app.crud.video import claim_video_job, finish_video_job
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-finish-preserves-attempt-debt",
            reservation_id="reservation-finish-preserves-attempt-debt",
        )
        db = _AsyncSessionAdapter(raw)
        claim = await claim_video_job(db, job.job_id, owner="worker")
        assert claim is not None
        await db.commit()
        attempt_id = "ffeeddccbbaa9988"
        assert worker._ATTEMPT_CLEANUP_DEBT_KEY not in (job.output_json or {})
        with Session(raw.get_bind(), expire_on_commit=False) as debt_raw:
            debt_db = _AsyncSessionAdapter(debt_raw)
            assert await worker._upsert_video_attempt_cleanup_debt(
                debt_db,
                job_id=job.job_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                storage_attempt_id=attempt_id,
                video_storage_key=(
                    f"generate_images/video/orphan-{attempt_id}.mp4"
                ),
                last_frame_storage_key=None,
            )
            await debt_db.commit()

        assert await finish_video_job(
            db,
            job.job_id,
            claim.lease_token,
            "succeeded",
            output_json={
                "video_storage_key": (
                    f"generate_images/video/authoritative-{attempt_id}.mp4"
                )
            },
            metrics={"provider_task_id": "provider-finish-debt"},
        )
        await db.commit()

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert attempt_id in saved.output_json[worker._ATTEMPT_CLEANUP_DEBT_KEY]
        assert saved.output_json["video_storage_key"].endswith(
            f"authoritative-{attempt_id}.mp4"
        )

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 视频 JSON 并发合并测试",
)
@pytest.mark.parametrize("write_kind", ["progress", "finish"])
def test_postgres_video_json_write_preserves_independent_cleanup_debt(
    write_kind: str,
) -> None:
    """旧 Session 的进度/终态写入不能覆盖独立事务刚提交的恢复债务。"""

    async def scenario() -> None:
        from sqlalchemy import delete
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from app.crud import video as video_crud
        from app.workers import video_generation as worker

        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://",
                "postgresql+asyncpg://",
                1,
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        script_id = f"script-json-{suffix}"
        episode_id = f"episode-json-{suffix}"
        shot_group_id = f"group-json-{suffix}"
        job_id = f"video-json-{write_kind}-{suffix}"
        lease_token = f"lease-json-{suffix}"
        storage_attempt_id = suffix.ljust(16, "0")[:16]
        cancellation_record = {
            "provider_task_id": f"provider-cancel-{suffix}",
            "outcome": "pending",
            "retryable": True,
            "attempts": 0,
        }

        try:
            async with session_factory() as seed_db:
                seed_db.add(
                    Script(
                        id=script_id,
                        title="视频 JSON 并发合并测试",
                        scope="personal",
                        owner_user_id=f"user-{suffix}",
                        created_by_user_id=f"user-{suffix}",
                    )
                )
                await seed_db.flush()
                seed_db.add(
                    Episode(
                        episode_id=episode_id,
                        script_id=script_id,
                        episode_no=1,
                        title="第一集",
                    )
                )
                await seed_db.flush()
                seed_db.add(
                    ShotGroup(
                        shot_group_id=shot_group_id,
                        script_id=script_id,
                        episode_id=episode_id,
                        scene_no=1,
                        group_no="G1",
                    )
                )
                await seed_db.flush()
                seed_db.add(
                    VideoGenerationJob(
                        job_id=job_id,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_group_id=shot_group_id,
                        scene_no=1,
                        unit_id=f"group:{shot_group_id}",
                        unit_runtime_key=f"{episode_id}:1:group:{shot_group_id}",
                        provider_key="modelsai",
                        job_type="all_reference",
                        status="processing",
                        progress=20,
                        lease_owner="worker-stale-json",
                        lease_token=lease_token,
                        output_json={"provider_task_id": f"provider-old-{suffix}"},
                        metrics={"provider_task_id": f"provider-old-{suffix}"},
                        input_json={},
                    )
                )
                await seed_db.commit()

            async with session_factory() as stale_db:
                stale_job = await stale_db.get(VideoGenerationJob, job_id)
                assert stale_job is not None
                assert worker._ATTEMPT_CLEANUP_DEBT_KEY not in (
                    stale_job.output_json or {}
                )

                async with session_factory() as debt_db:
                    assert await worker._upsert_video_attempt_cleanup_debt(
                        debt_db,
                        job_id=job_id,
                        shot_group_id=shot_group_id,
                        storage_attempt_id=storage_attempt_id,
                        video_storage_key=(
                            "generate_images/video/"
                            f"orphan-{storage_attempt_id}.mp4"
                        ),
                        last_frame_storage_key=None,
                    )
                    debt_job = await debt_db.get(
                        VideoGenerationJob,
                        job_id,
                        populate_existing=True,
                    )
                    assert debt_job is not None
                    latest_output = dict(debt_job.output_json or {})
                    latest_metrics = dict(debt_job.metrics or {})
                    latest_output["provider_cancellation"] = dict(
                        cancellation_record
                    )
                    latest_metrics["provider_cancellation"] = dict(
                        cancellation_record
                    )
                    debt_job.output_json = latest_output
                    debt_job.metrics = latest_metrics
                    await debt_db.commit()

                incoming_output = {
                    "provider_task_id": f"provider-new-{suffix}",
                }
                incoming_metrics = {
                    "provider_task_id": f"provider-new-{suffix}",
                }
                if write_kind == "progress":
                    updated = await video_crud.update_video_job_progress(
                        stale_db,
                        job_id,
                        progress=45,
                        progress_stage="polling",
                        output_json=incoming_output,
                        metrics=incoming_metrics,
                        lease_token=lease_token,
                    )
                else:
                    incoming_output["video_storage_key"] = (
                        "generate_images/video/"
                        f"authoritative-{storage_attempt_id}.mp4"
                    )
                    updated = await video_crud.finish_video_job(
                        stale_db,
                        job_id,
                        lease_token,
                        "succeeded",
                        output_json=incoming_output,
                        metrics=incoming_metrics,
                    )
                assert updated is True
                await stale_db.commit()

            async with session_factory() as verify_db:
                saved = await verify_db.get(VideoGenerationJob, job_id)
                assert saved is not None
                output_json = saved.output_json or {}
                metrics = saved.metrics or {}
                assert storage_attempt_id in output_json[
                    worker._ATTEMPT_CLEANUP_DEBT_KEY
                ]
                assert output_json["provider_cancellation"] == cancellation_record
                assert metrics["provider_cancellation"] == cancellation_record
                assert output_json["provider_task_id"] == f"provider-new-{suffix}"
                assert metrics["provider_task_id"] == f"provider-new-{suffix}"
        finally:
            async with session_factory() as cleanup_db:
                await cleanup_db.execute(
                    delete(VideoGenerationJob).where(
                        VideoGenerationJob.job_id == job_id
                    )
                )
                await cleanup_db.execute(
                    delete(ShotGroup).where(
                        ShotGroup.shot_group_id == shot_group_id
                    )
                )
                await cleanup_db.execute(
                    delete(Episode).where(Episode.episode_id == episode_id)
                )
                await cleanup_db.execute(
                    delete(Script).where(Script.id == script_id)
                )
                await cleanup_db.commit()
            await engine.dispose()

    asyncio.run(scenario())


def test_worker_reuses_persisted_provider_task_id_after_stale_cleanup(monkeypatch) -> None:
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

        from app.services import video_generation as video_service
        from app.workers import video_generation as worker

        remote_cancel = AsyncMock()
        monkeypatch.setattr(worker, "_credits_settle", AsyncMock())
        monkeypatch.setattr(
            worker,
            "_authoritative_video_attempt_succeeded",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            worker,
            "_authoritative_last_frame_referenced",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            worker,
            "_delete_lost_lease_outputs",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            video_service,
            "_best_effort_cancel_remote_video_task",
            remote_cancel,
        )

        assert await worker.reconcile_stale_video_jobs(db) == [job.job_id]
        raw.expire_all()
        recovered = raw.get(VideoGenerationJob, job.job_id)
        assert recovered is not None
        assert recovered.status == "starting"
        assert (recovered.output_json or {})["provider_task_id"] == "provider-existing"
        assert await worker.reconcile_video_attempt_cleanup_debts(db) == 1
        raw.expire_all()
        after_cleanup = raw.get(VideoGenerationJob, job.job_id)
        assert after_cleanup is not None
        assert (after_cleanup.output_json or {})["provider_task_id"] == "provider-existing"
        assert "attempt_output_cleanup" not in (after_cleanup.output_json or {})

        await worker.run_video_generation_once(
            db,
            "job-recovery",
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

        assert len(seen_metadata) == 1
        assert seen_metadata[0]["provider_task_id"] == "provider-existing"
        storage_attempt_id = seen_metadata[0]["storage_attempt_id"]
        assert len(storage_attempt_id) == 16
        assert int(storage_attempt_id, 16) >= 0
        remote_cancel.assert_not_awaited()

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


def test_stale_video_reconciler_does_not_resubmit_unresolved_submission() -> None:
    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-stale-submission-uncertain",
        reservation_id="reservation-stale-submission-uncertain",
    )
    job.status = "processing"
    job.lease_owner = "dead-worker"
    job.lease_token = "expired-token"
    job.lease_until = datetime.now(UTC) - timedelta(minutes=1)
    job.updated_at = datetime.now(UTC) - timedelta(hours=2)
    submission = {
        "state": "submission_uncertain",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    job.output_json = {"provider_submission": submission}
    job.metrics = {"provider_submission": submission}
    raw.commit()

    async def run() -> None:
        from app.workers.video_generation import reconcile_stale_video_jobs

        assert await reconcile_stale_video_jobs(_AsyncSessionAdapter(raw)) == []

    asyncio.run(run())
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "failed"
    assert saved.progress_stage == "manual_review"
    assert saved.error_type == "provider_submission_uncertain"
    assert saved.retryable is False
    assert saved.finished_at is not None
    assert saved.lease_owner is None
    assert saved.lease_token is None
    assert saved.lease_until is None
    assert reservation.status == "reserved"
    assert raw.scalars(select(TaskDispatchOutbox)).all() == []


def test_cancel_refuses_unresolved_submission_and_keeps_credit_frozen() -> None:
    from app.services.video_generation import cancel_video_job

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancel-submission-uncertain",
        reservation_id="reservation-cancel-submission-uncertain",
    )
    job.status = "processing"
    job.lease_owner = "worker"
    job.lease_token = "lease-token"
    job.lease_until = datetime.now(UTC) + timedelta(minutes=5)
    submission = {
        "state": "submission_uncertain",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    job.output_json = {"provider_submission": submission}
    job.metrics = {"provider_submission": submission}
    raw.commit()

    async def run() -> None:
        with pytest.raises(ConflictError, match="提交状态未知"):
            await cancel_video_job(
                _AsyncSessionAdapter(raw),
                script_id=seeded.script.id,
                job_id=job.job_id,
                actor_user_id=seeded.user.user_id,
            )

    asyncio.run(run())
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "processing"
    assert reservation.status == "reserved"


def test_force_cancel_expired_unresolved_video_job_releases_credit_and_cancels_outbox() -> None:
    from app.services.video_generation import force_cancel_expired_unresolved_video_job

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-force-cancel-submission-uncertain",
        reservation_id="reservation-force-cancel-submission-uncertain",
    )
    job.status = "processing"
    job.lease_owner = "worker"
    job.lease_token = "expired-lease-token"
    job.lease_until = datetime.now(UTC) - timedelta(minutes=1)
    submission = {
        "state": "submission_uncertain",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    job.output_json = {"provider_submission": submission}
    job.metrics = {"provider_submission": submission}
    outbox = TaskDispatchOutbox(
        id="dispatch-force-cancel-submission-uncertain",
        task_type="video.generate",
        job_id=job.job_id,
        attempt=job.attempt,
        payload={"job_id": job.job_id},
        status="pending",
    )
    raw.add(outbox)
    raw.commit()

    async def run() -> None:
        canceled = await force_cancel_expired_unresolved_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            job_id=job.job_id,
            actor_user_id=seeded.user.user_id,
        )
        assert canceled.status == "canceled"

    asyncio.run(run())
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    saved_outbox = raw.get(TaskDispatchOutbox, outbox.id)
    assert saved is not None
    assert reservation is not None
    assert saved_outbox is not None
    assert saved.status == "canceled"
    assert saved.progress_stage == "forced_local_cleanup"
    assert saved.error_type == "forced_local_cleanup"
    assert saved.retryable is False
    assert saved.lease_owner is None
    assert saved.lease_token is None
    assert saved.lease_until is None
    assert reservation.status == "released"
    assert saved_outbox.status == "canceled"


def test_force_cancel_expired_unresolved_video_job_rejects_live_or_known_remote_task() -> None:
    from app.services.video_generation import force_cancel_expired_unresolved_video_job

    _, raw = _make_db()
    seeded = _seed(raw)
    script_id = seeded.script.id
    actor_user_id = seeded.user.user_id
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-force-cancel-rejected",
        reservation_id="reservation-force-cancel-rejected",
    )
    job_id = job.job_id
    job.status = "processing"
    job.lease_owner = "worker"
    job.lease_token = "live-lease-token"
    job.lease_until = datetime.now(UTC) + timedelta(minutes=1)
    submission = {
        "state": "submission_uncertain",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    job.output_json = {"provider_submission": submission}
    job.metrics = {"provider_submission": submission}
    raw.commit()

    async def run() -> None:
        with pytest.raises(ConflictError, match="租约尚未过期"):
                await force_cancel_expired_unresolved_video_job(
                    _AsyncSessionAdapter(raw),
                    script_id=script_id,
                    job_id=job_id,
                    actor_user_id=actor_user_id,
            )

        job.lease_until = datetime.now(UTC) - timedelta(minutes=1)
        job.output_json = {
            "provider_submission": submission,
            "provider_task_id": "remote-task-known",
        }
        job.metrics = {
            "provider_submission": submission,
            "provider_task_id": "remote-task-known",
        }
        raw.commit()
        with pytest.raises(ConflictError, match="远端 ID"):
                await force_cancel_expired_unresolved_video_job(
                    _AsyncSessionAdapter(raw),
                    script_id=script_id,
                    job_id=job_id,
                    actor_user_id=actor_user_id,
            )

    asyncio.run(run())
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "processing"
    assert reservation.status == "reserved"


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
        with pytest.raises(BadRequestError):
            await api.batch_create_video_generation_jobs(
                VideoJobBatchCreateIn(
                    script_id=seeded.script.id,
                    episode_id=seeded.episode.episode_id,
                    shot_group_ids=[seeded.shot_group.shot_group_id],
                ),
                db=db,
                user=seeded.user,
                idempotency_key="   ",
            )

        create.assert_not_awaited()

    asyncio.run(scenario())


def test_batch_create_forwards_prompts_by_shot_group_and_rejects_unknown_keys_before_create(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoJobBatchCreateIn

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-batch-prompt-forward",
            reservation_id="reservation-batch-prompt-forward",
        )
        create = AsyncMock(return_value=job)
        monkeypatch.setattr("app.services.video_generation.create_video_job", create)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))

        with pytest.raises(BadRequestError) as exc_info:
            await api.batch_create_video_generation_jobs(
                VideoJobBatchCreateIn(
                    script_id=seeded.script.id,
                    episode_id=seeded.episode.episode_id,
                    shot_group_ids=["sg-10"],
                    prompts={"sg-unknown": "未知镜头组提示词"},
                ),
                db=db,
                user=seeded.user,
                idempotency_key="batch-prompt-unknown",
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "prompts 包含非目标镜头组"
        create.assert_not_awaited()

        result = await api.batch_create_video_generation_jobs(
            VideoJobBatchCreateIn(
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_ids=["sg-10", "sg-2"],
                prompts={
                    "sg-10": "  第十组客户端提示词  ",
                    "sg-2": "第二组客户端提示词",
                },
            ),
            db=db,
            user=seeded.user,
            idempotency_key="batch-prompt-forward",
        )

        assert len(result.success) == 2
        assert result.failed == []
        assert [call.kwargs["shot_group_id"] for call in create.await_args_list] == [
            "sg-10",
            "sg-2",
        ]
        assert [call.kwargs["prompt"] for call in create.await_args_list] == [
            "第十组客户端提示词",
            "第二组客户端提示词",
        ]

    asyncio.run(scenario())


def test_single_create_forwards_client_prompt(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoJobCreateIn

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-single-prompt-forward",
            reservation_id="reservation-single-prompt-forward",
        )
        create = AsyncMock(return_value=job)
        monkeypatch.setattr("app.services.video_generation.create_video_job", create)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))

        result = await api.create_video_generation_job(
            VideoJobCreateIn(
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                prompt="  单个客户端视频提示词  ",
            ),
            db=_AsyncSessionAdapter(raw),
            user=seeded.user,
            idempotency_key="single-prompt-forward",
        )

        assert result.job_id == job.job_id
        create.assert_awaited_once()
        assert create.await_args.kwargs["prompt"] == "单个客户端视频提示词"

    asyncio.run(scenario())


def test_video_api_does_not_forward_style_id(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoJobBatchCreateIn, VideoJobCreateIn

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-no-style-forward",
            reservation_id="reservation-no-style-forward",
        )
        create = AsyncMock(return_value=job)
        monkeypatch.setattr("app.services.video_generation.create_video_job", create)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))

        await api.create_video_generation_job(
            VideoJobCreateIn(
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
            ),
            db=_AsyncSessionAdapter(raw),
            user=seeded.user,
            idempotency_key="no-style-forward-single",
        )
        assert "style_id" not in create.await_args.kwargs

        create.reset_mock()
        await api.batch_create_video_generation_jobs(
            VideoJobBatchCreateIn(
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_ids=[seeded.shot_group.shot_group_id],
            ),
            db=_AsyncSessionAdapter(raw),
            user=seeded.user,
            idempotency_key="no-style-forward-batch",
        )
        assert "style_id" not in create.await_args.kwargs

    asyncio.run(scenario())


def test_batch_create_rolls_back_each_failed_item(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoJobBatchCreateIn

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))
        create = AsyncMock(
            side_effect=ForbiddenError("镜头组不属于该剧本/分集")
        )
        monkeypatch.setattr("app.services.video_generation.create_video_job", create)

        result = await api.batch_create_video_generation_jobs(
            VideoJobBatchCreateIn(
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_ids=["sg-foreign"],
            ),
            db=db,
            user=seeded.user,
            idempotency_key="batch-rollback",
        )

        assert result.success == []
        assert [item.shot_group_id for item in result.failed] == ["sg-foreign"]
        assert db.rollback_count == 1

    asyncio.run(scenario())


def test_video_creation_schemas_exclude_style_id() -> None:
    """视频创建契约不再公开风格元数据字段。"""
    from app.schemas.video import VideoJobBatchCreateIn, VideoJobCreateIn

    assert "style_id" not in VideoJobCreateIn.model_fields
    assert "style_id" not in VideoJobBatchCreateIn.model_fields


def test_video_create_service_removes_style_id_metadata_and_ignores_legacy_value(monkeypatch) -> None:
    async def scenario() -> None:
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)

        async def fake_enqueue(db, **kwargs):
            row = TaskDispatchOutbox(
                id="dispatch-no-style-metadata",
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
            idempotency_key="no-style-metadata",
        )

        assert "style_id" not in job.input_json

        job.input_json = {**job.input_json, "style_id": "legacy-style"}
        raw.commit()
        reused = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="no-style-metadata-replay",
        )
        assert reused.job_id == job.job_id

        with pytest.raises(TypeError, match="unexpected keyword argument 'style_id'"):
            await video_generation.create_video_job(
                None,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=seeded.shot_group.shot_group_id,
                user_id=seeded.user.user_id,
                idempotency_key="legacy-style-argument",
                style_id="legacy-style",
            )

    asyncio.run(scenario())


def test_video_batch_rejects_more_than_one_hundred_shot_groups() -> None:
    from app.schemas.video import VideoJobBatchCreateIn

    with pytest.raises(ValidationError):
        VideoJobBatchCreateIn(
            script_id="script-1",
            episode_id="episode-1",
            shot_group_ids=[f"sg-{index}" for index in range(101)],
        )


async def test_video_idempotency_key_http_length_contract(client, monkeypatch) -> None:
    from app.api.v1 import video_generation as api
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True)

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user
    load_script = AsyncMock(side_effect=AssertionError("超长 header 不应进入路由业务逻辑"))
    create = AsyncMock(side_effect=AssertionError("超长 header 不应创建任务"))
    monkeypatch.setattr(api, "_load_and_check_script", load_script)
    monkeypatch.setattr("app.services.video_generation.create_video_job", create)

    single_response = await client.post(
        "/v3api/video-generation-jobs",
        headers={"Idempotency-Key": "x" * 257},
        json={
            "script_id": "script-1",
            "episode_id": "episode-1",
            "shot_group_id": "sg-1",
        },
    )
    batch_response = await client.post(
        "/v3api/video-generation-jobs/batch",
        headers={"Idempotency-Key": "x" * 257},
        json={
            "script_id": "script-1",
            "episode_id": "episode-1",
            "shot_group_ids": ["sg-1"],
        },
    )

    assert single_response.status_code == 422
    assert single_response.json() == {"detail": "字段校验失败"}
    assert batch_response.status_code == 422
    assert batch_response.json() == {"detail": "字段校验失败"}
    load_script.assert_not_awaited()
    create.assert_not_awaited()


async def test_video_batch_rejects_derived_idempotency_overflow_before_creation(
    client, monkeypatch
) -> None:
    from app.api.v1 import video_generation as api
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True)

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user
    monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=SimpleNamespace()))
    create = AsyncMock(side_effect=AssertionError("派生键校验必须发生在首个任务创建前"))
    monkeypatch.setattr("app.services.video_generation.create_video_job", create)

    response = await client.post(
        "/v3api/video-generation-jobs/batch",
        headers={"Idempotency-Key": "x" * 255},
        json={
            "script_id": "script-1",
            "episode_id": "episode-1",
            "shot_group_ids": ["sg-1", "sg-2"],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "批量 Idempotency-Key 派生后超过 256 字符"}
    create.assert_not_awaited()


@pytest.mark.parametrize(
    ("configured_provider", "configured_model", "expected_provider", "expected_model"),
    [
        ("modelsai", "", "volcengine", "seedance-env-test"),
        ("dashscope", "", "bailian", "wan-env-test"),
        ("sora", "", "openai", "sora-env-test"),
        ("google", "", "gemini", "veo-env-test"),
        ("google", "explicit-video-model", "gemini", "explicit-video-model"),
        ("aicc", "", "mgrouter", "aicc-env-test"),
    ],
)
async def test_active_provider_env_mode_get_returns_effective_config_without_db(
    client,
    monkeypatch,
    configured_provider: str,
    configured_model: str,
    expected_provider: str,
    expected_model: str,
) -> None:
    from app.api.v1 import video_generation as api
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True, is_admin=False)

    configured = SimpleNamespace(
        video_provider_selection_mode="env",
        video_gen_provider=configured_provider,
        video_gen_model=configured_model,
        seedance_model="seedance-env-test",
        dashscope_video_model="wan-env-test",
        openai_video_model="sora-env-test",
        google_video_model="veo-env-test",
        mgrouter_video_model="aicc-env-test",
    )
    get_database_provider = AsyncMock(
        return_value=SimpleNamespace(
            provider_key="modelsai",
            job_type="all_reference",
            model="database-model",
        )
    )
    monkeypatch.setattr(api, "settings", configured, raising=False)
    monkeypatch.setattr(api.video_crud, "get_active_provider", get_database_provider)
    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user

    response = await client.get("/v3api/video-generation/active-provider")

    assert response.status_code == 200
    assert response.json() == {
        "provider_key": expected_provider,
        "job_type": "all_reference",
        "model": expected_model,
    }
    get_database_provider.assert_not_awaited()


async def test_active_provider_env_mode_rejects_admin_switch_without_db_mutation(
    client, monkeypatch
) -> None:
    from app.api.v1 import video_generation as api
    from app.core.auth import get_current_user
    from app.core.database import get_db

    async def override_user():
        return SimpleNamespace(user_id="admin-1", enabled=True, is_admin=True)

    commit = AsyncMock()

    async def override_db():
        yield SimpleNamespace(commit=commit)

    configured = SimpleNamespace(video_provider_selection_mode="env")
    ensure_configs = AsyncMock()
    provider = SimpleNamespace(
        provider_key="gemini",
        job_type="all_reference",
        model="veo-database-test",
    )
    get_provider = AsyncMock(return_value=provider)
    get_active = AsyncMock(
        return_value=SimpleNamespace(
            provider_key="modelsai",
            job_type="all_reference",
            model="seedance-database-test",
        )
    )
    set_active = AsyncMock(return_value=provider)
    audit = AsyncMock()
    monkeypatch.setattr(api, "settings", configured, raising=False)
    monkeypatch.setattr(api.video_crud, "ensure_provider_configs", ensure_configs)
    monkeypatch.setattr(api.video_crud, "get_provider_by_key", get_provider)
    monkeypatch.setattr(api.video_crud, "get_active_provider", get_active)
    monkeypatch.setattr(api.video_crud, "set_active_provider", set_active)
    monkeypatch.setattr(api, "audit_log", audit)
    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user
    app.dependency_overrides[get_db] = override_db

    response = await client.post(
        "/v3api/video-generation/active-provider",
        json={"provider_key": "gemini", "job_type": "all_reference"},
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": "视频供应商由环境配置锁定，无法通过 API 切换"
    }
    ensure_configs.assert_not_awaited()
    get_provider.assert_not_awaited()
    get_active.assert_not_awaited()
    set_active.assert_not_awaited()
    commit.assert_not_awaited()
    audit.assert_not_awaited()


def test_active_provider_initial_seed_is_committed(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api

        monkeypatch.setattr(api.settings, "video_provider_selection_mode", "database")
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


def test_active_provider_switch_seeds_gemini_for_existing_database(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import ActiveProviderUpdateIn

        monkeypatch.setattr(api.settings, "video_provider_selection_mode", "database")
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        audit = AsyncMock()
        monkeypatch.setattr(api, "audit_log", audit)

        out = await api.set_active_provider(
            body=ActiveProviderUpdateIn(provider_key="gemini"),
            db=db,
            user=SimpleNamespace(user_id=seeded.user.user_id, is_admin=True),
        )

        assert out.provider_key == "gemini"
        assert out.model
        assert raw.scalar(
            select(VideoProviderConfig).where(
                VideoProviderConfig.provider_key == "gemini",
                VideoProviderConfig.is_active.is_(True),
            )
        ) is not None
        audit.assert_awaited_once()

    asyncio.run(scenario())


def test_active_provider_switch_seeds_mgrouter_for_existing_database(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import ActiveProviderUpdateIn

        monkeypatch.setattr(api.settings, "video_provider_selection_mode", "database")
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(api, "audit_log", AsyncMock())

        out = await api.set_active_provider(
            body=ActiveProviderUpdateIn(provider_key="mgrouter"),
            db=db,
            user=SimpleNamespace(user_id=seeded.user.user_id, is_admin=True),
        )

        assert out.provider_key == "mgrouter"
        assert out.model == "AICC-Doubao-Seedance-2.0"
        assert raw.scalar(
            select(VideoProviderConfig).where(
                VideoProviderConfig.provider_key == "mgrouter",
                VideoProviderConfig.is_active.is_(True),
            )
        ) is not None

    asyncio.run(scenario())


def test_active_provider_switch_seeds_neolink_for_existing_database(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import ActiveProviderUpdateIn

        monkeypatch.setattr(api.settings, "video_provider_selection_mode", "database")
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(api, "audit_log", AsyncMock())

        out = await api.set_active_provider(
            body=ActiveProviderUpdateIn(provider_key="neolink"),
            db=db,
            user=SimpleNamespace(user_id=seeded.user.user_id, is_admin=True),
        )

        assert out.provider_key == "neolink"
        assert out.model == "doubao-seedance-2-0-260128"
        assert raw.scalar(
            select(VideoProviderConfig).where(
                VideoProviderConfig.provider_key == "neolink",
                VideoProviderConfig.is_active.is_(True),
            )
        ) is not None

    asyncio.run(scenario())


def test_video_provider_and_job_type_constraints_are_declared() -> None:
    job_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in VideoGenerationJob.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    provider_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in VideoProviderConfig.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "ck_video_jobs_provider_key" in job_checks
    assert "ck_video_jobs_job_type" in job_checks
    assert "ck_video_provider_configs_provider_key" in provider_checks
    assert "ck_video_provider_configs_job_type" in provider_checks
    assert "'gemini'" in job_checks["ck_video_jobs_provider_key"]
    assert "'gemini'" in provider_checks["ck_video_provider_configs_provider_key"]
    assert "'mgrouter'" in job_checks["ck_video_jobs_provider_key"]
    assert "'mgrouter'" in provider_checks["ck_video_provider_configs_provider_key"]
    assert "'neolink'" in job_checks["ck_video_jobs_provider_key"]
    assert "'neolink'" in provider_checks["ck_video_provider_configs_provider_key"]


def test_active_provider_schema_accepts_canonical_gemini() -> None:
    from app.schemas.video import ActiveProviderUpdateIn

    payload = ActiveProviderUpdateIn(provider_key="gemini")

    assert payload.provider_key == "gemini"


def test_active_provider_schema_accepts_canonical_mgrouter() -> None:
    from app.schemas.video import ActiveProviderUpdateIn

    payload = ActiveProviderUpdateIn(provider_key="mgrouter")

    assert payload.provider_key == "mgrouter"


def test_active_provider_schema_accepts_canonical_neolink() -> None:
    from app.schemas.video import ActiveProviderUpdateIn

    payload = ActiveProviderUpdateIn(provider_key="neolink")

    assert payload.provider_key == "neolink"


@pytest.mark.parametrize("alias", ["google", "gemini", "veo"])
def test_video_service_persists_gemini_aliases_as_canonical_provider(
    monkeypatch, alias: str
) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.core.config import Settings
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.shot_group.audio_elements = {"characters": [], "sounds": []}
        db = _AsyncSessionAdapter(raw)
        configured = Settings(
            _env_file=None,
            video_provider_selection_mode="env",
            video_gen_provider=alias,
            video_gen_api_key="test-video-key",
            video_gen_model="",
            google_video_model="veo-google-test",
        )
        monkeypatch.setattr(api, "settings", configured, raising=False)
        monkeypatch.setattr(video_generation, "settings", configured)
        monkeypatch.setattr(video_generation, "_credits_reserve", None)

        active = await api.get_active_provider(db=db, user=seeded.user)
        job = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key=f"gemini-alias-{alias}",
        )

        assert active.provider_key == job.provider_key
        assert active.model == job.input_json["model"]
        assert job.provider_key == "gemini"
        assert job.input_json["model"] == "veo-google-test"

    asyncio.run(scenario())


def test_video_service_uses_database_selected_gemini_provider(monkeypatch) -> None:
    async def scenario() -> None:
        from app.core.config import Settings
        from app.crud.video import set_active_provider
        from app.services import video_generation

        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.shot_group.audio_elements = {"characters": [], "sounds": []}
        raw.add(
            VideoProviderConfig(
                id="provider-gemini",
                provider_key="gemini",
                job_type="all_reference",
                model="veo-database-test",
                is_active=False,
            )
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        await set_active_provider(db, provider_key="gemini")
        await db.commit()
        configured = Settings(
            _env_file=None,
            video_provider_selection_mode="database",
        )
        monkeypatch.setattr(video_generation, "settings", configured)
        monkeypatch.setattr(video_generation, "_credits_reserve", None)

        job = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="gemini-database-mode",
        )

        assert job.provider_key == "gemini"
        assert job.input_json["model"] == "veo-database-test"

    asyncio.run(scenario())


def test_gemini_service_job_routes_through_google_worker_factory(monkeypatch) -> None:
    async def scenario() -> None:
        from app.core.config import Settings
        from app.providers import video_gen
        from app.providers.video_gen import VideoGenerationResult
        from app.providers.video_gen_google import GoogleVideoGenProvider
        from app.services import video_generation
        from app.workers.video_generation import run_video_generation_once

        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.shot_group.audio_elements = {"characters": [], "sounds": []}
        db = _AsyncSessionAdapter(raw)
        configured = Settings(
            _env_file=None,
            video_provider_selection_mode="env",
            video_gen_provider="veo",
            video_gen_api_key="test-video-key",
            video_gen_model="",
            google_video_model="veo-google-test",
        )
        monkeypatch.setattr(video_generation, "settings", configured)
        monkeypatch.setattr(video_generation, "_credits_reserve", None)

        job = await video_generation.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="gemini-worker-route",
        )

        factory_keys: list[str] = []
        generated = AsyncMock(
            return_value=VideoGenerationResult(
                video_storage_key="generate_images/video/gemini-worker-route.mp4",
                provider_job_id="operations/gemini-worker-route",
            )
        )
        real_factory = video_gen.get_video_provider

        def observed_factory(provider_key: str):
            factory_keys.append(provider_key)
            provider = real_factory(provider_key)
            assert isinstance(provider, GoogleVideoGenProvider)
            provider.generate = generated  # type: ignore[method-assign]
            return provider

        monkeypatch.setattr(video_gen, "get_video_provider", observed_factory)
        await run_video_generation_once(
            db,
            job.job_id,
            owner="gemini-route-worker",
            extract_last_frame=AsyncMock(return_value=None),
        )

        assert factory_keys == ["gemini"]
        generated.assert_awaited_once()
        request = generated.await_args.args[0]
        assert request.model == "veo-google-test"
        assert raw.get(VideoGenerationJob, job.job_id).status == "succeeded"

    asyncio.run(scenario())


def test_financial_cleanup_has_schedulable_celery_task() -> None:
    from app.workers import video_generation as worker

    assert worker.reconcile_video_financial_cleanup_task.name == (
        "app.workers.video_generation.reconcile_video_financial_cleanup"
    )
    assert callable(worker.reconcile_video_financial_cleanup_task.delay)


def test_remote_cancellation_cleanup_has_registered_beat_task() -> None:
    from app.workers import video_generation as worker
    from app.workers.celery_app import app as celery_app

    task_name = "app.workers.video_generation.reconcile_video_remote_cancellations"
    schedule = celery_app.conf.beat_schedule["video-remote-cancellation-cleanup"]

    assert worker.reconcile_video_remote_cancellations_task.name == task_name
    assert callable(worker.reconcile_video_remote_cancellations_task.delay)
    assert schedule["task"] == task_name
    assert schedule["schedule"] == 60.0


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


async def test_video_generation_jobs_list_orders_by_group_index_and_exposes_group_identity(
    client,
) -> None:
    from app.core.auth import get_current_user
    from app.core.database import get_db

    engine, raw = _make_db()
    seeded = _seed(raw)
    group_01 = ShotGroup(
        shot_group_id="sg-01",
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        scene_no=1,
        group_index=1,
        group_no="G1",
        prompt="镜头组 01",
    )
    group_04 = ShotGroup(
        shot_group_id="sg-04",
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        scene_no=1,
        group_index=4,
        group_no="G4",
        prompt="镜头组 04",
    )
    created_at = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)

    def make_job(
        job_id: str,
        shot_group_id: str,
        group_no: str,
        attempt: int,
        created: datetime,
    ) -> VideoGenerationJob:
        return VideoGenerationJob(
            job_id=job_id,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=f"group:{shot_group_id}",
            unit_runtime_key=f"episode-1:1:{group_no}:{attempt}",
            provider_key="modelsai",
            job_type="all_reference",
            idempotency_key=f"order-{job_id}",
            request_hash="h",
            input_version=1,
            attempt=attempt,
            status="succeeded",
            input_json={},
            created_at=created,
        )

    # 创建顺序是 01、02、04；旧接口按创建时间倒序时会返回 04、02、01。
    # 同一镜头组的相同创建时间必须继续按 job_id 倒序，避免数据库返回不稳定。
    raw.add_all([
        group_01,
        group_04,
        make_job("job-01", "sg-01", "G1", 1, created_at + timedelta(seconds=1)),
        make_job("job-02", "sg-2", "G2", 1, created_at + timedelta(seconds=2)),
        make_job("job-04-a", "sg-04", "G4", 1, created_at + timedelta(seconds=3)),
        make_job("job-04-z", "sg-04", "G4", 2, created_at + timedelta(seconds=3)),
    ])
    raw.commit()
    db = _AsyncSessionAdapter(raw)

    async def override_db():
        yield db

    async def override_user():
        return seeded.user

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_current_user] = override_user
    try:
        response = await client.get(
            "/v3api/video-generation-jobs",
            params={
                "script_id": seeded.script.id,
                "episode_id": seeded.episode.episode_id,
                "limit": 500,
            },
        )
    finally:
        app.dependency_overrides.clear()
        engine.dispose()

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 4
    assert "group_index" in body["items"][0]
    assert "group_no" in body["items"][0]
    assert [
        (item["group_index"], item["group_no"], item["shot_group_id"], item["job_id"])
        for item in body["items"]
    ] == [
        (1, "G1", "sg-01", "job-01"),
        (2, "G2", "sg-2", "job-02"),
        (4, "G4", "sg-04", "job-04-z"),
        (4, "G4", "sg-04", "job-04-a"),
    ]


def test_volcengine_cancel_calls_tasks_delete_and_closes_client(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        deleted: list[str] = []

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                deleted.append(task_id)

        class Client:
            def __init__(self) -> None:
                self.content_generation = SimpleNamespace(tasks=Tasks())
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        client = Client()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

        result = await provider.cancel("provider-task-known")

        assert result.provider_task_id == "provider-task-known"
        assert result.outcome is VideoCancellationOutcome.CANCELED
        assert deleted == ["provider-task-known"]
        assert client.closed is True

    asyncio.run(scenario())


@pytest.mark.parametrize("signal_name", ["soft", "hard"])
def test_volcengine_cancel_propagates_time_limit_signals(
    monkeypatch,
    signal_name: str,
) -> None:
    async def scenario() -> None:
        from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        signal = (
            SoftTimeLimitExceeded()
            if signal_name == "soft"
            else TimeLimitExceeded()
        )

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == f"provider-task-{signal_name}-limit"
                raise signal

        class Client:
            content_generation = SimpleNamespace(tasks=Tasks())

            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        client = Client()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

        with pytest.raises(type(signal)) as exc_info:
            await provider.cancel(f"provider-task-{signal_name}-limit")

        assert exc_info.value is signal
        assert client.closed is True

    asyncio.run(scenario())


def test_volcengine_cancel_treats_structured_not_found_as_already_canceled() -> None:
    async def scenario() -> None:
        import httpx
        from volcenginesdkarkruntime._exceptions import ArkNotFoundError

        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        request = httpx.Request(
            "DELETE",
            "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks/task",
        )
        response = httpx.Response(404, request=request)
        error = ArkNotFoundError(
            "opaque sdk error",
            response=response,
            body={"error": {"code": "opaque"}},
            request_id="request-id",
        )

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == "provider-task-already-gone"
                raise error

        result = await VolcengineVideoGenProvider._cancel_task(
            Tasks(),
            "provider-task-already-gone",
        )

        assert result.provider_task_id == "provider-task-already-gone"
        assert result.outcome is VideoCancellationOutcome.CANCELED
        assert result.detail == "火山方舟远端任务已不存在"

    asyncio.run(scenario())


def test_volcengine_cancel_does_not_treat_other_errors_as_not_found() -> None:
    async def scenario() -> None:
        import httpx
        from volcenginesdkarkruntime._exceptions import (
            ArkAuthenticationError,
            ArkInternalServerError,
            ArkPermissionDeniedError,
            ArkRateLimitError,
        )

        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        error_cases = [
            (401, ArkAuthenticationError),
            (403, ArkPermissionDeniedError),
            (429, ArkRateLimitError),
            (500, ArkInternalServerError),
        ]
        errors: list[Exception] = []
        for status_code, error_type in error_cases:
            request = httpx.Request(
                "DELETE",
                "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks/task",
            )
            response = httpx.Response(status_code, request=request)
            errors.append(
                error_type(
                    "opaque sdk error",
                    response=response,
                    body=None,
                    request_id="request-id",
                )
            )
        errors.append(RuntimeError("task not found in an unstructured message"))

        for error in errors:
            class Tasks:
                async def delete(self, *, task_id: str) -> None:
                    assert task_id == "provider-task-error"
                    raise error

            result = await VolcengineVideoGenProvider._cancel_task(
                Tasks(),
                "provider-task-error",
            )
            assert result.outcome is VideoCancellationOutcome.FAILED

    asyncio.run(scenario())


def test_volcengine_cancel_uses_short_sdk_timeout_and_total_deadline(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers import video_gen_volcengine as module
        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        delete_started = asyncio.Event()
        delete_stopped = asyncio.Event()
        build_kwargs: list[dict] = []

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == "provider-task-timeout"
                delete_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    delete_stopped.set()

        class Client:
            def __init__(self) -> None:
                self.content_generation = SimpleNamespace(tasks=Tasks())
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        client = Client()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)

        def build_client(**kwargs):
            build_kwargs.append(kwargs)
            return client

        monkeypatch.setattr(provider, "_build_client", build_client)
        monkeypatch.setattr(module, "_CANCEL_TOTAL_TIMEOUT", 0.01, raising=False)

        result = await asyncio.wait_for(
            provider.cancel("provider-task-timeout"),
            timeout=0.2,
        )

        assert result.outcome is VideoCancellationOutcome.FAILED
        assert result.provider_task_id == "provider-task-timeout"
        assert result.detail and "Timeout" in result.detail
        assert build_kwargs == [
            {"timeout": module._CANCEL_SDK_TIMEOUT, "max_retries": 0}
        ]
        assert delete_started.is_set()
        assert delete_stopped.is_set()
        assert client.closed is True

    asyncio.run(scenario())


def test_volcengine_cancel_close_failure_does_not_override_result(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == "provider-task-close-failure"

        class Client:
            content_generation = SimpleNamespace(tasks=Tasks())

            async def close(self) -> None:
                raise RuntimeError("close_failed")

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: Client())

        result = await provider.cancel("provider-task-close-failure")

        assert result.outcome is VideoCancellationOutcome.CANCELED

    asyncio.run(scenario())


def test_volcengine_cancel_hanging_close_has_own_deadline(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers import video_gen
        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        close_stopped = asyncio.Event()

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == "provider-task-close-timeout"

        class Client:
            content_generation = SimpleNamespace(tasks=Tasks())

            async def close(self) -> None:
                try:
                    await asyncio.Event().wait()
                finally:
                    close_stopped.set()

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: Client())
        monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)

        result = await asyncio.wait_for(
            provider.cancel("provider-task-close-timeout"),
            timeout=0.2,
        )

        assert result.outcome is VideoCancellationOutcome.CANCELED
        assert close_stopped.is_set()

    asyncio.run(scenario())


def test_volcengine_generation_close_that_swallows_cancellation_has_hard_deadline(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        import time

        from app.providers import video_gen
        from app.providers.video_gen import VideoGenerationRemoteTerminalError
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        primary_error = VideoGenerationRemoteTerminalError(
            provider_task_id="volcengine-generation-stubborn-close",
            terminal_status="failed",
        )
        close_cancelled = asyncio.Event()
        close_finished = asyncio.Event()
        release_close = asyncio.Event()

        class ContentGeneration:
            @property
            def tasks(self):
                raise primary_error

        class Client:
            content_generation = ContentGeneration()

            async def close(self) -> None:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    close_cancelled.set()
                    current = asyncio.current_task()
                    assert current is not None
                    while current.cancelling():
                        current.uncancel()
                    await release_close.wait()
                finally:
                    close_finished.set()

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_build_client", lambda: Client())
        monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)
        asyncio.get_running_loop().call_later(0.15, release_close.set)

        started_at = time.monotonic()
        with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
            await provider._submit_and_poll(
                "doubao-seedance-2-0-260128",
                "镜头提示词",
                [],
                [],
                None,
                lambda *_args: None,
            )
        elapsed = time.monotonic() - started_at

        assert exc_info.value is primary_error
        assert close_cancelled.is_set()
        assert elapsed < 0.08
        assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1
        await asyncio.wait_for(close_finished.wait(), timeout=0.3)
        await asyncio.sleep(0)
        assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()

    asyncio.run(scenario())


def test_volcengine_cancel_close_that_swallows_cancellation_has_hard_deadline(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        import time

        from app.providers import video_gen
        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        close_cancelled = asyncio.Event()
        close_finished = asyncio.Event()
        release_close = asyncio.Event()

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == "volcengine-cancel-stubborn-close"

        class Client:
            content_generation = SimpleNamespace(tasks=Tasks())

            async def close(self) -> None:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    close_cancelled.set()
                    current = asyncio.current_task()
                    assert current is not None
                    while current.cancelling():
                        current.uncancel()
                    await release_close.wait()
                finally:
                    close_finished.set()

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: Client())
        monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)
        asyncio.get_running_loop().call_later(0.15, release_close.set)

        started_at = time.monotonic()
        result = await provider.cancel("volcengine-cancel-stubborn-close")
        elapsed = time.monotonic() - started_at

        assert result.outcome is VideoCancellationOutcome.CANCELED
        assert close_cancelled.is_set()
        assert elapsed < 0.08
        assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1
        await asyncio.wait_for(close_finished.wait(), timeout=0.3)
        await asyncio.sleep(0)
        assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()

    asyncio.run(scenario())


def test_volcengine_cancel_reserves_one_cross_loop_close_slot_atomically(
    monkeypatch,
) -> None:
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    from app.providers import video_gen
    from app.providers.video_gen import VideoCancellationOutcome
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    concurrency = 24
    enter_close = threading.Barrier(concurrency)
    release_close = threading.Event()
    counter_lock = threading.Lock()
    close_started = 0
    contexts_returned = 0

    async def invoke(index: int) -> None:
        nonlocal close_started, contexts_returned

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                assert task_id == f"volcengine-concurrent-close-{index}"
                enter_close.wait(timeout=5)

        class Client:
            content_generation = SimpleNamespace(tasks=Tasks())

            async def close(self) -> None:
                nonlocal close_started
                with counter_lock:
                    close_started += 1
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    assert current is not None
                    while not release_close.is_set():
                        while current.cancelling():
                            current.uncancel()
                        try:
                            await asyncio.sleep(0.005)
                        except asyncio.CancelledError:
                            continue

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        provider._check_keys = lambda: None  # type: ignore[method-assign]
        provider._build_client = lambda **_kwargs: Client()  # type: ignore[method-assign]
        result = await provider.cancel(f"volcengine-concurrent-close-{index}")
        assert result.outcome is VideoCancellationOutcome.CANCELED
        with counter_lock:
            contexts_returned += 1

    def run_in_thread(index: int) -> None:
        asyncio.run(invoke(index))

    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.02, raising=False)
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_in_thread, index) for index in range(concurrency)]
            try:
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    with counter_lock:
                        if close_started == concurrency or contexts_returned >= concurrency - 1:
                            break
                    time.sleep(0.005)
                with counter_lock:
                    assert close_started == 1
                deadline = time.monotonic() + 0.5
                while (
                    not video_gen._VIDEO_CLIENT_CLOSE_ORPHANS
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.005)
                assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1
            finally:
                release_close.set()
            for future in futures:
                future.result(timeout=2)
    finally:
        release_close.set()

    deadline = time.monotonic() + 1
    while video_gen._VIDEO_CLIENT_CLOSE_ORPHANS and time.monotonic() < deadline:
        time.sleep(0.005)
    assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()

    closed: list[bool] = []

    class ProbeTasks:
        async def delete(self, *, task_id: str) -> None:
            assert task_id == "volcengine-sync-close-probe"

    class ProbeClient:
        content_generation = SimpleNamespace(tasks=ProbeTasks())

        def close(self) -> None:
            closed.append(True)

    async def run_probe() -> None:
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        provider._check_keys = lambda: None  # type: ignore[method-assign]
        provider._build_client = lambda **_kwargs: ProbeClient()  # type: ignore[method-assign]
        result = await provider.cancel("volcengine-sync-close-probe")
        assert result.outcome is VideoCancellationOutcome.CANCELED

    asyncio.run(run_probe())
    assert closed == [True]


def test_volcengine_submit_tasks_lookup_and_close_preserve_original_error(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        class ContentGeneration:
            @property
            def tasks(self):
                raise RuntimeError("tasks_lookup_failed")

        class Client:
            content_generation = ContentGeneration()

            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True
                raise RuntimeError("close_failed")

        client = Client()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_build_client", lambda: client)

        with pytest.raises(RuntimeError, match="tasks_lookup_failed"):
            await provider._submit_and_poll(
                "doubao-seedance-2-0-260128",
                "镜头提示词",
                [],
                [],
                None,
                lambda *_args: None,
            )

        assert client.closed is True

    asyncio.run(scenario())


def test_volcengine_checks_cancellation_before_downloading(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
            VideoGenerationCancelled,
            VideoGenerationRequest,
        )
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_preflight_reference_sizes", AsyncMock())
        monkeypatch.setattr(
            provider,
            "_submit_and_poll",
            AsyncMock(
                return_value=(
                    "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/video.mp4",
                    {"task_id": "provider-task-download", "volcengine_status": "succeeded"},
                )
            ),
        )
        download = AsyncMock(side_effect=AssertionError("取消后不得开始下载"))
        monkeypatch.setattr(provider, "_download_to_file", download)
        monkeypatch.setattr(
            provider,
            "cancel",
            AsyncMock(
                return_value=VideoCancellationResult(
                    provider_task_id="provider-task-download",
                    outcome=VideoCancellationOutcome.CANCELED,
                )
            ),
        )
        request = VideoGenerationRequest(
            prompt="镜头提示词",
            reference_images=[],
            reference_image_names=[],
            reference_audios=[],
            reference_audio_names=[],
            model="doubao-seedance-2-0-260128",
            job_id="job-download-cancel",
            metadata={"provider_task_id": "provider-task-download"},
            cancel_requested=AsyncMock(return_value=True),
        )

        with pytest.raises(VideoGenerationCancelled):
            await provider.generate(request)

        download.assert_not_awaited()
        provider.cancel.assert_awaited_once_with("provider-task-download")

    asyncio.run(scenario())


def test_volcengine_cancels_between_submit_and_first_poll(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoGenerationCancelled,
        )
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        calls: dict[str, list[str]] = {"get": [], "delete": []}

        class Tasks:
            async def create(self, **_kwargs):
                return SimpleNamespace(id="provider-task-race")

            async def get(self, *, task_id: str):
                calls["get"].append(task_id)
                raise AssertionError("本地已取消时不得进入首次轮询")

            async def delete(self, *, task_id: str) -> None:
                calls["delete"].append(task_id)

        class Client:
            def __init__(self) -> None:
                self.content_generation = SimpleNamespace(tasks=Tasks())
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        client = Client()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_build_client", lambda: client)
        cancel_requested = AsyncMock(side_effect=[False, True])

        with pytest.raises(VideoGenerationCancelled) as caught:
            await provider._submit_and_poll(
                "doubao-seedance-2-0-260128",
                "镜头提示词",
                [],
                [],
                None,
                lambda *_args: None,
                job_id="job-race",
                cancel_requested=cancel_requested,
            )

        assert cancel_requested.await_count == 2
        assert caught.value.result.outcome is VideoCancellationOutcome.CANCELED
        assert caught.value.result.provider_task_id == "provider-task-race"
        assert calls == {"get": [], "delete": ["provider-task-race"]}
        assert client.closed is True

    asyncio.run(scenario())


def test_volcengine_does_not_submit_when_cancellation_is_already_visible(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoGenerationCancelled,
        )
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        calls = {"create": 0, "delete": 0}

        class Tasks:
            async def create(self, **_kwargs):
                calls["create"] += 1
                return SimpleNamespace(id="must-not-exist")

            async def delete(self, *, task_id: str) -> None:
                calls["delete"] += 1

        class Client:
            def __init__(self) -> None:
                self.content_generation = SimpleNamespace(tasks=Tasks())
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        client = Client()
        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_build_client", lambda: client)

        with pytest.raises(VideoGenerationCancelled) as caught:
            await provider._submit_and_poll(
                "doubao-seedance-2-0-260128",
                "镜头提示词",
                [],
                [],
                None,
                lambda *_args: None,
                job_id="job-canceled-before-submit",
                cancel_requested=AsyncMock(return_value=True),
            )

        assert caught.value.result.outcome is VideoCancellationOutcome.CANCELED
        assert caught.value.result.provider_task_id == ""
        assert calls == {"create": 0, "delete": 0}
        assert client.closed is True

    asyncio.run(scenario())


def test_other_video_providers_declare_remote_cancel_as_unsupported() -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoCancellationOutcome
        from app.providers.video_gen_bailian import BailianVideoGenProvider
        from app.providers.video_gen_google import GoogleVideoGenProvider
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        providers = [
            BailianVideoGenProvider(),
            OpenAIVideoGenProvider(),
            GoogleVideoGenProvider(),
        ]
        for provider in providers:
            result = await provider.cancel("provider-task-unsupported")
            assert result.provider_task_id == "provider-task-unsupported"
            assert result.outcome is VideoCancellationOutcome.UNSUPPORTED
            assert result.detail

    asyncio.run(scenario())


def test_cancel_video_job_best_effort_cancels_known_provider_task(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
        )
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-provider-cancel",
            reservation_id="reservation-provider-cancel",
        )
        job.output_json = {"provider_task_id": "provider-task-known"}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        cancel = AsyncMock(
            return_value=VideoCancellationResult(
                provider_task_id="provider-task-known",
                outcome=VideoCancellationOutcome.CANCELED,
            )
        )
        provider = SimpleNamespace(cancel=cancel)
        monkeypatch.setattr(service, "get_video_provider", lambda _key: provider, raising=False)
        monkeypatch.setattr(service, "_credits_reserve", object())

        result = await service.cancel_video_job(
            db,
            script_id=seeded.script.id,
            job_id=job.job_id,
            actor_user_id=seeded.user.user_id,
        )

        assert result.status == "canceled"
        cancel.assert_awaited_once_with("provider-task-known")
        cancellation = (result.output_json or {})["provider_cancellation"]
        assert cancellation["outcome"] == "canceled"
        assert cancellation["retryable"] is False

    asyncio.run(scenario())


def test_cancel_neolink_job_before_remote_submission_is_allowed(monkeypatch) -> None:
    async def scenario() -> None:
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-neolink-cancel-before-submit",
            reservation_id="reservation-neolink-cancel-before-submit",
        )
        job.provider_key = "neolink"
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        release = AsyncMock()
        cancel_outbox = AsyncMock()
        monkeypatch.setattr(service, "_credits_reserve", object())
        monkeypatch.setattr("app.services.credits.release", release)
        monkeypatch.setattr(service, "cancel_outbox_task", cancel_outbox)

        result = await service.cancel_video_job(
            db,
            script_id=seeded.script.id,
            job_id=job.job_id,
            actor_user_id=seeded.user.user_id,
        )

        assert result.status == "canceled"
        release.assert_awaited_once()
        cancel_outbox.assert_awaited_once_with(
            db,
            task_type="video.generate",
            job_id=job.job_id,
            attempt=job.attempt,
        )

    asyncio.run(scenario())


def test_cancel_neolink_job_after_remote_submission_has_no_side_effects(monkeypatch) -> None:
    async def scenario() -> None:
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-neolink-cancel-after-submit",
            reservation_id="reservation-neolink-cancel-after-submit",
        )
        job.provider_key = "neolink"
        job.output_json = {"provider_task_id": "neolink-task-known"}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        release = AsyncMock(side_effect=AssertionError("不得释放冻结积分"))
        cancel_outbox = AsyncMock(side_effect=AssertionError("不得取消 Outbox"))
        remote_cancel = AsyncMock(side_effect=AssertionError("不得尝试不存在的远端取消接口"))
        monkeypatch.setattr(service, "_credits_reserve", object())
        monkeypatch.setattr("app.services.credits.release", release)
        monkeypatch.setattr(service, "cancel_outbox_task", cancel_outbox)
        monkeypatch.setattr(service, "get_video_provider", remote_cancel)

        with pytest.raises(ConflictError, match="NeoLink.*不支持取消"):
            await service.cancel_video_job(
                db,
                script_id=seeded.script.id,
                job_id=job.job_id,
                actor_user_id=seeded.user.user_id,
            )

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert saved.status == "starting"
        assert saved.output_json == {"provider_task_id": "neolink-task-known"}
        assert raw.get(CreditReservation, job.reservation_id).status == "reserved"
        release.assert_not_awaited()
        cancel_outbox.assert_not_awaited()
        remote_cancel.assert_not_awaited()

    asyncio.run(scenario())


def test_cancel_video_job_persists_failed_remote_cancellation_debt(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
        )
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-provider-cancel-failed",
            reservation_id="reservation-provider-cancel-failed",
        )
        job.output_json = {"provider_task_id": "provider-task-direct-failed"}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        cancel = AsyncMock(
            return_value=VideoCancellationResult(
                provider_task_id="provider-task-direct-failed",
                outcome=VideoCancellationOutcome.FAILED,
                detail="Bearer direct-secret " + "z" * 1000,
            )
        )
        monkeypatch.setattr(
            service,
            "get_video_provider",
            lambda _key: SimpleNamespace(cancel=cancel),
            raising=False,
        )
        monkeypatch.setattr(service, "_credits_reserve", object())
        release = AsyncMock()
        cancel_outbox = AsyncMock()
        monkeypatch.setattr("app.services.credits.release", release)
        monkeypatch.setattr(service, "cancel_outbox_task", cancel_outbox)

        result = await service.cancel_video_job(
            db,
            script_id=seeded.script.id,
            job_id=job.job_id,
            actor_user_id=seeded.user.user_id,
        )

        debt = (result.output_json or {})["provider_cancellation"]
        assert result.status == "canceled"
        assert result.retryable is False
        assert debt["provider_task_id"] == "provider-task-direct-failed"
        assert debt["outcome"] == "failed"
        assert debt["retryable"] is True
        assert debt["attempts"] == 1
        assert "direct-secret" not in debt["detail"]
        assert len(debt["detail"]) <= 512
        release.assert_awaited_once()
        cancel_outbox.assert_awaited_once_with(
            db,
            task_type="video.generate",
            job_id=job.job_id,
            attempt=job.attempt,
        )

    asyncio.run(scenario())


def test_cancel_video_job_commits_pending_debt_before_remote_call(monkeypatch) -> None:
    async def scenario() -> None:
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-provider-cancel-interrupted",
            reservation_id="reservation-provider-cancel-interrupted",
        )
        job.output_json = {"provider_task_id": "provider-task-interrupted"}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(service, "_credits_reserve", object())
        monkeypatch.setattr(
            service,
            "_best_effort_cancel_remote_video_task",
            AsyncMock(side_effect=asyncio.CancelledError),
        )

        with pytest.raises(asyncio.CancelledError):
            await service.cancel_video_job(
                db,
                script_id=seeded.script.id,
                job_id=job.job_id,
                actor_user_id=seeded.user.user_id,
            )

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        debt = (saved.output_json or {})["provider_cancellation"]
        assert saved.status == "canceled"
        assert debt == {
            "provider_task_id": "provider-task-interrupted",
            "outcome": "pending",
            "detail": None,
            "retryable": True,
            "attempts": 0,
        }

    asyncio.run(scenario())


def test_worker_treats_local_cancellation_as_terminal_without_artifacts(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
            VideoGenerationCancelled,
        )
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-worker-cancel",
            reservation_id="reservation-worker-cancel",
        )
        db = _AsyncSessionAdapter(raw)
        probe_calls = 0
        failed = AsyncMock()
        settled = AsyncMock()
        extract_last_frame = AsyncMock()

        async def canceled(job_id: str) -> bool:
            nonlocal probe_calls
            probe_calls += 1
            assert job_id == job.job_id
            saved = raw.get(VideoGenerationJob, job_id)
            saved.status = "canceled"
            saved.progress_stage = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            raw.commit()
            return True

        class Provider:
            closed = False

            async def generate(self, request, _on_progress=None):
                try:
                    assert request.cancel_requested is not None
                    if await request.cancel_requested():
                        raise VideoGenerationCancelled(
                            VideoCancellationResult(
                                provider_task_id="provider-task-race",
                                outcome=VideoCancellationOutcome.CANCELED,
                            )
                        )
                    raise AssertionError("取消探针返回 true 后不得生成产物")
                finally:
                    self.closed = True

        provider = Provider()
        monkeypatch.setattr(worker, "_is_video_job_canceled", canceled, raising=False)
        monkeypatch.setattr(worker, "_mark_failed_and_release_once", failed)
        monkeypatch.setattr(worker, "_credits_settle", settled)

        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=provider,
            extract_last_frame=extract_last_frame,
        )

        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved.status == "canceled"
        assert not (saved.output_json or {}).get("video_storage_key")
        cancellation = (saved.output_json or {})["provider_cancellation"]
        assert (saved.output_json or {})["provider_task_id"] == "provider-task-race"
        assert cancellation == {
            "provider_task_id": "provider-task-race",
            "outcome": "canceled",
            "detail": None,
            "retryable": False,
            "attempts": 1,
        }
        assert probe_calls == 1
        assert provider.closed is True
        failed.assert_not_awaited()
        settled.assert_not_awaited()
        extract_last_frame.assert_not_awaited()

    asyncio.run(scenario())


def test_worker_persists_sanitized_retryable_remote_cancellation_debt(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
            VideoGenerationCancelled,
        )
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-worker-cancel-failed",
            reservation_id="reservation-worker-cancel-failed",
        )
        db = _AsyncSessionAdapter(raw)

        class Provider:
            async def generate(self, request, _on_progress=None):
                saved = raw.get(VideoGenerationJob, request.job_id)
                saved.status = "canceled"
                saved.progress_stage = "canceled"
                saved.lease_owner = None
                saved.lease_token = None
                saved.lease_until = None
                raw.commit()
                raise VideoGenerationCancelled(
                    VideoCancellationResult(
                        provider_task_id="provider-task-debt",
                        outcome=VideoCancellationOutcome.FAILED,
                        detail=(
                            "Bearer secret-token https://private.example/video?token=secret "
                            + "x" * 1000
                        ),
                    )
                )

        release = AsyncMock()
        settle = AsyncMock()
        monkeypatch.setattr(worker, "_credits_release", release)
        monkeypatch.setattr(worker, "_credits_settle", settle)

        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

        saved = raw.get(VideoGenerationJob, job.job_id)
        debt = (saved.output_json or {})["provider_cancellation"]
        assert saved.status == "canceled"
        assert saved.retryable is False
        assert debt["outcome"] == "failed"
        assert debt["retryable"] is True
        assert debt["attempts"] == 1
        assert len(debt["detail"]) <= 512
        assert "secret-token" not in debt["detail"]
        assert "private.example" not in debt["detail"]
        assert "token=secret" not in debt["detail"]
        release.assert_not_awaited()
        settle.assert_not_awaited()

    asyncio.run(scenario())


def test_remote_cancellation_reconcile_clears_debt_and_does_not_repeat_delete(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
        )
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-cancel-debt-success",
            reservation_id="reservation-cancel-debt-success",
        )
        job.status = "canceled"
        job.output_json = {
            "provider_task_id": "provider-task-debt-success",
            "provider_cancellation": {
                "provider_task_id": "provider-task-debt-success",
                "outcome": "failed",
                "detail": "TimeoutError",
                "retryable": True,
                "attempts": 1,
            },
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        cancel = AsyncMock(
            return_value=VideoCancellationResult(
                provider_task_id="provider-task-debt-success",
                outcome=VideoCancellationOutcome.CANCELED,
            )
        )
        monkeypatch.setattr(
            worker,
            "get_video_provider",
            lambda _provider_key: SimpleNamespace(cancel=cancel),
            raising=False,
        )
        release = AsyncMock()
        monkeypatch.setattr(worker, "_credits_release", release)

        assert await worker.reconcile_video_remote_cancellations(db) == 1
        assert await worker.reconcile_video_remote_cancellations(db) == 0

        saved = raw.get(VideoGenerationJob, job.job_id)
        debt = (saved.output_json or {})["provider_cancellation"]
        assert saved.status == "canceled"
        assert saved.retryable is False
        assert debt["outcome"] == "canceled"
        assert debt["retryable"] is False
        assert debt["attempts"] == 2
        cancel.assert_awaited_once_with("provider-task-debt-success")
        release.assert_not_awaited()

    asyncio.run(scenario())


def test_remote_cancellation_reconcile_uses_bounded_default_batch() -> None:
    import inspect

    from app.workers import video_generation as worker

    reconcile_limit = inspect.signature(
        worker.reconcile_video_remote_cancellations
    ).parameters["limit"]
    task_limit = inspect.signature(
        worker.reconcile_video_remote_cancellations_task.run
    ).parameters["limit"]

    assert reconcile_limit.default == 10
    assert task_limit.default == 10


def test_remote_cancellation_reconcile_locks_and_commits_one_debt_at_a_time(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
        )
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        first = _add_worker_job(
            raw,
            seeded,
            job_id="job-cancel-debt-batch-1",
            reservation_id="reservation-cancel-debt-batch-1",
            unit_id="group:sg-10",
        )
        second = _add_worker_job(
            raw,
            seeded,
            job_id="job-cancel-debt-batch-2",
            reservation_id="reservation-cancel-debt-batch-2",
            unit_id="group:sg-2",
        )
        for index, job in enumerate((first, second), start=1):
            task_id = f"provider-task-batch-{index}"
            job.status = "canceled"
            job.output_json = {
                "provider_task_id": task_id,
                "provider_cancellation": {
                    "provider_task_id": task_id,
                    "outcome": "failed",
                    "detail": "TimeoutError",
                    "retryable": True,
                    "attempts": 1,
                },
            }
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def cancel(task_id: str) -> VideoCancellationResult:
            return VideoCancellationResult(
                provider_task_id=task_id,
                outcome=VideoCancellationOutcome.CANCELED,
            )

        monkeypatch.setattr(
            worker,
            "get_video_provider",
            lambda _provider_key: SimpleNamespace(cancel=AsyncMock(side_effect=cancel)),
            raising=False,
        )

        assert await worker.reconcile_video_remote_cancellations(db) == 2
        assert db.commit_count == 2

        select_sql = [
            " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
            for statement in db.executed_statements
            if getattr(statement, "is_select", False)
        ]
        assert "FOR UPDATE" not in select_sql[0]
        locked_sql = [
            sql
            for sql in select_sql[1:]
            if "FOR UPDATE SKIP LOCKED" in sql
            and "video_generation_jobs.status =" in sql
        ]
        assert len(locked_sql) == 2
        update_sql = [
            " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
            for statement in db.executed_statements
            if getattr(statement, "is_update", False)
        ]
        versioned_updates = [
            sql
            for sql in update_sql
            if "UPDATE video_generation_jobs" in sql
            and "video_generation_jobs.version =" in sql
        ]
        assert len(versioned_updates) == 2

    asyncio.run(scenario())


def test_remote_cancellation_reconcile_keeps_failed_debt_and_increments_attempts(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
        )
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-cancel-debt-failed",
            reservation_id="reservation-cancel-debt-failed",
        )
        job.status = "canceled"
        job.output_json = {
            "provider_task_id": "provider-task-debt-failed",
            "provider_cancellation": {
                "provider_task_id": "provider-task-debt-failed",
                "outcome": "failed",
                "detail": "TimeoutError",
                "retryable": True,
                "attempts": 2,
            },
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        cancel = AsyncMock(
            return_value=VideoCancellationResult(
                provider_task_id="provider-task-debt-failed",
                outcome=VideoCancellationOutcome.FAILED,
                detail="Bearer another-secret " + "y" * 1000,
            )
        )
        monkeypatch.setattr(
            worker,
            "get_video_provider",
            lambda _provider_key: SimpleNamespace(cancel=cancel),
            raising=False,
        )

        assert await worker.reconcile_video_remote_cancellations(db) == 0

        saved = raw.get(VideoGenerationJob, job.job_id)
        debt = (saved.output_json or {})["provider_cancellation"]
        assert saved.status == "canceled"
        assert debt["outcome"] == "failed"
        assert debt["retryable"] is True
        assert debt["attempts"] == 3
        assert "another-secret" not in debt["detail"]
        assert len(debt["detail"]) <= 512

    asyncio.run(scenario())


@pytest.mark.parametrize("debt_outcome", ["pending", "failed"])
def test_remote_cancellation_reconcile_clears_debt_on_real_volcengine_not_found(
    monkeypatch,
    debt_outcome: str,
) -> None:
    async def scenario() -> None:
        import httpx
        from volcenginesdkarkruntime._exceptions import ArkNotFoundError

        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id=f"job-real-404-{debt_outcome}",
            reservation_id=f"reservation-real-404-{debt_outcome}",
        )
        provider_task_id = f"provider-task-real-404-{debt_outcome}"
        job.status = "canceled"
        job.output_json = {
            "provider_task_id": provider_task_id,
            "provider_cancellation": {
                "provider_task_id": provider_task_id,
                "outcome": debt_outcome,
                "detail": None,
                "retryable": True,
                "attempts": 0 if debt_outcome == "pending" else 1,
            },
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        delete_calls: list[str] = []

        request = httpx.Request(
            "DELETE",
            "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks/task",
        )
        response = httpx.Response(404, request=request)
        not_found = ArkNotFoundError(
            "opaque sdk error",
            response=response,
            body={"error": {"code": "opaque"}},
            request_id="request-id",
        )

        class Tasks:
            async def delete(self, *, task_id: str) -> None:
                delete_calls.append(task_id)
                raise not_found

        class Client:
            content_generation = SimpleNamespace(tasks=Tasks())

            async def close(self) -> None:
                return None

        provider = VolcengineVideoGenProvider(provider_key="modelsai")
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: Client())
        monkeypatch.setattr(
            worker,
            "get_video_provider",
            lambda _provider_key: provider,
            raising=False,
        )

        assert await worker.reconcile_video_remote_cancellations(db) == 1
        assert await worker.reconcile_video_remote_cancellations(db) == 0

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        debt = (saved.output_json or {})["provider_cancellation"]
        assert saved.status == "canceled"
        assert saved.retryable is False
        assert debt["outcome"] == "canceled"
        assert debt["retryable"] is False
        assert delete_calls == [provider_task_id]

    asyncio.run(scenario())


@pytest.mark.parametrize("confirmed_outcome", ["canceled", "unsupported"])
def test_confirmed_remote_cancellation_is_not_downgraded_by_late_failure(
    confirmed_outcome: str,
) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import (
            VideoCancellationOutcome,
            VideoCancellationResult,
        )
        from app.services.video_remote_cancellation import (
            persist_provider_cancellation_result,
        )

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id=f"job-confirmed-{confirmed_outcome}",
            reservation_id=f"reservation-confirmed-{confirmed_outcome}",
        )
        job.status = "canceled"
        job.output_json = {
            "provider_task_id": "provider-task-confirmed",
            "provider_cancellation": {
                "provider_task_id": "provider-task-confirmed",
                "outcome": confirmed_outcome,
                "detail": None,
                "retryable": False,
                "attempts": 1,
            },
        }
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        assert await persist_provider_cancellation_result(
            db,
            job.job_id,
            VideoCancellationResult(
                provider_task_id="provider-task-confirmed",
                outcome=VideoCancellationOutcome.FAILED,
                detail="late_duplicate_delete_failed",
            ),
        )
        await db.commit()

        saved = raw.get(VideoGenerationJob, job.job_id)
        record = (saved.output_json or {})["provider_cancellation"]
        assert record["outcome"] == confirmed_outcome
        assert record["retryable"] is False
        assert record["attempts"] == 2

    asyncio.run(scenario())
