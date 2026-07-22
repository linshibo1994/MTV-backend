import asyncio
import io
import os
import secrets
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from PIL import Image
from pydantic import ValidationError
from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    UniqueConstraint,
    create_engine,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.v1 import generation_jobs as generation_api
from app.core.errors import BadRequestError, ConflictError, ForbiddenError, NotFoundError
from app.crud import generation as generation_crud
from app.models.asset_entry import AssetEntry
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.generation_batch_request import GenerationBatchRequest
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_storage_job import MediaStorageJob
from app.models.pipeline_stage import PipelineStage
from app.models.script import Script
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.providers.image_gen import ImageResult, image_result_from_url
from app.schemas.generation import BatchCreateJobIn, CreateJobIn
from app.services import generation as generation_service
from app.services.pipeline.enrich import build_base_prompt
from app.workers import generation as generation_worker


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

    async def execute(self, statement):
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    def add(self, row) -> None:
        self.session.add(row)

    def begin_nested(self):
        return _AsyncNested(self.session.begin_nested())


def _png(color: tuple[int, int, int] = (255, 0, 0)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", (4, 4), color).save(out, format="PNG")
    return out.getvalue()


def _webp() -> bytes:
    out = io.BytesIO()
    Image.new("RGB", (4, 4), (0, 255, 0)).save(out, format="WEBP")
    return out.getvalue()


def _twelve_sections() -> str:
    names = [
        "形制描述",
        "材质钉死",
        "使用痕迹",
        "摆放姿态",
        "反悬浮锁",
        "时代锚定",
        "画面构图",
        "画风统一锁",
        "三点柔光箱白底布光",
        "反水印锁",
        "文字渲染降级方案",
        "写实质感锚定",
    ]
    return "\n".join(f"【{name}】内容 {index}" for index, name in enumerate(names, 1))


def _numbered_twelve_sections() -> str:
    return "\n".join(
        f"{index}. {line}" for index, line in enumerate(_twelve_sections().splitlines(), 1)
    )


def _make_db() -> tuple[object, Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        User.__table__,
        Script.__table__,
        Materials.__table__,
        Subject.__table__,
        Episode.__table__,
        PipelineStage.__table__,
        GenerationBatchRequest.__table__,
        CreditAccount.__table__,
        CreditReservation.__table__,
        CreditLog.__table__,
        GenerationJob.__table__,
        TaskDispatchOutbox.__table__,
        MediaStorageJob.__table__,
        AssetEntry.__table__,
    ):
        table.create(engine)
    raw = Session(engine, expire_on_commit=False)
    return engine, raw


def _seed(raw: Session, *, subject_type: str = "character") -> SimpleNamespace:
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
    material = Materials(id="material-1", name="画布", script_id=script.id)
    episode = Episode(
        episode_id="episode-1",
        script_id=script.id,
        episode_no=1,
        title="第一集",
        episode_version=7,
    )
    subject = Subject(
        id="subject-1",
        script_id=script.id,
        type=subject_type,
        name="主角",
        description="原始主体描述",
        episode_nos=[1],
        review_status="not_designed",
    )
    enrich = PipelineStage(
        id="stage-enrich",
        script_id=script.id,
        stage="enrich",
        status="done",
        detail={
            "input_version": episode.episode_version,
            "prompts": {
                subject.id: {
                    "subject_version": 1,
                    "enrich_version": 1,
                    "resolved_prompts": {
                        "final": _twelve_sections(),
                    },
                }
            },
        },
    )
    account = CreditAccount(
        id="account-1",
        owner_type="user",
        owner_id=user.user_id,
        balance_points=1000,
        frozen_points=0,
        status="active",
    )
    raw.add_all([user, script, material, episode, subject, enrich, account])
    raw.commit()
    return SimpleNamespace(
        user=user,
        script=script,
        material=material,
        episode=episode,
        subject=subject,
        enrich=enrich,
        account=account,
    )


def _create_payload(**overrides) -> CreateJobIn:
    data = {
        "material_id": "material-1",
        "subject_id": "subject-1",
        "model": "banana-2",
        "size": "2k",
        "aspect": "16:9",
        "count": 2,
    }
    data.update(overrides)
    return CreateJobIn(**data)


@pytest.fixture(autouse=True)
def _no_real_permissions(monkeypatch):
    async def allow(*args, **kwargs):
        return None

    monkeypatch.setattr(generation_service, "check_script_access", allow)


def test_generation_job_model_has_reliable_task_fields_and_idempotency_constraint():
    column_names = {column.name for column in GenerationJob.__table__.columns}
    assert {
        "owner_user_id",
        "subject_id",
        "idempotency_key",
        "request_hash",
        "input_version",
        "attempt",
        "lease_owner",
        "lease_token",
        "lease_until",
        "heartbeat_at",
        "started_at",
        "finished_at",
        "error_type",
        "retryable",
        "canceled_at",
    } <= column_names
    for column_name in (
        "script_id",
        "subject_id",
        "owner_user_id",
        "idempotency_key",
        "request_hash",
        "attempt",
        "job_type",
    ):
        assert GenerationJob.__table__.c[column_name].nullable is False
    fks = {
        column.name: {fk.target_fullname for fk in column.foreign_keys}
        for column in GenerationJob.__table__.columns
        if column.foreign_keys
    }
    assert "materials.id" in fks["material_id"]
    assert "scripts.id" in fks["script_id"]
    assert "subjects.id" in fks["subject_id"]
    assert fks["owner_user_id"] == {"users.user_id"}
    assert fks["episode_id"] == {"episodes.episode_id"}
    constraints = [
        constraint
        for constraint in GenerationJob.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert any(
        {column.name for column in constraint.columns}
        == {"owner_user_id", "idempotency_key"}
        for constraint in constraints
    )
    check_names = {
        constraint.name
        for constraint in GenerationJob.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert {
        "ck_generation_jobs_status",
        "ck_generation_jobs_progress",
        "ck_generation_jobs_attempt",
        "ck_generation_jobs_job_type",
        "ck_generation_jobs_input_version_positive",
        "ck_generation_jobs_idempotency_key_not_blank",
    } <= check_names
    composite_fks = [
        {element.parent.name: element.column.table.name + "." + element.column.name for element in constraint.elements}
        for constraint in GenerationJob.__table__.constraints
        if isinstance(constraint, ForeignKeyConstraint) and len(constraint.elements) > 1
    ]
    assert {"script_id": "materials.script_id", "material_id": "materials.id"} in composite_fks
    assert {"script_id": "subjects.script_id", "subject_id": "subjects.id"} in composite_fks
    assert {"script_id": "episodes.script_id", "episode_id": "episodes.episode_id"} in composite_fks
    index_columns = {
        index.name: [column.name for column in index.columns]
        for index in GenerationJob.__table__.indexes
        if isinstance(index, Index)
    }
    assert index_columns["ix_generation_jobs_status_created"] == ["status", "created_at"]
    assert index_columns["ix_generation_jobs_material_created"] == ["material_id", "created_at"]


def test_generation_batch_request_model_records_exact_batch_fact():
    from app.models.generation_batch_request import GenerationBatchRequest

    column_names = {column.name for column in GenerationBatchRequest.__table__.columns}
    assert {
        "id",
        "owner_user_id",
        "idempotency_key",
        "request_hash",
        "target_subject_ids",
        "job_ids",
        "skipped_subject_ids",
        "failed_items",
        "status",
    } <= column_names
    constraints = [
        constraint
        for constraint in GenerationBatchRequest.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert any(
        {column.name for column in constraint.columns}
        == {"owner_user_id", "idempotency_key"}
        for constraint in constraints
    )


def test_subject_model_has_generation_input_version_fields():
    column_names = {column.name for column in Subject.__table__.columns}
    assert {"subject_version", "enrich_version"} <= column_names
    assert Subject.__table__.c["subject_version"].nullable is False
    assert Subject.__table__.c["enrich_version"].nullable is False
    index_columns = {
        index.name: [column.name for column in index.columns]
        for index in Subject.__table__.indexes
        if isinstance(index, Index)
    }
    assert index_columns["ix_subjects_script_review_type"] == [
        "script_id",
        "review_status",
        "type",
    ]


def test_generation_schema_accepts_optional_trimmed_prompt_with_length_limit():
    without_prompt = CreateJobIn(
        material_id="material-1",
        subject_id="subject-1",
    )
    assert without_prompt.prompt is None

    with_prompt = CreateJobIn(
        material_id="material-1",
        subject_id="subject-1",
        prompt="  客户端最终提示词\n",
    )
    assert with_prompt.prompt == "客户端最终提示词"
    assert CreateJobIn(
        material_id="material-1",
        subject_id="subject-1",
        prompt="图" * 32000,
    ).prompt == "图" * 32000

    for invalid_prompt in ("", " \n\t ", "图" * 32001):
        with pytest.raises(ValidationError):
            CreateJobIn(
                material_id="material-1",
                subject_id="subject-1",
                prompt=invalid_prompt,
            )


def test_generation_schema_rejects_client_resolved_prompt_forgery():
    with pytest.raises(ValidationError):
        CreateJobIn(
            material_id="material-1",
            subject_id="subject-1",
            prompt="client prompt",
            resolved_prompt="forged",
        )
    with pytest.raises(ValidationError):
        BatchCreateJobIn(
            material_id="material-1",
            resolved_prompt="forged",
        )


def test_generation_rejects_too_long_idempotency_key():
    with pytest.raises(BadRequestError):
        generation_service._normalize_idempotency_key("x" * 129)


@pytest.mark.parametrize(
    ("job_ids", "loaded_job_ids"),
    [
        pytest.param(["job-1"], [], id="empty-single-reload"),
        pytest.param(["job-1", "job-2"], ["job-1"], id="partial-batch-reload"),
    ],
)
def test_load_batch_jobs_rejects_incomplete_reload(job_ids, loaded_job_ids):
    async def scenario() -> None:
        loaded = [SimpleNamespace(job_id=job_id) for job_id in loaded_job_ids]

        async def execute(_statement):
            return SimpleNamespace(
                scalars=lambda: SimpleNamespace(all=lambda: loaded),
            )

        with pytest.raises(RuntimeError, match="generation job reload is incomplete"):
            await generation_service._load_batch_jobs(
                SimpleNamespace(execute=execute),
                job_ids,
            )

    asyncio.run(scenario())


def test_create_generation_job_uses_client_prompt_without_valid_enrich(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.enrich.detail = {
            "input_version": seeded.episode.episode_version,
            "prompts": {},
        }
        raw.commit()
        monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

        job = await generation_service.create_generation_job(
            _AsyncSessionAdapter(raw),
            user=seeded.user,
            payload=_create_payload(prompt="  客户端自定义生图提示词  "),
            idempotency_key="client-prompt-without-enrich",
        )

        assert job.input_json["prompt"] == "客户端自定义生图提示词"
        assert job.input_json["resolved_prompts"]["final"] == "客户端自定义生图提示词"

    asyncio.run(scenario())


def test_create_generation_job_same_idempotency_key_rejects_changed_client_prompt(
    monkeypatch,
):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

        first = await generation_service.create_generation_job(
            db,
            user=seeded.user,
            payload=_create_payload(prompt="第一版客户端提示词"),
            idempotency_key="client-prompt-idempotency",
        )
        replay = await generation_service.create_generation_job(
            db,
            user=seeded.user,
            payload=_create_payload(prompt="第一版客户端提示词"),
            idempotency_key="client-prompt-idempotency",
        )

        assert replay.job_id == first.job_id
        with pytest.raises(ConflictError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(prompt="第二版客户端提示词"),
                idempotency_key="client-prompt-idempotency",
            )

    asyncio.run(scenario())


def test_create_generation_job_requires_server_prompt_and_idempotent_outbox(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        enqueued: list[dict] = []

        async def fake_enqueue(db, **kwargs):
            enqueued.append(kwargs)
            return TaskDispatchOutbox(
                id="dispatch-1",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload=kwargs["payload"],
                status="pending",
            )

        monkeypatch.setattr(generation_service, "enqueue_task", fake_enqueue)
        job = await generation_service.create_generation_job(
            db,
            user=seeded.user,
            payload=_create_payload(),
            idempotency_key="key-1",
        )
        duplicate = await generation_service.create_generation_job(
            db,
            user=seeded.user,
            payload=_create_payload(),
            idempotency_key="key-1",
        )

        assert duplicate.job_id == job.job_id
        assert len(raw.scalars(select(GenerationJob)).all()) == 1
        assert len(enqueued) == 1
        assert enqueued[0]["task_type"] == "generation.generate_images"
        assert raw.scalars(select(TaskDispatchOutbox)).all() == []
        assert job.input_json["prompt"] == build_base_prompt(seeded.subject)
        assert job.input_json["resolved_prompts"]["final"] == _twelve_sections()
        assert job.input_json["input_version"] == seeded.episode.episode_version
        assert job.input_json["reservation_id"]
        assert raw.scalars(select(CreditReservation)).one().status == "reserved"

        with pytest.raises(ConflictError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(count=1),
                idempotency_key="key-1",
            )

    asyncio.run(scenario())


def test_enrich_prompt_requires_exact_ordered_unique_twelve_sections():
    assert generation_service._validate_twelve_section_prompt(_twelve_sections()) == _twelve_sections()
    assert (
        generation_service._validate_twelve_section_prompt(_numbered_twelve_sections())
        == _twelve_sections()
    )
    duplicate = _twelve_sections().replace("【写实质感锚定】", "【形制描述】")
    with pytest.raises(ConflictError):
        generation_service._validate_twelve_section_prompt(duplicate)
    shuffled = "\n".join(reversed(_twelve_sections().splitlines()))
    with pytest.raises(ConflictError):
        generation_service._validate_twelve_section_prompt(shuffled)
    missing = "\n".join(_twelve_sections().splitlines()[:-1])
    with pytest.raises(ConflictError):
        generation_service._validate_twelve_section_prompt(missing)
    empty_body = "\n".join(
        f"【{name}】  "
        for name in [
            "形制描述",
            "材质钉死",
            "使用痕迹",
            "摆放姿态",
            "反悬浮锁",
            "时代锚定",
            "画面构图",
            "画风统一锁",
            "三点柔光箱白底布光",
            "反水印锁",
            "文字渲染降级方案",
            "写实质感锚定",
        ]
    )
    with pytest.raises(ConflictError):
        generation_service._validate_twelve_section_prompt(empty_body)
    extra = f"{_twelve_sections()}\n【额外段落】不允许"
    with pytest.raises(ConflictError):
        generation_service._validate_twelve_section_prompt(extra)
    wrong_number = _numbered_twelve_sections().replace("2. 【材质钉死】", "3. 【材质钉死】")
    with pytest.raises(ConflictError):
        generation_service._validate_twelve_section_prompt(wrong_number)


def test_create_generation_job_accepts_and_normalizes_numbered_legacy_enrich_prompt(
    monkeypatch,
):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        stage = seeded.enrich
        stage.detail = {
            **stage.detail,
            "prompts": {
                seeded.subject.id: {
                    **stage.detail["prompts"][seeded.subject.id],
                    "resolved_prompts": {"final": _numbered_twelve_sections()},
                }
            },
        }
        raw.commit()
        monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

        job = await generation_service.create_generation_job(
            _AsyncSessionAdapter(raw),
            user=seeded.user,
            payload=_create_payload(),
            idempotency_key="numbered-legacy",
        )

        assert job.input_json["resolved_prompts"]["final"] == _twelve_sections()

    asyncio.run(scenario())


def test_batch_requires_idempotency_key_and_reuses_same_payload(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        enqueued: list[dict] = []

        async def fake_enqueue(db, **kwargs):
            enqueued.append(kwargs)
            return TaskDispatchOutbox(
                id=f"dispatch-{len(enqueued)}",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload=kwargs["payload"],
                status="pending",
            )

        monkeypatch.setattr(generation_service, "enqueue_task", fake_enqueue)
        payload = BatchCreateJobIn(
            material_id=seeded.material.id,
            subject_ids=[seeded.subject.id],
            count=1,
        )
        with pytest.raises(BadRequestError):
            await generation_service.create_batch_generation_jobs(
                db, user=seeded.user, payload=payload, idempotency_key=None
            )

        first, skipped, failed = await generation_service.create_batch_generation_jobs(
            db, user=seeded.user, payload=payload, idempotency_key="batch-key"
        )
        second, second_skipped, second_failed = await generation_service.create_batch_generation_jobs(
            db, user=seeded.user, payload=payload, idempotency_key="batch-key"
        )

        assert [job.job_id for job in second] == [job.job_id for job in first]
        assert skipped == second_skipped == []
        assert failed == second_failed == []
        assert len(raw.scalars(select(GenerationJob)).all()) == 1
        from app.models.generation_batch_request import GenerationBatchRequest

        batch_rows = raw.scalars(select(GenerationBatchRequest)).all()
        assert len(batch_rows) == 1
        assert batch_rows[0].target_subject_ids == [seeded.subject.id]
        assert len(raw.scalars(select(CreditReservation)).all()) == 1
        assert len(enqueued) == 1
        with pytest.raises(ConflictError):
            await generation_service.create_batch_generation_jobs(
                db,
                user=seeded.user,
                payload=BatchCreateJobIn(
                    material_id=seeded.material.id,
                    subject_ids=[seeded.subject.id],
                    count=2,
                ),
                idempotency_key="batch-key",
            )

    asyncio.run(scenario())


def test_batch_explicit_prompt_mapping_overrides_invalid_enrich(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        seeded.enrich.detail = {
            "input_version": seeded.episode.episode_version,
            "prompts": {},
        }
        raw.commit()
        monkeypatch.setattr(generation_service, "enqueue_task", AsyncMock())

        created, skipped, failed = await generation_service.create_batch_generation_jobs(
            _AsyncSessionAdapter(raw),
            user=seeded.user,
            payload=BatchCreateJobIn(
                material_id=seeded.material.id,
                subject_ids=[seeded.subject.id],
                prompts={seeded.subject.id: "  批量客户端自定义提示词  "},
                count=1,
            ),
            idempotency_key="batch-client-prompt",
        )

        assert skipped == []
        assert failed == []
        assert len(created) == 1
        assert created[0].input_json["prompt"] == "批量客户端自定义提示词"
        assert created[0].input_json["resolved_prompts"]["final"] == "批量客户端自定义提示词"

    asyncio.run(scenario())


def test_batch_rejects_prompt_mapping_for_non_target_subject():
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)

        with pytest.raises(BadRequestError, match="prompts 包含非目标主体"):
            await generation_service.create_batch_generation_jobs(
                _AsyncSessionAdapter(raw),
                user=seeded.user,
                payload=BatchCreateJobIn(
                    material_id=seeded.material.id,
                    subject_ids=[seeded.subject.id],
                    prompts={"subject-other": "不属于目标主体的提示词"},
                ),
                idempotency_key="batch-unknown-prompt-subject",
            )

        assert raw.scalars(select(GenerationJob)).all() == []

    asyncio.run(scenario())


def test_batch_idempotency_uses_exact_batch_fact_not_sql_like(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-similar-prefix",
                owner_user_id=seeded.user.user_id,
                material_id=seeded.material.id,
                script_id=seeded.script.id,
                subject_id=seeded.subject.id,
                idempotency_key="batch:literalX:subject-1",
                request_hash="different-hash",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={},
            )
        )
        raw.commit()

        async def fake_enqueue(db, **kwargs):
            return TaskDispatchOutbox(
                id="dispatch-literal-percent",
                task_type=kwargs["task_type"],
                job_id=kwargs["job_id"],
                attempt=kwargs["attempt"],
                payload=kwargs["payload"],
                status="pending",
            )

        monkeypatch.setattr(generation_service, "enqueue_task", fake_enqueue)
        created, skipped, failed = await generation_service.create_batch_generation_jobs(
            db,
            user=seeded.user,
            payload=BatchCreateJobIn(
                material_id=seeded.material.id,
                subject_ids=[seeded.subject.id],
                count=1,
            ),
            idempotency_key="batch:literal%",
        )
        assert len(created) == 1
        assert skipped == failed == []

    asyncio.run(scenario())


def test_batch_empty_target_replay_is_stable(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw, subject_type="sound")
        raw.get(Subject, seeded.subject.id).review_status = "approved"
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        payload = BatchCreateJobIn(material_id=seeded.material.id, subject_ids=None, count=1)

        first = await generation_service.create_batch_generation_jobs(
            db, user=seeded.user, payload=payload, idempotency_key="empty-batch"
        )
        second = await generation_service.create_batch_generation_jobs(
            db, user=seeded.user, payload=payload, idempotency_key="empty-batch"
        )

        assert first == second == ([], [], [])
        from app.models.generation_batch_request import GenerationBatchRequest

        assert len(raw.scalars(select(GenerationBatchRequest)).all()) == 1

    asyncio.run(scenario())


def test_batch_persists_manifest_for_all_skipped_or_failed_replay(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        raw.add(
            Subject(
                id="subject-sound",
                script_id=seeded.script.id,
                type="sound",
                name="音效",
                description="环境音",
                review_status="not_designed",
            )
        )
        raw.execute(
            update(PipelineStage)
            .where(PipelineStage.id == "stage-enrich")
            .values(
                detail={
                    "input_version": seeded.episode.episode_version,
                    "prompts": {
                        seeded.subject.id: {
                            "subject_version": seeded.subject.subject_version,
                            "enrich_version": seeded.subject.enrich_version,
                            "resolved_prompts": {"final": _twelve_sections()},
                        }
                    },
                }
            )
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        payload = BatchCreateJobIn(
            material_id=seeded.material.id,
            subject_ids=["subject-sound", seeded.subject.id],
            count=1,
        )

        async def fail_enqueue(*args, **kwargs):
            raise RuntimeError("outbox unavailable")

        monkeypatch.setattr(generation_service, "enqueue_task", fail_enqueue)
        first_created, first_skipped, first_failed = await generation_service.create_batch_generation_jobs(
            db, user=seeded.user, payload=payload, idempotency_key="batch-manifest"
        )
        second_created, second_skipped, second_failed = await generation_service.create_batch_generation_jobs(
            db, user=seeded.user, payload=payload, idempotency_key="batch-manifest"
        )

        assert first_created == second_created == []
        assert first_skipped == second_skipped == ["subject-sound"]
        assert first_failed == second_failed == [
            {
                "subject_id": seeded.subject.id,
                "subject_name": seeded.subject.name,
                "reason": "generation_create_failed",
            }
        ]
        batch = raw.scalars(select(GenerationBatchRequest)).one()
        assert batch.target_subject_ids == ["subject-sound", seeded.subject.id]
        assert batch.job_ids == []
        assert batch.skipped_subject_ids == ["subject-sound"]
        assert batch.failed_items == first_failed
        assert batch.status == "failed"
        assert raw.scalars(select(GenerationJob)).all() == []
        assert raw.scalars(select(CreditReservation)).all() == []
        assert raw.scalars(select(TaskDispatchOutbox)).all() == []

    asyncio.run(scenario())


def test_single_idempotency_unique_conflict_uses_savepoint_not_outer_rollback(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        seeded.user.username = "alice-renamed"

        original_find = generation_service._find_idempotent_job
        original_create_one_job = generation_service._create_one_job
        calls = 0

        async def racing_find(db, owner_user_id, idempotency_key):
            nonlocal calls
            calls += 1
            if calls == 1:
                return None
            return await original_find(db, owner_user_id, idempotency_key)

        async def create_conflicting_job(*args, **kwargs):
            existing = GenerationJob(
                job_id="job-raced",
                owner_user_id=seeded.user.user_id,
                material_id=seeded.material.id,
                script_id=seeded.script.id,
                subject_id=seeded.subject.id,
                idempotency_key=kwargs["idempotency_key"],
                request_hash=kwargs["request_hash"],
                input_version=kwargs["input_version"],
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={},
            )
            raw.add(existing)
            raw.flush()
            return await original_create_one_job(*args, **kwargs)

        monkeypatch.setattr(generation_service, "_find_idempotent_job", racing_find)
        monkeypatch.setattr(generation_service, "_create_one_job", create_conflicting_job)

        job = await generation_service.create_generation_job(
            db,
            user=seeded.user,
            payload=_create_payload(count=1),
            idempotency_key="race-key",
        )

        assert job.job_id == "job-raced"
        assert raw.get(User, seeded.user.user_id).username == "alice-renamed"

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过 expire_on_commit PostgreSQL 回归",
)
async def test_generation_returns_loaded_jobs_with_expire_on_commit(monkeypatch) -> None:
    database_url = os.environ["MTV_TEST_POSTGRES_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(database_url)
    connection = await engine.connect()
    transaction = await connection.begin()
    session_factory = async_sessionmaker(
        connection,
        expire_on_commit=True,
        join_transaction_mode="create_savepoint",
    )
    suffix = secrets.token_hex(4)
    user_id = f"user-expire-{suffix}"
    script_id = f"script-expire-{suffix}"
    material_id = f"material-expire-{suffix}"
    subject_id = f"subject-expire-{suffix}"
    raced_job_id = f"job-raced-{suffix}"
    user = SimpleNamespace(user_id=user_id)
    payload = CreateJobIn(
        material_id=material_id,
        subject_id=subject_id,
        count=1,
    )

    async def load_material_script(*args, **kwargs):
        return SimpleNamespace(id=material_id), SimpleNamespace(id=script_id)

    async def load_credit_account(*args, **kwargs):
        return SimpleNamespace(id=f"account-{suffix}")

    async def subject_snapshot(*args, **kwargs):
        return None, 1, None, {}

    async def resolved_prompt(*args, **kwargs):
        return _twelve_sections()

    async def reserve_without_persistence(*args, **kwargs):
        return f"reservation-{kwargs['source_id']}"

    monkeypatch.setattr(generation_service, "_load_material_script", load_material_script)
    monkeypatch.setattr(generation_service, "_load_credit_account", load_credit_account)
    monkeypatch.setattr(generation_service, "_subject_episode_snapshot", subject_snapshot)
    monkeypatch.setattr(generation_service, "_load_server_resolved_prompt", resolved_prompt)
    monkeypatch.setattr(generation_service, "reserve", reserve_without_persistence)

    try:
        async with session_factory() as db:
            db.add_all(
                [
                    User(
                        user_id=user_id,
                        username=f"expire-{suffix}",
                        password_hash="hash",
                        enabled=True,
                        account_type="personal",
                    ),
                    Script(
                        id=script_id,
                        title="expire_on_commit 回归",
                        scope="personal",
                        owner_user_id=user_id,
                        created_by_user_id=user_id,
                    ),
                    Materials(id=material_id, name="画布", script_id=script_id),
                    Subject(
                        id=subject_id,
                        script_id=script_id,
                        type="character",
                        name="主角",
                        description="原始主体描述",
                        episode_nos=[1],
                        review_status="not_designed",
                    ),
                ]
            )
            await db.commit()

            single = await generation_service.create_generation_job(
                db,
                user=user,
                payload=payload,
                idempotency_key=f"expire-single-{suffix}",
            )
            assert generation_api._to_out(single).status == "starting"

            created, skipped, failed = await generation_service.create_batch_generation_jobs(
                db,
                user=user,
                payload=BatchCreateJobIn(
                    material_id=material_id,
                    subject_ids=[subject_id],
                    count=1,
                ),
                idempotency_key=f"expire-batch-{suffix}",
            )
            assert skipped == []
            assert failed == []
            assert len(created) == 1
            assert generation_api._to_out(created[0]).status == "starting"

            original_find = generation_service._find_idempotent_job
            original_create_one_job = generation_service._create_one_job
            find_calls = 0

            async def racing_find(db, owner_user_id, idempotency_key):
                nonlocal find_calls
                find_calls += 1
                if find_calls == 1:
                    return None
                return await original_find(db, owner_user_id, idempotency_key)

            async def create_conflicting_job(*args, **kwargs):
                db.add(
                    GenerationJob(
                        job_id=raced_job_id,
                        owner_user_id=user_id,
                        material_id=material_id,
                        script_id=script_id,
                        subject_id=subject_id,
                        idempotency_key=kwargs["idempotency_key"],
                        request_hash=kwargs["request_hash"],
                        input_version=kwargs["input_version"],
                        attempt=1,
                        job_type="generate_images",
                        status="starting",
                        progress=0,
                        input_json={},
                    )
                )
                await db.flush()
                return await original_create_one_job(*args, **kwargs)

            monkeypatch.setattr(generation_service, "_find_idempotent_job", racing_find)
            monkeypatch.setattr(generation_service, "_create_one_job", create_conflicting_job)
            raced = await generation_service.create_generation_job(
                db,
                user=user,
                payload=payload,
                idempotency_key=f"expire-race-{suffix}",
            )
            assert raced.job_id == raced_job_id
            assert generation_api._to_out(raced).status == "starting"
    finally:
        await transaction.rollback()
        await connection.close()
        await engine.dispose()


def test_batch_explicit_subject_ids_404_and_403_without_partial_create(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        payload = BatchCreateJobIn(
            material_id=seeded.material.id,
            subject_ids=[seeded.subject.id, "missing-subject"],
        )
        with pytest.raises(NotFoundError):
            await generation_service.create_batch_generation_jobs(
                db, user=seeded.user, payload=payload, idempotency_key="batch-missing"
            )
        assert raw.scalars(select(GenerationJob)).all() == []

        raw.add(
            Subject(
                id="subject-foreign",
                script_id="script-foreign",
                type="character",
                name="外部主体",
                description="外部描述",
                review_status="not_designed",
            )
        )
        raw.commit()
        with pytest.raises(ForbiddenError):
            await generation_service.create_batch_generation_jobs(
                db,
                user=seeded.user,
                payload=BatchCreateJobIn(
                    material_id=seeded.material.id,
                    subject_ids=[seeded.subject.id, "subject-foreign"],
                ),
                idempotency_key="batch-foreign",
            )
        assert raw.scalars(select(GenerationJob)).all() == []

    asyncio.run(scenario())


def test_create_generation_job_rejects_sound_and_missing_enrich_snapshot():
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw, subject_type="sound")
        db = _AsyncSessionAdapter(raw)
        with pytest.raises(BadRequestError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(),
                idempotency_key="key-sound",
            )
        seeded.subject.type = "character"
        raw.execute(
            update(PipelineStage)
            .where(PipelineStage.id == "stage-enrich")
            .values(detail={"input_version": 6, "prompts": {}})
        )
        raw.commit()
        with pytest.raises(ConflictError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(),
                idempotency_key="key-stale-enrich",
            )

    asyncio.run(scenario())


def test_create_generation_job_rejects_stale_subject_version_in_enrich_prompt():
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.execute(
            update(PipelineStage)
            .where(PipelineStage.id == "stage-enrich")
            .values(
                detail={
                    "input_version": seeded.episode.episode_version,
                    "prompts": {
                        seeded.subject.id: {
                            "subject_version": seeded.subject.subject_version - 1,
                            "enrich_version": seeded.subject.enrich_version,
                            "resolved_prompts": {"final": _twelve_sections()},
                        }
                    },
                }
            )
        )
        raw.commit()

        with pytest.raises(ConflictError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(),
                idempotency_key="key-stale-subject-version",
            )

    asyncio.run(scenario())


def test_generation_claim_heartbeat_finish_and_cancel_use_token_cas(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        now = datetime.now(UTC)
        raw.add_all(
            [
                GenerationJob(
                    job_id="job-pending",
                    owner_user_id="user-1",
                    material_id="material-1",
                    script_id="script-1",
                    subject_id="subject-1",
                    idempotency_key="pending",
                    request_hash="h1",
                    input_version=7,
                    attempt=1,
                    job_type="generate_images",
                    status="starting",
                    progress=0,
                    input_json={},
                ),
                GenerationJob(
                    job_id="job-expired",
                    owner_user_id="user-1",
                    material_id="material-1",
                    script_id="script-1",
                    subject_id="subject-1",
                    idempotency_key="expired",
                    request_hash="h2",
                    input_version=7,
                    attempt=1,
                    job_type="generate_images",
                    status="processing",
                    progress=10,
                    input_json={},
                    lease_owner="old",
                    lease_token="old-token",
                    lease_until=now - timedelta(seconds=1),
                ),
            ]
        )
        cancel_outbox = TaskDispatchOutbox(
            id="dispatch-generation-cancel",
            task_type="generation.generate_images",
            job_id="job-pending",
            attempt=1,
            payload={"job_id": "job-pending"},
            status="running",
            lease_owner="dispatcher",
            lease_token="dispatch-token",
            lease_until=now + timedelta(minutes=1),
        )
        wrong_attempt = TaskDispatchOutbox(
            id="dispatch-generation-wrong-attempt",
            task_type="generation.generate_images",
            job_id="job-pending",
            attempt=2,
            payload={"job_id": "job-pending"},
            status="retry",
        )
        wrong_task_type = TaskDispatchOutbox(
            id="dispatch-generation-wrong-type",
            task_type="timeline.render",
            job_id="job-pending",
            attempt=1,
            payload={"job_id": "job-pending"},
            status="pending",
        )
        raw.add_all([cancel_outbox, wrong_attempt, wrong_task_type])
        raw.commit()

        pending = await generation_worker.claim_generation_job(db, "job-pending", "worker-a")
        reclaimed = await generation_worker.claim_generation_job(db, "job-expired", "worker-b")
        assert pending is not None and reclaimed is not None
        assert reclaimed.lease_token != "old-token"
        assert not await generation_worker.heartbeat_generation_job(
            db, reclaimed.job_id, "old-token"
        )
        assert await generation_worker.heartbeat_generation_job(
            db, reclaimed.job_id, reclaimed.lease_token
        )
        assert not await generation_worker.finish_generation_job(
            db,
            reclaimed.job_id,
            "old-token",
            "succeeded",
            output_json={"imageOutputs": []},
            metrics={},
        )
        assert await generation_worker.cancel_generation_job(
            db, "job-pending", actor_user_id="user-1"
        )
        await db.commit()
        raw.expire_all()
        cancel_outbox = raw.get(TaskDispatchOutbox, "dispatch-generation-cancel")
        wrong_attempt = raw.get(TaskDispatchOutbox, "dispatch-generation-wrong-attempt")
        wrong_task_type = raw.get(TaskDispatchOutbox, "dispatch-generation-wrong-type")
        assert cancel_outbox is not None
        assert wrong_attempt is not None
        assert wrong_task_type is not None
        assert cancel_outbox.status == "canceled"
        assert cancel_outbox.lease_owner is None
        assert cancel_outbox.lease_token is None
        assert cancel_outbox.lease_until is None
        assert wrong_attempt.status == "retry"
        assert wrong_task_type.status == "pending"
        assert not await generation_worker.finish_generation_job(
            db,
            pending.job_id,
            pending.lease_token,
            "succeeded",
            output_json={"imageOutputs": []},
            metrics={},
        )
        assert await generation_worker.finish_generation_job(
            db,
            reclaimed.job_id,
            reclaimed.lease_token,
            "succeeded",
            output_json={"imageOutputs": []},
            metrics={},
        )
        with pytest.raises(ConflictError):
            await generation_worker.cancel_generation_job(
                db, reclaimed.job_id, actor_user_id="user-1"
            )

    asyncio.run(scenario())


def test_generation_progress_updates_are_monotonic():
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-progress",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-progress",
                request_hash="hash-progress",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="processing",
                progress=80,
                input_json={},
            )
        )
        raw.commit()

        await generation_crud.update_progress(
            db, "job-progress", progress=50, stage="storage"
        )
        await db.commit()
        assert raw.get(GenerationJob, "job-progress").progress == 80
        assert raw.get(GenerationJob, "job-progress").progress_stage == "storage"

        await generation_crud.update_progress(
            db, "job-progress", progress=90, stage="settlement"
        )
        await db.commit()
        assert raw.get(GenerationJob, "job-progress").progress == 90
        assert raw.get(GenerationJob, "job-progress").progress_stage == "settlement"

    asyncio.run(scenario())


def test_cancel_rejects_legacy_job_without_owner() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-no-owner",
                owner_user_id="",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-no-owner",
                request_hash="hash-no-owner",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={},
            )
        )
        raw.commit()
        with pytest.raises(ForbiddenError):
            await generation_worker.cancel_generation_job(
                db, "job-no-owner", actor_user_id="user-1"
            )

    asyncio.run(scenario())


def test_cancel_access_uses_run_ai_permission(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        job = GenerationJob(
            job_id="job-access",
            owner_user_id="user-1",
            material_id="material-1",
            script_id="script-1",
            subject_id="subject-1",
            idempotency_key="key-access",
            request_hash="hash-access",
            input_version=7,
            attempt=1,
            job_type="generate_images",
            status="starting",
            progress=0,
            input_json={},
        )
        raw.add(job)
        raw.commit()
        actions: list[object] = []

        async def fake_check(db, user, script, action):
            actions.append(action)

        monkeypatch.setattr(generation_api, "check_script_access", fake_check)
        await generation_api._verify_job_access(
            db, seeded.user, job, action=generation_api.Action.RUN_AI
        )
        assert actions == [generation_api.Action.RUN_AI]

    asyncio.run(scenario())


def test_generation_job_output_sanitizes_public_metrics_and_errors() -> None:
    job = GenerationJob(
        job_id="job-public",
        owner_user_id="user-1",
        material_id="material-1",
        script_id="script-1",
        subject_id="subject-1",
        idempotency_key="key-public",
        request_hash="hash-public",
        input_version=7,
        attempt=1,
        job_type="generate_images",
        status="failed",
        progress=0,
        error_message="provider_or_storage_error",
        input_json={
            "prompt": "公开 prompt",
            "reservation_id": "reservation-secret",
            "resolved_prompts": {
                "final": _twelve_sections(),
                "debug_token": "Authorization=secret-token /private/path",
                "extra": {"provider_response": "raw"},
            },
        },
        metrics={
            "traceback": "Traceback Authorization=secret-token /tmp/private",
            "actual_points": 10,
            "count": 1,
        },
    )

    out = generation_api._to_out(job)

    assert out.subject_id == "subject-1"
    assert out.input_json == {
        "prompt": "公开 prompt",
        "resolved_prompts": {"final": _twelve_sections()},
    }
    assert out.metrics == {"actual_points": 10, "count": 1}
    failed = GenerationJob(
        job_id="job-public-error",
        owner_user_id="user-1",
        material_id="material-1",
        script_id="script-1",
        subject_id="subject-1",
        idempotency_key="key-public-error",
        request_hash="hash-public-error",
        input_version=7,
        attempt=1,
        job_type="generate_images",
        status="failed",
        progress=0,
        error_type="provider_or_storage_error",
        error_message="Authorization=secret-token /private/provider/path",
    )
    assert generation_api._to_out(failed).error_message == "生成失败，请稍后重试"


def test_generation_create_batch_and_poll_share_public_subject_mapping(monkeypatch) -> None:
    async def scenario() -> None:
        def job(job_id: str, subject_id: str) -> GenerationJob:
            return GenerationJob(
                job_id=job_id,
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id=subject_id,
                idempotency_key=f"key-{job_id}",
                request_hash=f"hash-{job_id}",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": f"公开 prompt {subject_id}",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "subject_id": subject_id,
                    "reservation_id": f"reservation-{job_id}",
                },
            )

        single_job = job("job-single-public", "subject-single")
        batch_job = job("job-batch-public", "subject-batch")

        async def create_one(*args, **kwargs):
            return single_job

        async def create_batch(*args, **kwargs):
            return [batch_job], [], []

        async def get_one(*args, **kwargs):
            return single_job

        async def verify_access(*args, **kwargs) -> None:
            return None

        monkeypatch.setattr(generation_api, "create_generation_job", create_one)
        monkeypatch.setattr(generation_api, "create_batch_generation_jobs", create_batch)
        monkeypatch.setattr(generation_api, "get_job", get_one)
        monkeypatch.setattr(generation_api, "_verify_job_access", verify_access)

        actor = SimpleNamespace(user_id="user-1")
        db = SimpleNamespace()
        created = await generation_api.create_job_endpoint(
            CreateJobIn(material_id="material-1", subject_id="subject-single"),
            "single-key",
            db=db,
            user=actor,
        )
        batch = await generation_api.create_batch_jobs_endpoint(
            BatchCreateJobIn(material_id="material-1", subject_ids=["subject-batch"]),
            "batch-key",
            db=db,
            user=actor,
        )
        polled = await generation_api.get_job_endpoint(
            "job-single-public",
            db=db,
            user=actor,
        )

        assert created.subject_id == "subject-single"
        assert batch.created[0].subject_id == "subject-batch"
        assert polled.subject_id == "subject-single"
        for output in (created, batch.created[0], polled):
            assert output.input_json is not None
            assert "subject_id" not in output.input_json
            assert "reservation_id" not in output.input_json

    asyncio.run(scenario())


def test_provider_result_validation_accepts_partial_and_rejects_invalid_counts_and_ssrf_url():
    assert generation_worker.validate_image_result(
        ImageResult(data=_png(), mime_type="image/png")
    ).mime_type == "image/png"
    assert generation_worker.validate_image_result(
        ImageResult(data=_webp(), mime_type="image/png")
    ).mime_type == "image/webp"
    with pytest.raises(ValueError):
        generation_worker.validate_image_result(ImageResult(data=b"not-image", mime_type="image/png"))
    generation_worker.ensure_provider_result_count(
        [ImageResult(data=_png(), mime_type="image/png")],
        expected=2,
    )
    with pytest.raises(ValueError, match="未返回图片"):
        generation_worker.ensure_provider_result_count([], expected=2)
    with pytest.raises(ValueError, match="超过请求数"):
        generation_worker.ensure_provider_result_count(
            [
                ImageResult(data=_png(), mime_type="image/png"),
                ImageResult(data=_png(), mime_type="image/png"),
            ],
            expected=1,
        )
    with pytest.raises(RuntimeError):
        asyncio.run(image_result_from_url("http://127.0.0.1/private.png"))


@pytest.mark.parametrize(("provider_count", "expected_actual"), [(2, 20), (1, 10)])
def test_run_generation_persists_candidates_settles_actual_and_guards_old_version(
    monkeypatch, provider_count, expected_actual
):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        job = GenerationJob(
            job_id="job-1",
            owner_user_id="user-1",
            material_id="material-1",
            script_id="script-1",
            subject_id="subject-1",
            idempotency_key="key-1",
            request_hash="hash",
            input_version=7,
            attempt=1,
            job_type="generate_images",
            status="starting",
            progress=0,
            input_json={
                "prompt": "原始主体描述",
                "resolved_prompts": {"final": _twelve_sections()},
                "model": "banana-2",
                "size": "2k",
                "aspect": "16:9",
                "count": 2,
                "subject_id": "subject-1",
                "input_version": 7,
                "reservation_id": "reservation-1",
            },
        )
        raw.add(job)
        raw.add(
            CreditReservation(
                id="reservation-1",
                account_id="account-1",
                source_type="generation_job",
                source_id=job.job_id,
                reserved_points=20,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 20
        raw.commit()

        class Provider:
            async def generate(self, **kwargs):
                return [
                    ImageResult(data=_png(), mime_type="image/png"),
                    ImageResult(data=_png((0, 0, 255)), mime_type="image/png"),
                ][:provider_count]

        stored: list[tuple[str, str]] = []

        class Storage:
            async def put(self, key, data):
                stored.append((key, data[:8].hex()))

        monkeypatch.setattr(generation_worker.settings, "price_image_gen", 10)
        await generation_worker.run_generation_once(
            db,
            "job-1",
            owner="worker-1",
            provider=Provider(),
            storage=Storage(),
        )
        finished = raw.get(GenerationJob, "job-1")
        assert finished.status == "succeeded"
        assert finished.metrics["actual_points"] == expected_actual
        assert finished.metrics["count"] == provider_count
        assert len(finished.output_json["imageOutputs"]) == provider_count
        assert {entry.mime_type for entry in raw.scalars(select(AssetEntry)).all()} == {"image/png"}
        assert raw.get(Subject, "subject-1").review_status == "pending"
        assert raw.get(CreditReservation, "reservation-1").status == "captured"
        assert raw.get(CreditReservation, "reservation-1").actual_points == expected_actual
        assert all(key.endswith(".png") for key, _ in stored)

        stale = GenerationJob(
            job_id="job-stale",
            owner_user_id="user-1",
            material_id="material-1",
            script_id="script-1",
            subject_id="subject-1",
            idempotency_key="key-stale",
            request_hash="hash-stale",
            input_version=6,
            attempt=1,
            job_type="generate_images",
            status="starting",
            progress=0,
            input_json={
                "prompt": "旧描述",
                "resolved_prompts": {"final": _twelve_sections()},
                "model": "banana-2",
                "size": "2k",
                "aspect": "16:9",
                "count": 1,
                "subject_id": "subject-1",
                "input_version": 6,
                "reservation_id": "reservation-stale",
            },
        )
        raw.add(stale)
        raw.add(
            CreditReservation(
                id="reservation-stale",
                account_id="account-1",
                source_type="generation_job",
                source_id=stale.job_id,
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points += 10
        raw.commit()
        before = len(raw.scalars(select(AssetEntry)).all())
        await generation_worker.run_generation_once(
            db,
            "job-stale",
            owner="worker-1",
            provider=Provider(),
            storage=Storage(),
        )
        assert len(raw.scalars(select(AssetEntry)).all()) == before
        assert raw.get(GenerationJob, "job-stale").status == "canceled"
        assert raw.get(CreditReservation, "reservation-stale").status == "released"

    asyncio.run(scenario())


def test_run_generation_stops_before_provider_when_heartbeat_fails(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-heartbeat-false",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-heartbeat-false",
                request_hash="hash-heartbeat-false",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": "原始主体描述",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 1,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "reservation_id": "reservation-heartbeat-false",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-heartbeat-false",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-heartbeat-false",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()
        provider_called = False

        class Provider:
            async def generate(self, **kwargs):
                nonlocal provider_called
                provider_called = True
                return [ImageResult(data=_png(), mime_type="image/png")]

        async def heartbeat_false(*args, **kwargs):
            return False

        monkeypatch.setattr(generation_worker, "heartbeat_generation_job", heartbeat_false)
        await generation_worker.run_generation_once(
            db,
            "job-heartbeat-false",
            owner="worker-1",
            provider=Provider(),
            storage=SimpleNamespace(),
        )

        assert provider_called is False
        assert raw.get(CreditReservation, "reservation-heartbeat-false").status == "reserved"
        assert raw.scalars(select(AssetEntry)).all() == []

    asyncio.run(scenario())


def test_run_generation_cancels_when_subject_version_changed_before_provider(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        subject = raw.get(Subject, "subject-1")
        subject.subject_version = 2
        raw.add(
            GenerationJob(
                job_id="job-subject-stale",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-subject-stale",
                request_hash="hash-subject-stale",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": "原始主体描述",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 1,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "subject_version": 1,
                    "enrich_version": 1,
                    "reservation_id": "reservation-subject-stale",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-subject-stale",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-subject-stale",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()
        provider_called = False

        class Provider:
            async def generate(self, **kwargs):
                nonlocal provider_called
                provider_called = True
                return [ImageResult(data=_png(), mime_type="image/png")]

        await generation_worker.run_generation_once(
            db,
            "job-subject-stale",
            owner="worker-1",
            provider=Provider(),
            storage=SimpleNamespace(),
        )

        assert provider_called is False
        assert raw.get(GenerationJob, "job-subject-stale").status == "canceled"
        assert raw.get(CreditReservation, "reservation-subject-stale").status == "released"
        assert raw.scalars(select(AssetEntry)).all() == []

    asyncio.run(scenario())


def test_run_generation_rolls_back_when_terminal_cas_fails(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-terminal-cas",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-terminal-cas",
                request_hash="hash-terminal-cas",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": "原始主体描述",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 1,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "reservation_id": "reservation-terminal-cas",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-terminal-cas",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-terminal-cas",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()

        class Provider:
            async def generate(self, **kwargs):
                return [ImageResult(data=_png(), mime_type="image/png")]

        class Storage:
            def __init__(self) -> None:
                self.puts: list[str] = []
                self.deletes: list[str] = []

            async def put(self, key, data):
                self.puts.append(key)

            async def delete(self, key):
                self.deletes.append(key)

        events: list[str] = []
        original_lock = generation_worker._lock_current_generation_job
        original_settle = generation_worker.credits_settle

        async def record_lock(*args, **kwargs):
            events.append("job_lock")
            return await original_lock(*args, **kwargs)

        async def record_settle(*args, **kwargs):
            events.append("settle")
            return await original_settle(*args, **kwargs)

        async def terminal_cas_false(*args, **kwargs):
            events.append("terminal_cas")
            return False

        storage = Storage()
        monkeypatch.setattr(
            generation_worker, "_lock_current_generation_job", record_lock
        )
        monkeypatch.setattr(generation_worker, "credits_settle", record_settle)
        monkeypatch.setattr(generation_worker, "finish_generation_job", terminal_cas_false)
        await generation_worker.run_generation_once(
            db,
            "job-terminal-cas",
            owner="worker-1",
            provider=Provider(),
            storage=storage,
        )

        assert raw.get(CreditReservation, "reservation-terminal-cas").status == "reserved"
        assert raw.scalars(select(AssetEntry)).all() == []
        assert storage.puts
        assert set(storage.puts) <= set(storage.deletes)
        assert events == ["job_lock", "settle", "terminal_cas"]

    asyncio.run(scenario())


def test_generation_failure_with_stale_lease_does_not_release_reservation() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-stale-failure",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-stale-failure",
                request_hash="hash-stale-failure",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="processing",
                progress=50,
                lease_owner="new-worker",
                lease_token="new-token",
                input_json={"reservation_id": "reservation-stale-failure"},
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-stale-failure",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-stale-failure",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()

        completed = await generation_worker._fail_generation_with_release(
            db,
            "job-stale-failure",
            "old-token",
            "reservation-stale-failure",
            error_type="provider_or_storage_error",
            retryable=False,
        )

        assert completed is False
        assert raw.get(GenerationJob, "job-stale-failure").status == "processing"
        assert raw.get(CreditReservation, "reservation-stale-failure").status == "reserved"
        assert raw.get(CreditAccount, "account-1").frozen_points == 10

    asyncio.run(scenario())


def test_generation_failure_rolls_back_release_when_terminal_cas_fails(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-failure-cas",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-failure-cas",
                request_hash="hash-failure-cas",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="processing",
                progress=50,
                lease_owner="worker-1",
                lease_token="token-current",
                input_json={"reservation_id": "reservation-failure-cas"},
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-failure-cas",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-failure-cas",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()
        events: list[str] = []
        original_lock = generation_worker._lock_current_generation_job
        original_release = generation_worker.credits_release

        async def record_lock(*args, **kwargs):
            events.append("job_lock")
            return await original_lock(*args, **kwargs)

        async def record_release(*args, **kwargs):
            events.append("release")
            return await original_release(*args, **kwargs)

        async def terminal_cas_false(*args, **kwargs):
            events.append("terminal_cas")
            return False

        monkeypatch.setattr(
            generation_worker, "_lock_current_generation_job", record_lock
        )
        monkeypatch.setattr(generation_worker, "credits_release", record_release)
        monkeypatch.setattr(generation_worker, "finish_generation_job", terminal_cas_false)

        completed = await generation_worker._fail_generation_with_release(
            db,
            "job-failure-cas",
            "token-current",
            "reservation-failure-cas",
            error_type="provider_or_storage_error",
            retryable=False,
        )

        assert completed is False
        assert events == ["job_lock", "release", "terminal_cas"]
        assert raw.get(GenerationJob, "job-failure-cas").status == "processing"
        assert raw.get(CreditReservation, "reservation-failure-cas").status == "reserved"
        assert raw.get(CreditAccount, "account-1").frozen_points == 10

    asyncio.run(scenario())


def test_run_generation_uses_lease_scoped_temp_keys_and_delete_compensation(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-delete-compensation",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-delete-compensation",
                request_hash="hash-delete-compensation",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": "原始主体描述",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 1,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "reservation_id": "reservation-delete-compensation",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-delete-compensation",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-delete-compensation",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()

        class Provider:
            async def generate(self, **kwargs):
                return [ImageResult(data=_png(), mime_type="image/png")]

        class Storage:
            def __init__(self) -> None:
                self.puts: list[str] = []
                self.promotes: list[tuple[str, str]] = []

            async def put(self, key, data):
                self.puts.append(key)

            async def promote(self, source_key, final_key):
                self.promotes.append((source_key, final_key))

            async def delete(self, key):
                raise OSError("delete failed /private/path")

        async def fail_add_candidate(*args, **kwargs):
            raise RuntimeError("db failed")

        class CompensationDb:
            async def commit(self):
                return None

        compensation_db = CompensationDb()

        class CompensationContext:
            async def __aenter__(self):
                return compensation_db

            async def __aexit__(self, exc_type, exc, tb):
                return None

        compensation_deletes: list[str] = []

        async def fake_enqueue_delete(db, *, idempotency_key, source_key, **kwargs):
            assert db is compensation_db
            compensation_deletes.append(source_key)

        storage = Storage()
        monkeypatch.setattr(generation_worker, "add_candidate", fail_add_candidate)
        monkeypatch.setattr(generation_worker, "enqueue_delete", fake_enqueue_delete)
        monkeypatch.setattr(
            generation_worker,
            "async_session_factory",
            lambda: CompensationContext(),
        )
        await generation_worker.run_generation_once(
            db,
            "job-delete-compensation",
            owner="worker-1",
            provider=Provider(),
            storage=storage,
        )

        assert storage.puts
        assert "/tmp/" in storage.puts[0]
        assert "job-delete-compensation" in storage.puts[0]
        assert set(storage.puts) <= set(compensation_deletes)

    asyncio.run(scenario())


def test_generation_failure_rolls_back_candidates_and_financial_cleanup_reconciles(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-fail",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-fail",
                request_hash="hash-fail",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": "原始主体描述",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 1,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "reservation_id": "reservation-fail",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-fail",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-fail",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()

        class Provider:
            async def generate(self, **kwargs):
                return [ImageResult(data=_png(), mime_type="image/png")]

        class FailingStorage:
            async def put(self, key, data):
                raise OSError("secret-token-123")

        async def failing_release(*args, **kwargs):
            raise RuntimeError("release facility down")

        monkeypatch.setattr(generation_worker, "credits_release", failing_release)
        await generation_worker.run_generation_once(
            db,
            "job-fail",
            owner="worker-1",
            provider=Provider(),
            storage=FailingStorage(),
        )
        failed = raw.get(GenerationJob, "job-fail")
        assert failed.status == "failed"
        assert failed.error_type == "financial_cleanup"
        assert failed.retryable is True
        assert "secret-token-123" not in (failed.error_message or "")
        assert raw.scalars(select(AssetEntry)).all() == []

        released: list[str] = []

        async def release_ok(db, reservation_id, **kwargs):
            released.append(reservation_id)
            reservation = raw.get(CreditReservation, reservation_id)
            reservation.status = "released"

        monkeypatch.setattr(generation_worker, "credits_release", release_ok)
        assert await generation_worker.reconcile_generation_jobs(db) == 1
        assert released == ["reservation-fail"]
        assert raw.get(GenerationJob, "job-fail").retryable is False

    asyncio.run(scenario())


def test_settle_db_failure_records_financial_cleanup_action_and_actual_points(monkeypatch):
    async def scenario() -> None:
        engine, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-settle-failure",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-settle-failure",
                request_hash="hash-settle-failure",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": "原始主体描述",
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 2,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "subject_version": 1,
                    "enrich_version": 1,
                    "reservation_id": "reservation-settle-failure",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-settle-failure",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-settle-failure",
                reserved_points=20,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 20
        raw.commit()

        class Provider:
            async def generate(self, **kwargs):
                return [
                    ImageResult(data=_png(), mime_type="image/png"),
                    ImageResult(data=_png((0, 0, 255)), mime_type="image/png"),
                ]

        class Storage:
            async def put(self, key, data):
                return None

            async def delete(self, key):
                raise OSError("delete failed")

        class CompensationDb:
            async def commit(self):
                return None

        compensation_db = CompensationDb()

        class CompensationContext:
            async def __aenter__(self):
                return compensation_db

            async def __aexit__(self, exc_type, exc, tb):
                return None

        compensation_deletes: list[str] = []

        async def fake_enqueue_delete(db, *, idempotency_key, source_key, **kwargs):
            assert db is compensation_db
            compensation_deletes.append(source_key)

        async def failing_settle(*args, **kwargs):
            raise RuntimeError("db settle failed")

        monkeypatch.setattr(generation_worker.settings, "price_image_gen", 10)
        monkeypatch.setattr(generation_worker, "credits_settle", failing_settle)
        monkeypatch.setattr(
            generation_worker,
            "async_session_factory",
            lambda: CompensationContext(),
        )
        monkeypatch.setattr(generation_worker, "enqueue_delete", fake_enqueue_delete)
        await generation_worker.run_generation_once(
            db,
            "job-settle-failure",
            owner="worker-1",
            provider=Provider(),
            storage=Storage(),
        )

        failed = raw.get(GenerationJob, "job-settle-failure")
        assert failed.status == "failed"
        assert failed.error_type == "financial_cleanup"
        assert failed.metrics["financial_cleanup"]["action"] == "settle"
        assert failed.metrics["financial_cleanup"]["actual_points"] == 20
        assert raw.get(CreditReservation, "reservation-settle-failure").status == "reserved"
        assert raw.scalars(select(AssetEntry)).all() == []
        assert len(compensation_deletes) == 4

    asyncio.run(scenario())


def test_reconciler_does_not_leave_failed_captured_without_assets(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-settle-reconcile",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-settle-reconcile",
                request_hash="hash-settle-reconcile",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="failed",
                progress=10,
                error_type="financial_cleanup",
                retryable=True,
                input_json={"reservation_id": "reservation-settle-reconcile"},
                metrics={
                    "financial_cleanup": {
                        "action": "settle",
                        "reservation_id": "reservation-settle-reconcile",
                        "actual_points": 10,
                    }
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-settle-reconcile",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-settle-reconcile",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.add(
            AssetEntry(
                id="asset-old-candidate",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key="generate_images/material-1/old.png",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
                sha256="0" * 64,
                status="candidate",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()

        assert await generation_worker.reconcile_generation_jobs(db) == 1

        job = raw.get(GenerationJob, "job-settle-reconcile")
        reservation = raw.get(CreditReservation, "reservation-settle-reconcile")
        assert not (
            job.status == "failed"
            and reservation.status == "captured"
            and job.output_json is None
        )
        assert reservation.status == "released"
        assert job.retryable is False

    asyncio.run(scenario())


def test_run_generation_deletes_written_objects_when_db_side_effect_fails(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        raw.add(
            GenerationJob(
                job_id="job-db-fail",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-db-fail",
                request_hash="hash-db-fail",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="starting",
                progress=0,
                input_json={
                    "prompt": build_base_prompt(raw.get(Subject, "subject-1")),
                    "resolved_prompts": {"final": _twelve_sections()},
                    "model": "banana-2",
                    "size": "2k",
                    "aspect": "16:9",
                    "count": 1,
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "reservation_id": "reservation-db-fail",
                },
            )
        )
        raw.add(
            CreditReservation(
                id="reservation-db-fail",
                account_id="account-1",
                source_type="generation_job",
                source_id="job-db-fail",
                reserved_points=10,
                status="reserved",
            )
        )
        raw.get(CreditAccount, "account-1").frozen_points = 10
        raw.commit()

        class Provider:
            async def generate(self, **kwargs):
                return [ImageResult(data=_png(), mime_type="image/png")]

        class TrackingStorage:
            def __init__(self) -> None:
                self.puts: list[str] = []
                self.deletes: list[str] = []

            async def put(self, key, data):
                self.puts.append(key)
                return key

            async def delete(self, key):
                self.deletes.append(key)

        async def fail_add_candidate(*args, **kwargs):
            raise RuntimeError("db path /private/storage/key leaked")

        storage = TrackingStorage()
        monkeypatch.setattr(generation_worker, "add_candidate", fail_add_candidate)
        await generation_worker.run_generation_once(
            db,
            "job-db-fail",
            owner="worker-1",
            provider=Provider(),
            storage=storage,
        )

        assert storage.puts
        assert set(storage.puts) <= set(storage.deletes)
        failed = raw.get(GenerationJob, "job-db-fail")
        assert failed.status == "failed"
        assert failed.error_message == "provider_or_storage_error"
        assert "private" not in failed.error_message
        assert raw.scalars(select(AssetEntry)).all() == []

    asyncio.run(scenario())


def test_real_celery_task_entry_uses_run_generation_once(monkeypatch):
    calls: list[tuple[str, str, str]] = []

    class SessionContext:
        async def __aenter__(self):
            return "db-session"

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def fake_run_generation_once(db, job_id, *, owner, **kwargs):
        calls.append((db, job_id, owner))

    monkeypatch.setattr(generation_worker, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(generation_worker, "run_generation_once", fake_run_generation_once)
    monkeypatch.setattr(
        generation_worker,
        "_run_generation",
        lambda job_id: (_ for _ in ()).throw(AssertionError("旧 worker 路径不应被调用")),
    )

    generation_worker.generate_images_task.run("job-real-entry")

    assert calls == [("db-session", "job-real-entry", "celery:generation.generate_images")]


def test_resource_access_uses_404_for_global_missing_and_403_for_foreign(monkeypatch):
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)
        with pytest.raises(NotFoundError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(material_id="missing-material"),
                idempotency_key="key-missing",
            )
        raw.add(
            Subject(
                id="subject-foreign",
                script_id="script-foreign",
                type="character",
                name="外部主体",
                description="外部描述",
                review_status="not_designed",
            )
        )
        raw.commit()
        with pytest.raises(ForbiddenError):
            await generation_service.create_generation_job(
                db,
                user=seeded.user,
                payload=_create_payload(subject_id="subject-foreign"),
                idempotency_key="key-foreign",
            )

    asyncio.run(scenario())


async def test_generation_jobs_asgi_requires_non_blank_idempotency_key(client, monkeypatch):
    from app.core.auth import get_current_user
    from app.core.database import get_db

    _, raw = _make_db()
    seeded = _seed(raw)
    db = _AsyncSessionAdapter(raw)

    async def override_db():
        yield db

    async def override_user():
        yield seeded.user

    client._app.dependency_overrides[get_db] = override_db
    client._app.dependency_overrides[get_current_user] = override_user

    response = await client.post(
        "/v3api/generation-jobs",
        json={"material_id": seeded.material.id, "subject_id": seeded.subject.id},
        headers={"Idempotency-Key": "   "},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "缺少 Idempotency-Key"

    batch_response = await client.post(
        "/v3api/generation-jobs/batch",
        json={"material_id": seeded.material.id, "subject_ids": [seeded.subject.id]},
    )
    assert batch_response.status_code == 422


def test_list_jobs_missing_material_returns_404_not_403():
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)

        with pytest.raises(NotFoundError):
            await generation_api.list_jobs_endpoint(
                material_id="missing-material",
                limit=30,
                offset=0,
                db=db,
                user=seeded.user,
            )

    asyncio.run(scenario())


def test_stale_reconciler_requeues_published_outbox_with_token_cas():
    async def scenario() -> None:
        _, raw = _make_db()
        _seed(raw)
        db = _AsyncSessionAdapter(raw)
        expired = datetime.now(UTC) - timedelta(seconds=1)
        raw.add(
            GenerationJob(
                job_id="job-stale-requeue",
                owner_user_id="user-1",
                material_id="material-1",
                script_id="script-1",
                subject_id="subject-1",
                idempotency_key="key-stale-requeue",
                request_hash="hash-stale-requeue",
                input_version=7,
                attempt=1,
                job_type="generate_images",
                status="processing",
                progress=10,
                input_json={
                    "subject_id": "subject-1",
                    "input_version": 7,
                    "reservation_id": "reservation-stale-requeue",
                },
                lease_owner="old-worker",
                lease_token="old-token",
                lease_until=expired,
            )
        )
        raw.add(
            TaskDispatchOutbox(
                id="outbox-stale-published",
                task_type="generation.generate_images",
                job_id="job-stale-requeue",
                attempt=1,
                payload={"job_id": "job-stale-requeue", "dispatch_id": "outbox-stale-published", "run_id": "job-stale-requeue"},
                status="published",
                published_at=expired,
            )
        )
        raw.commit()

        assert await generation_worker.reconcile_stale_generation_jobs(db) == 1
        job = raw.get(GenerationJob, "job-stale-requeue")
        assert job.status == "starting"
        rows = raw.scalars(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.job_id == "job-stale-requeue"
            )
        ).all()
        assert any(row.status in {"pending", "retry"} for row in rows)

    asyncio.run(scenario())


def test_bailian_image_model_is_accepted_and_routes_to_provider() -> None:
    from app.providers.image_gen import get_image_provider
    from app.providers.image_gen_bailian import BailianImageGenProvider
    from app.schemas.generation import CreateJobIn

    request = CreateJobIn(
        material_id="material-1",
        subject_id="subject-1",
        model="qwen-image-2.0",
    )

    assert request.model == "qwen-image-2.0"
    assert isinstance(get_image_provider(request.model), BailianImageGenProvider)
    with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
        BailianImageGenProvider._validate_download_host(
            "http://127.0.0.1/private.png",
            "https://dashscope.aliyuncs.com",
        )
