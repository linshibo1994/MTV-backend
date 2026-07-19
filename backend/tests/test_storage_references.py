"""生命周期删除的全局对象存储引用判权测试。"""

import asyncio
import hashlib
import os
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, delete, event, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.core.errors import ConflictError
from app.models import storage_reference_guard
from app.models.asset_entry import AssetEntry
from app.models.base import Base
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_storage_job import MediaStorageJob
from app.models.media_upload import MediaUpload
from app.models.pipeline_job import PipelineJob
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.storage_reference_guard import (
    acquire_storage_key_locks,
    storage_key_lock_id,
)
from app.models.storyboard_job import StoryboardJob
from app.models.subject import Subject
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.services import storage_references
from app.services.storage_references import (
    StorageReferenceExclusions,
    acquire_storage_reference_barrier,
    decide_storage_key_deletes,
    enqueue_storage_deletes,
)


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    def add(self, row) -> None:
        self.session.add(row)

    async def flush(self) -> None:
        self.session.flush()

    def get_bind(self):
        return self.session.get_bind()


class _RecordingPostgresSession:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    async def execute(self, statement) -> None:
        self.statements.append(str(statement))


class _AdvisoryRecordingPostgresSession:
    def __init__(self) -> None:
        self.lock_ids: list[int] = []

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    async def execute(self, statement) -> None:
        values = list(statement.compile().params.values())
        self.lock_ids.append(next(value for value in values if isinstance(value, int)))


def test_storage_reference_barrier_does_not_take_relation_locks() -> None:
    async def scenario() -> None:
        db = _RecordingPostgresSession()

        await acquire_storage_reference_barrier(db)

        assert db.statements == []

    asyncio.run(scenario())


def test_storage_key_advisory_locks_use_stable_hash_and_normalized_order() -> None:
    async def scenario() -> None:
        db = _AdvisoryRecordingPostgresSession()
        first = "generate_images/order/a.png"
        second = "generate_images/order/b.png"

        locked = await acquire_storage_key_locks(
            db,
            {second, f"/{first}", first},
        )

        assert locked == (first, second)
        assert db.lock_ids == [storage_key_lock_id(first), storage_key_lock_id(second)]
        assert storage_key_lock_id("generate_images/stable/key.png") == -597364162470872562

    asyncio.run(scenario())


def test_storage_key_hash_collision_remains_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Rows:
        def all(self):
            return [("delete-collision-a", "succeeded")]

    class _Connection:
        def __init__(self) -> None:
            self.dialect = SimpleNamespace(name="postgresql")
            self.lock_ids: list[int] = []
            self.queried_keys: set[str] = set()

        def execute(self, statement):
            compiled = statement.compile()
            params = list(compiled.params.values())
            if "pg_advisory_xact_lock" in str(statement):
                self.lock_ids.append(next(value for value in params if isinstance(value, int)))
                return _Rows()
            for value in params:
                if isinstance(value, list):
                    self.queried_keys.update(
                        item for item in value if isinstance(item, str) and "/" in item
                    )
            return _Rows()

    first = "generate_images/collision/a.png"
    second = "generate_images/collision/b.png"
    connection = _Connection()
    monkeypatch.setattr(storage_reference_guard, "storage_key_lock_id", lambda _key: 7)

    with pytest.raises(ConflictError, match="正在删除|已删除"):
        storage_reference_guard._guard_reference_keys(connection, {second, first})

    assert connection.lock_ids == [7, 7]
    assert connection.queried_keys == {first, second}


def test_promote_closure_applies_database_limit_before_materializing_rows() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        promote_queries: list[str] = []

        @event.listens_for(engine, "before_cursor_execute")
        def _record_promote_query(_conn, _cursor, statement, _parameters, _context, _many):
            normalized = " ".join(statement.lower().split())
            if (
                normalized.startswith("select")
                and "from media_storage_jobs" in normalized
                and "source_key in" in normalized
            ):
                promote_queries.append(normalized)

        with Session(engine, expire_on_commit=False) as raw:
            raw.add(
                MediaStorageJob(
                    id="promote-query-limit",
                    idempotency_key="promote-query-limit",
                    job_type="promote",
                    source_key="upload_images/_tmp/query-limit.png",
                    final_key="upload_images/query-limit.png",
                    status="pending",
                )
            )
            raw.commit()

            await enqueue_storage_deletes(
                _AsyncSessionAdapter(raw),
                {"upload_images/query-limit.png"},
                exclusions=StorageReferenceExclusions(),
            )

        assert promote_queries
        assert all(" limit " in query for query in promote_queries)

    asyncio.run(scenario())


def test_reference_writer_guard_covers_bulk_json_mutation_insertmany_and_raw_sql() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script = Script(
            id="script-writer-guard",
            title="写入守卫",
            scope="personal",
            owner_user_id="user-writer-guard",
            created_by_user_id="user-writer-guard",
        )
        subject = Subject(
            id="subject-writer-guard",
            script_id=script.id,
            type="character",
            name="角色",
        )
        pipeline = PipelineJob(
            id="pipeline-writer-guard",
            script_id=script.id,
            stage="split",
            dispatch_id="dispatch-writer-guard",
            run_id="run-writer-guard",
            status="done",
            input_version=1,
            attempt=1,
            output_json={},
        )
        keys = {
            "mutation": "generate_outputs/writer-guard/mutation.json",
            "bulk": "generate_outputs/writer-guard/bulk.json",
            "insert_a": "generate_images/writer-guard/insert-a.png",
            "insert_b": "generate_images/writer-guard/insert-b.png",
            "dirty_tombstone": "generate_outputs/writer-guard/dirty-tombstone.json",
        }
        jobs = {
            name: MediaStorageJob(
                id=f"delete-writer-guard-{name}",
                idempotency_key=f"delete-writer-guard-{name}",
                job_type="delete",
                source_key=key,
                status="pending",
            )
            for name, key in keys.items()
        }
        raw.add_all([script, subject, pipeline, *jobs.values()])
        raw.commit()

        pipeline.output_json["storage_key"] = keys["mutation"]
        flag_modified(pipeline, "output_json")
        raw.commit()
        raw.refresh(jobs["mutation"])
        assert jobs["mutation"].status == "canceled"

        raw.execute(
            update(PipelineJob)
            .where(PipelineJob.id == pipeline.id)
            .values(output_json={"storage_key": keys["bulk"]})
        )
        raw.commit()
        raw.refresh(jobs["bulk"])
        assert jobs["bulk"].status == "canceled"

        raw.add_all(
            [
                AssetEntry(
                    id=f"asset-writer-guard-{suffix}",
                    script_id=script.id,
                    subject_id=subject.id,
                    kind="image",
                    storage_key=keys[f"insert_{suffix}"],
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                )
                for suffix in ("a", "b")
            ]
        )
        raw.commit()
        for name in ("insert_a", "insert_b"):
            raw.refresh(jobs[name])
            assert jobs[name].status == "canceled"

        jobs["dirty_tombstone"].status = "deleting"
        pipeline.output_json = {"storage_key": keys["dirty_tombstone"]}
        with pytest.raises(ConflictError, match="正在删除|已删除"):
            raw.commit()
        raw.rollback()
        raw.refresh(jobs["dirty_tombstone"])
        assert jobs["dirty_tombstone"].status == "pending"

        with pytest.raises(ConflictError, match="原始 SQL"):
            raw.execute(
                text(
                    "UPDATE pipeline_jobs SET output_json = '{}' "
                    "WHERE id = 'pipeline-writer-guard'"
                )
            )
        raw.rollback()


def test_reference_scan_fallback_has_hard_row_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        monkeypatch.setattr(storage_references, "_MAX_REFERENCE_SCAN_ROWS", 2)
        with Session(engine, expire_on_commit=False) as raw:
            script = Script(
                id="script-scan-limit",
                title="扫描上限",
                scope="personal",
                owner_user_id="user-scan-limit",
                created_by_user_id="user-scan-limit",
            )
            raw.add(script)
            raw.flush()
            raw.add_all(
                [
                    PipelineJob(
                        id=f"pipeline-scan-limit-{index}",
                        script_id=script.id,
                        stage="split",
                        dispatch_id=f"dispatch-scan-limit-{index}",
                        run_id=f"run-scan-limit-{index}",
                        status="done",
                        input_version=1,
                        attempt=1,
                        output_json={"storage_key": f"generate_outputs/unrelated/{index}.json"},
                    )
                    for index in range(3)
                ]
            )
            raw.commit()

            with pytest.raises(ConflictError, match="引用结果过多"):
                await decide_storage_key_deletes(
                    _AsyncSessionAdapter(raw),
                    {"generate_outputs/scan-limit/candidate.json"},
                    exclusions=StorageReferenceExclusions(),
                )

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过 PostgreSQL FK 锁反环测试",
)
@pytest.mark.parametrize("writer_kind", ["asset", "generation", "timeline"])
def test_postgres_reference_writer_waits_one_way_without_parent_lock_cycle(
    writer_kind: str,
) -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:8]
        user_id = f"u-fk-{writer_kind}-{suffix}"
        script_id = f"s-fk-{writer_kind}-{suffix}"
        episode_id = f"e-fk-{writer_kind}-{suffix}"
        subject_id = f"sub-fk-{writer_kind}-{suffix}"
        material_id = f"m-fk-{writer_kind}-{suffix}"
        row_id = f"row-fk-{writer_kind}-{suffix}"
        candidate_key = f"generate_outputs/fk-{writer_kind}/{suffix}.bin"

        async def _wait_for_parent_block(
            observer,
            *,
            writer_pid: int,
            parent_pid: int,
        ) -> None:
            for _ in range(500):
                blockers = (
                    await observer.execute(
                        text("SELECT pg_blocking_pids(:pid)"),
                        {"pid": writer_pid},
                    )
                ).scalar_one()
                if parent_pid in blockers:
                    return
                await asyncio.sleep(0.01)
            raise AssertionError("未观察到 writer 等待父行锁")

        try:
            async with session_factory() as db:
                user = User(
                    user_id=user_id,
                    username=f"fk-{writer_kind}-{suffix}",
                    password_hash="unused",
                )
                script = Script(
                    id=script_id,
                    title="FK 单向等待",
                    scope="personal",
                    owner_user_id=user_id,
                    created_by_user_id=user_id,
                )
                db.add_all([user, script])
                await db.flush()
                db.add_all(
                    [
                        Episode(
                            episode_id=episode_id,
                            script_id=script_id,
                            episode_no=1,
                            title="第一集",
                        ),
                        Subject(
                            id=subject_id,
                            script_id=script_id,
                            type="character",
                            name="FK 主体",
                        ),
                        Materials(
                            id=material_id,
                            script_id=script_id,
                            name="FK 画布",
                        ),
                    ]
                )
                await db.commit()

            async with (
                session_factory() as parent_db,
                session_factory() as writer_db,
                session_factory() as observer,
            ):
                parent_pid = int(
                    (await parent_db.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                writer_pid = int(
                    (await writer_db.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                await parent_db.execute(
                    select(Script).where(Script.id == script_id).with_for_update()
                )
                if writer_kind == "asset":
                    writer_row = AssetEntry(
                        id=row_id,
                        script_id=script_id,
                        subject_id=subject_id,
                        kind="image",
                        storage_key=candidate_key,
                        mime_type="image/png",
                        size_bytes=1,
                        source="generate_images",
                    )
                elif writer_kind == "generation":
                    writer_row = GenerationJob(
                        job_id=row_id,
                        material_id=material_id,
                        script_id=script_id,
                        subject_id=subject_id,
                        episode_id=episode_id,
                        owner_user_id=user_id,
                        idempotency_key=f"idem-{row_id}",
                        request_hash="a" * 64,
                        input_version=1,
                        attempt=1,
                        job_type="generate_images",
                        status="succeeded",
                        progress=100,
                        output_json={"storage_key": candidate_key},
                    )
                else:
                    writer_row = TimelineRenderJob(
                        job_id=row_id,
                        script_id=script_id,
                        episode_id=episode_id,
                        owner_user_id=user_id,
                        idempotency_key=f"idem-{row_id}",
                        request_hash="b" * 64,
                        input_version=1,
                        attempt=1,
                        status="succeeded",
                        progress=100,
                        input_json={},
                        output_json={"storage_key": candidate_key},
                        output_storage_key=candidate_key,
                    )
                writer_db.add(writer_row)
                writer_task = asyncio.create_task(writer_db.commit())
                await asyncio.wait_for(
                    _wait_for_parent_block(
                        observer,
                        writer_pid=writer_pid,
                        parent_pid=parent_pid,
                    ),
                    timeout=10,
                )

                decision = await asyncio.wait_for(
                    decide_storage_key_deletes(
                        parent_db,
                        {candidate_key},
                        exclusions=StorageReferenceExclusions(),
                    ),
                    timeout=5,
                )
                assert decision.deletable_keys == {candidate_key}
                parent_blockers = (
                    await observer.execute(
                        text("SELECT pg_blocking_pids(:pid)"),
                        {"pid": parent_pid},
                    )
                ).scalar_one()
                assert writer_pid not in parent_blockers

                await parent_db.rollback()
                assert await asyncio.wait_for(writer_task, timeout=5) is None
        finally:
            async with session_factory() as db:
                await db.execute(delete(AssetEntry).where(AssetEntry.id == row_id))
                await db.execute(delete(GenerationJob).where(GenerationJob.job_id == row_id))
                await db.execute(
                    delete(TimelineRenderJob).where(TimelineRenderJob.job_id == row_id)
                )
                await db.execute(delete(Materials).where(Materials.id == material_id))
                await db.execute(delete(Subject).where(Subject.id == subject_id))
                await db.execute(delete(Episode).where(Episode.episode_id == episode_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.execute(delete(User).where(User.user_id == user_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过 PostgreSQL 引用预筛测试",
)
@pytest.mark.parametrize("leading_slashes", [1, 4])
def test_postgres_reference_prefilter_limits_scalar_and_jsonb_rows(
    monkeypatch: pytest.MonkeyPatch,
    leading_slashes: int,
) -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:8]
        user_id = f"u-prefilter-{suffix}"
        script_id = f"s-prefilter-{suffix}"
        episode_id = f"e-prefilter-{suffix}"
        subject_id = f"sub-prefilter-{suffix}"
        material_id = f"m-prefilter-{suffix}"
        candidate_key = f"generate_outputs/prefilter/{suffix}.mp4"
        stored_candidate_key = f"{'/' * leading_slashes}{candidate_key}"
        recorded_queries: list[str] = []

        def _record_reference_query(
            _conn,
            _cursor,
            statement,
            _parameters,
            _context,
            _many,
        ) -> None:
            normalized = " ".join(statement.lower().split())
            if normalized.startswith("select") and (
                " from generation_jobs " in normalized
                or " from timeline_render_jobs " in normalized
            ):
                recorded_queries.append(normalized)

        event.listen(engine.sync_engine, "before_cursor_execute", _record_reference_query)
        try:
            async with session_factory() as db:
                user = User(
                    user_id=user_id,
                    username=f"prefilter-{suffix}",
                    password_hash="unused",
                )
                script = Script(
                    id=script_id,
                    title="PG 引用预筛",
                    scope="personal",
                    owner_user_id=user_id,
                    created_by_user_id=user_id,
                )
                db.add_all([user, script])
                await db.flush()
                db.add_all(
                    [
                        Episode(
                            episode_id=episode_id,
                            script_id=script_id,
                            episode_no=1,
                            title="第一集",
                        ),
                        Subject(
                            id=subject_id,
                            script_id=script_id,
                            type="character",
                            name="预筛主体",
                        ),
                        Materials(
                            id=material_id,
                            script_id=script_id,
                            name="预筛画布",
                        ),
                    ]
                )
                await db.flush()
                for index in range(6):
                    db.add(
                        GenerationJob(
                            job_id=f"g-u-{index}-{suffix}",
                            material_id=material_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            episode_id=episode_id,
                            owner_user_id=user_id,
                            idempotency_key=f"g-u-{index}-{suffix}",
                            request_hash="c" * 64,
                            input_version=1,
                            attempt=1,
                            job_type="generate_images",
                            status="succeeded",
                            progress=100,
                            output_json={
                                "storage_key": f"generate_outputs/unrelated/{suffix}-{index}.mp4"
                            },
                        )
                    )
                    db.add(
                        TimelineRenderJob(
                            job_id=f"t-u-{index}-{suffix}",
                            script_id=script_id,
                            episode_id=episode_id,
                            owner_user_id=user_id,
                            idempotency_key=f"t-u-{index}-{suffix}",
                            request_hash="d" * 64,
                            input_version=1,
                            attempt=1,
                            status="succeeded",
                            progress=100,
                            input_json={},
                            output_json={},
                            output_storage_key=(
                                f"generate_outputs/unrelated/{suffix}-timeline-{index}.mp4"
                            ),
                        )
                    )
                db.add_all(
                    [
                        GenerationJob(
                            job_id=f"g-match-{suffix}",
                            material_id=material_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            episode_id=episode_id,
                            owner_user_id=user_id,
                            idempotency_key=f"g-match-{suffix}",
                            request_hash="e" * 64,
                            input_version=1,
                            attempt=1,
                            job_type="generate_images",
                            status="succeeded",
                            progress=100,
                            output_json={
                                "nested": [{"storage_key": stored_candidate_key}]
                            },
                        ),
                        TimelineRenderJob(
                            job_id=f"t-match-{suffix}",
                            script_id=script_id,
                            episode_id=episode_id,
                            owner_user_id=user_id,
                            idempotency_key=f"t-match-{suffix}",
                            request_hash="f" * 64,
                            input_version=1,
                            attempt=1,
                            status="succeeded",
                            progress=100,
                            input_json={},
                            output_json={},
                            output_storage_key=stored_candidate_key,
                        ),
                    ]
                )
                await db.commit()

            recorded_queries.clear()
            monkeypatch.setattr(storage_references, "_MAX_REFERENCE_SCAN_ROWS", 1)
            async with session_factory() as db:
                decision = await decide_storage_key_deletes(
                    db,
                    {candidate_key},
                    exclusions=StorageReferenceExclusions(),
                )
                assert decision.protected_keys == {candidate_key}
                assert decision.deletable_keys == set()

            generation_query = next(
                query for query in recorded_queries if " from generation_jobs " in query
            )
            timeline_query = next(
                query for query in recorded_queries if " from timeline_render_jobs " in query
            )
            assert "jsonb_path_exists" in generation_query
            assert "ltrim(timeline_render_jobs.output_storage_key" in timeline_query
            assert "jsonb_path_exists" in timeline_query
            assert candidate_key not in generation_query
            assert candidate_key not in timeline_query
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", _record_reference_query)
            async with session_factory() as db:
                await db.execute(
                    delete(GenerationJob).where(GenerationJob.script_id == script_id)
                )
                await db.execute(
                    delete(TimelineRenderJob).where(TimelineRenderJob.script_id == script_id)
                )
                await db.execute(delete(Materials).where(Materials.id == material_id))
                await db.execute(delete(Subject).where(Subject.id == subject_id))
                await db.execute(delete(Episode).where(Episode.episode_id == episode_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.execute(delete(User).where(User.user_id == user_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


def test_global_storage_reference_matrix_protects_every_persisted_field() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            keys = {
                "asset": "generate_images/matrix/asset.png",
                "upload_temp": "upload_images/_tmp/matrix/upload.png",
                "upload_final": "upload_images/matrix/upload.png",
                "episode": "upload_images/matrix/episode.txt",
                "generation_output": "generate_images/matrix/generation-output.png",
                "generation_metrics": "generate_images/matrix/generation-metrics.png",
                "video_input": "generate_images/matrix/video-input.png",
                "video_output": "generate_outputs/matrix/video-output.mp4",
                "video_metrics": "generate_images/matrix/video-metrics.png",
                "timeline_input": "generate_outputs/matrix/timeline-input.mp4",
                "timeline_output": "generate_outputs/matrix/timeline-output.json",
                "timeline_key": "render_outputs/matrix/timeline.mp4",
                "storyboard": "generate_outputs/matrix/storyboard.json",
                "pipeline": "generate_outputs/matrix/pipeline.json",
                "shot_reference": "generate_images/matrix/shot-reference.png",
                "shot_material": "upload_images/matrix/shot-material.png",
                "shot_audio": "upload_images/matrix/shot-audio.mp3",
                "canvas_node": "upload_images/matrix/canvas-node.png",
                "canvas_edge": "generate_images/matrix/canvas-edge.png",
                "orphan": "generate_outputs/matrix/orphan.bin",
            }
            user = User(
                user_id="user-matrix",
                username="matrix",
                password_hash="unused",
            )
            script = Script(
                id="script-matrix",
                title="引用矩阵",
                scope="personal",
                owner_user_id=user.user_id,
                created_by_user_id=user.user_id,
            )
            episode = Episode(
                episode_id="episode-matrix",
                script_id=script.id,
                episode_no=1,
                title="第一集",
                script_file_storage_key=keys["episode"],
            )
            subject = Subject(
                id="subject-matrix",
                script_id=script.id,
                type="character",
                name="角色",
                episode_nos=[1],
            )
            material = Materials(
                id="material-matrix",
                script_id=script.id,
                name="画布",
            )
            raw.add_all([user, script, episode, subject, material])
            raw.flush()

            asset = AssetEntry(
                id="asset-matrix",
                script_id=script.id,
                subject_id=subject.id,
                kind="image",
                storage_key=keys["asset"],
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
            upload = MediaUpload(
                id="upload-matrix",
                script_id=script.id,
                material_id=material.id,
                subject_id=subject.id,
                temp_storage_key=keys["upload_temp"],
                final_storage_key=keys["upload_final"],
                original_filename="matrix.png",
                mime_type="image/png",
                size_bytes=1,
                sha256="a" * 64,
                status="ready",
            )
            generation = GenerationJob(
                job_id="generation-matrix",
                material_id=material.id,
                script_id=script.id,
                subject_id=subject.id,
                episode_id=episode.episode_id,
                owner_user_id=user.user_id,
                idempotency_key="generation-matrix",
                request_hash="b" * 64,
                input_version=1,
                attempt=1,
                job_type="generate_images",
                status="succeeded",
                progress=100,
                output_json={"storage_key": keys["generation_output"]},
                metrics={"storage_key": keys["generation_metrics"]},
            )
            group = ShotGroup(
                shot_group_id="group-matrix",
                script_id=script.id,
                episode_id=episode.episode_id,
                scene_no=1,
                group_no="G1",
                reference_frames={"storage_key": keys["shot_reference"]},
                materials={"items": [keys["shot_material"]]},
                audio_elements={"items": [keys["shot_audio"]]},
            )
            raw.add_all([asset, upload, generation, group])
            raw.flush()

            video = VideoGenerationJob(
                job_id="video-matrix",
                script_id=script.id,
                episode_id=episode.episode_id,
                shot_group_id=group.shot_group_id,
                scene_no=1,
                unit_id="group:group-matrix",
                unit_runtime_key="episode-matrix:1:group:group-matrix",
                provider_key="modelsai",
                job_type="all_reference",
                status="succeeded",
                input_json={"storage_key": keys["video_input"]},
                output_json={"storage_key": keys["video_output"]},
                metrics={"storage_key": keys["video_metrics"]},
            )
            timeline = TimelineRenderJob(
                job_id="timeline-matrix",
                script_id=script.id,
                episode_id=episode.episode_id,
                owner_user_id=user.user_id,
                idempotency_key="timeline-matrix",
                request_hash="c" * 64,
                input_json={"storage_key": keys["timeline_input"]},
                output_json={"storage_key": keys["timeline_output"]},
                output_storage_key=keys["timeline_key"],
                status="succeeded",
            )
            storyboard = StoryboardJob(
                job_id="storyboard-matrix",
                script_id=script.id,
                episode_id=episode.episode_id,
                idempotency_key="storyboard-matrix",
                request_hash="d" * 64,
                status="succeeded",
                progress=100,
                detail={"storage_key": keys["storyboard"]},
            )
            pipeline = PipelineJob(
                id="pipeline-matrix",
                script_id=script.id,
                episode_id=episode.episode_id,
                stage="split",
                dispatch_id="dispatch-matrix",
                run_id="run-matrix",
                status="done",
                input_version=1,
                attempt=1,
                output_json={"storage_key": keys["pipeline"]},
            )
            source_node = CanvasNode(
                id="node-source-matrix",
                material_id=material.id,
                type="uploaded",
                data={"storage_key": keys["canvas_node"]},
            )
            target_node = CanvasNode(
                id="node-target-matrix",
                material_id=material.id,
                type="text",
                data={},
            )
            raw.add_all(
                [video, timeline, storyboard, pipeline, source_node, target_node]
            )
            raw.flush()
            edge = CanvasEdge(
                id="edge-matrix",
                material_id=material.id,
                source=source_node.id,
                target=target_node.id,
                data={"storage_key": keys["canvas_edge"]},
            )
            raw.add(edge)
            raw.commit()

            candidates = set(keys.values())
            decision = await decide_storage_key_deletes(
                _AsyncSessionAdapter(raw),
                candidates,
                exclusions=StorageReferenceExclusions(),
            )
            assert decision.protected_keys == frozenset(candidates - {keys["orphan"]})
            assert decision.deletable_keys == {keys["orphan"]}

            exclusions = StorageReferenceExclusions(
                asset_entry_ids={asset.id},
                media_upload_ids={upload.id},
                episode_ids={episode.episode_id},
                generation_job_ids={generation.job_id},
                video_generation_job_ids={video.job_id},
                timeline_render_job_ids={timeline.job_id},
                storyboard_job_ids={storyboard.job_id},
                pipeline_job_ids={pipeline.id},
                shot_group_ids={group.shot_group_id},
                canvas_node_ids={source_node.id, target_node.id},
                canvas_edge_ids={edge.id},
            )
            fully_excluded = await decide_storage_key_deletes(
                _AsyncSessionAdapter(raw),
                candidates,
                exclusions=exclusions,
            )
            assert fully_excluded.protected_keys == frozenset()
            assert fully_excluded.deletable_keys == candidates

    asyncio.run(scenario())


@pytest.mark.parametrize("retain_intermediate", [True, False])
def test_enqueue_storage_deletes_closes_promote_graph_before_side_effects(
    retain_intermediate: bool,
) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            final_a = "upload_images/closure/final-a.png"
            intermediate_b = "upload_images/closure/intermediate-b.png"
            source_c = "upload_images/_tmp/closure/source-c.png"
            branch_d = "upload_images/_tmp/closure/branch-d.png"

            if retain_intermediate:
                user = User(
                    user_id="user-closure-retained",
                    username="closure-retained",
                    password_hash="unused",
                )
                script = Script(
                    id="script-closure-retained",
                    title="保留剧本",
                    scope="personal",
                    owner_user_id=user.user_id,
                    created_by_user_id=user.user_id,
                )
                subject = Subject(
                    id="subject-closure-retained",
                    script_id=script.id,
                    type="character",
                    name="保留主体",
                    episode_nos=[1],
                )
                material = Materials(
                    id="material-closure-retained",
                    script_id=script.id,
                    name="保留画布",
                )
                raw.add_all([user, script, subject, material])
                raw.flush()
                raw.add_all(
                    [
                        AssetEntry(
                            id="asset-closure-retained",
                            script_id=script.id,
                            subject_id=subject.id,
                            kind="image",
                            storage_key=intermediate_b,
                            mime_type="image/png",
                            size_bytes=1,
                            source="upload_images",
                        ),
                        MediaUpload(
                            id="upload-closure-retained",
                            script_id=script.id,
                            material_id=material.id,
                            subject_id=subject.id,
                            temp_storage_key=intermediate_b,
                            final_storage_key="upload_images/closure/retained-final.png",
                            original_filename="retained.png",
                            mime_type="image/png",
                            size_bytes=1,
                            sha256="e" * 64,
                            status="ready",
                        ),
                    ]
                )

            promote_b_to_a = MediaStorageJob(
                id="promote-closure-b-a",
                idempotency_key="promote-closure-b-a",
                job_type="promote",
                source_key=intermediate_b,
                final_key=final_a,
                status="pending",
            )
            promote_c_to_b = MediaStorageJob(
                id="promote-closure-c-b",
                idempotency_key="promote-closure-c-b",
                job_type="promote",
                source_key=source_c,
                final_key=intermediate_b,
                status="pending",
            )
            promote_d_to_a = MediaStorageJob(
                id="promote-closure-d-a",
                idempotency_key="promote-closure-d-a",
                job_type="promote",
                source_key=branch_d,
                final_key=final_a,
                status="pending",
            )
            raw.add_all([promote_b_to_a, promote_c_to_b, promote_d_to_a])
            raw.commit()

            await enqueue_storage_deletes(
                _AsyncSessionAdapter(raw),
                {final_a},
                exclusions=StorageReferenceExclusions(),
            )
            raw.commit()
            await enqueue_storage_deletes(
                _AsyncSessionAdapter(raw),
                {final_a},
                exclusions=StorageReferenceExclusions(),
            )
            raw.commit()
            raw.refresh(promote_b_to_a)
            raw.refresh(promote_c_to_b)
            raw.refresh(promote_d_to_a)
            delete_rows = list(
                raw.scalars(
                    select(MediaStorageJob)
                    .where(MediaStorageJob.job_type == "delete")
                    .order_by(MediaStorageJob.id.asc())
                )
            )
            delete_keys = {row.source_key for row in delete_rows}

            assert promote_b_to_a.status == "canceled"
            assert promote_d_to_a.status == "canceled"
            if retain_intermediate:
                assert promote_c_to_b.status == "pending"
                assert delete_keys == {final_a, branch_d}
            else:
                assert promote_c_to_b.status == "canceled"
                assert delete_keys == {final_a, intermediate_b, source_c, branch_d}
            assert len(delete_rows) == len(delete_keys)
            assert len({row.idempotency_key for row in delete_rows}) == len(delete_rows)

    asyncio.run(scenario())
