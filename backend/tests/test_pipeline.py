import asyncio
import importlib
import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from sqlalchemy import UniqueConstraint, create_engine, select, update
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.v1 import scripts as scripts_api
from app.core.errors import ConflictError
from app.core.job_state import (
    PipelineFailure,
    StageSnapshot,
    is_current_input_version,
    reset_stage,
)
from app.crud.script import replace_subjects
from app.models.base import Base
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.schemas import pipeline_contracts
from app.schemas.pipeline_contracts import (
    EnrichOutput,
    ExtractOutput,
    MergeOutput,
    ParentsOutput,
    SplitOutput,
    should_enrich_subject,
)
from app.schemas.script import EpisodeCreateIn
from app.services import generation as generation_service
from app.services.pipeline import (
    enrich,
    extract,
    input_episode_is_current,
    merge,
    orchestrator,
    parents,
    split,
)
from app.workers import pipeline as pipeline_worker


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


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


class _TriggerSessionAdapter(_AsyncSessionAdapter):
    async def execute(self, statement):
        if "pg_advisory_xact_lock" in str(statement):
            return self.session.execute(select(1))
        return await super().execute(statement)


class FakeProvider:
    def __init__(self, payloads: list[dict]) -> None:
        self._payloads = iter(payloads)

    async def complete_json(self, system: str, user: str) -> dict:
        return {"schema_version": "pipeline.v1", **next(self._payloads)}


class _RowsResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def scalars(self):
        return self

    def all(self) -> list[object]:
        return self._rows

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _ServiceSession:
    def __init__(self, episode, query_rows: list[object] | None = None) -> None:
        self.episode = episode
        self.query_rows = query_rows or []
        self.added: list[object] = []
        self.commits = 0
        self.rollbacks = 0

    async def get(self, model, row_id, **kwargs):
        return self.episode if row_id == self.episode.episode_id else None

    async def execute(self, statement):
        return _RowsResult(self.query_rows)

    def add(self, row) -> None:
        self.added.append(row)

    async def flush(self) -> None:
        return None

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _episode(version: int = 3):
    return SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_text="正文",
        episode_version=version,
    )


def _retrying_provider(valid_payload: dict) -> tuple[FakeProvider, list[dict]]:
    payloads = [{"invalid": True}, {"invalid": True}, valid_payload]
    return FakeProvider(payloads), payloads


def _twelve_sections() -> str:
    labels = [
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
    return "\n".join(f"【{label}】有效内容" for label in labels)


def _strict_contract_payloads() -> dict[type, dict]:
    return {
        SplitOutput: {
            "schema_version": "pipeline.v1",
            "episodes": [{"episode_no": 1, "title": "第一集", "script_text": "正文"}],
        },
        ExtractOutput: {
            "schema_version": "pipeline.v1",
            "subjects": [
                                {
                                    "name": "主角",
                                    "type": "character",
                                    "description": "红衣角色",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
            ],
        },
        MergeOutput: {
            "schema_version": "pipeline.v1",
            "merged_subjects": [
                {
                    "name": "主角",
                    "type": "character",
                    "description": "红衣角色",
                    "is_child": True,
                    "episode_nos": [1],
                }
            ],
            "total_count": 1,
        },
        EnrichOutput: {
            "schema_version": "pipeline.v1",
            "resolved_prompts": {"final": _twelve_sections()},
        },
        ParentsOutput: {
            "schema_version": "pipeline.v1",
            "parent_subjects": [
                {
                    "name": "主角",
                    "type": "character",
                    "description": "角色集合",
                    "child_count": 1,
                    "child_ids": ["child-1"],
                }
            ],
        },
    }


@pytest.mark.parametrize("schema", list(_strict_contract_payloads()))
def test_pipeline_contracts_require_pipeline_v1_and_forbid_root_extra(schema) -> None:
    payload = _strict_contract_payloads()[schema]
    with pytest.raises(ValidationError):
        schema.model_validate({**payload, "unexpected": True})
    with pytest.raises(ValidationError):
        schema.model_validate({**payload, "schema_version": "pipeline.v2"})


@pytest.mark.parametrize(
    ("schema", "container", "field"),
    [
        (SplitOutput, "episodes", None),
        (ExtractOutput, "subjects", None),
        (MergeOutput, "merged_subjects", None),
        (EnrichOutput, "resolved_prompts", "unexpected"),
        (ParentsOutput, "parent_subjects", None),
    ],
)
def test_pipeline_contracts_forbid_nested_extra(schema, container, field) -> None:
    payload = _strict_contract_payloads()[schema]
    if isinstance(payload[container], list):
        payload[container][0]["unexpected"] = True
    else:
        payload[container][field] = True
    with pytest.raises(ValidationError):
        schema.model_validate(payload)


@pytest.mark.parametrize(
    ("schema", "mutate"),
    [
        (SplitOutput, lambda p: p["episodes"][0].update(episode_no="1")),
        (ExtractOutput, lambda p: p["subjects"][0].update(is_child="true")),
        (MergeOutput, lambda p: p.update(total_count="1")),
        (
            EnrichOutput,
            lambda p: p["resolved_prompts"].update(final=_twelve_sections().encode()),
        ),
        (ParentsOutput, lambda p: p["parent_subjects"][0].update(child_count="1")),
    ],
)
def test_pipeline_contracts_reject_type_coercion(schema, mutate) -> None:
    payload = _strict_contract_payloads()[schema]
    mutate(payload)
    with pytest.raises(ValidationError):
        schema.model_validate(payload)


def test_pipeline_contracts_reject_whitespace_gaps_and_duplicates() -> None:
    split_payload = _strict_contract_payloads()[SplitOutput]
    split_payload["episodes"] = [
        {"episode_no": 1, "title": "第一集", "script_text": "正文"},
        {"episode_no": 3, "title": "第三集", "script_text": "正文"},
    ]
    with pytest.raises(ValidationError):
        SplitOutput.model_validate(split_payload)

    extract_payload = _strict_contract_payloads()[ExtractOutput]
    extract_payload["subjects"][0]["name"] = "   "
    with pytest.raises(ValidationError):
        ExtractOutput.model_validate(extract_payload)

    duplicate_episodes = _strict_contract_payloads()[ExtractOutput]
    duplicate_episodes["subjects"][0]["episode_nos"] = [1, 1]
    with pytest.raises(ValidationError):
        ExtractOutput.model_validate(duplicate_episodes)

    missing_required_boolean = _strict_contract_payloads()[ExtractOutput]
    missing_required_boolean["subjects"][0].pop("is_child")
    with pytest.raises(ValidationError):
        ExtractOutput.model_validate(missing_required_boolean)

    duplicate_subjects = _strict_contract_payloads()[MergeOutput]
    duplicate_subjects["merged_subjects"].append(
        duplicate_subjects["merged_subjects"][0].copy()
    )
    duplicate_subjects["total_count"] = 2
    with pytest.raises(ValidationError):
        MergeOutput.model_validate(duplicate_subjects)


@pytest.mark.parametrize("mode", ["missing", "extra"])
def test_merge_rejects_incomplete_or_extra_subject_keyset(monkeypatch, mode: str) -> None:
    extract_stage = SimpleNamespace(
        detail={
            "episodes": [
                {
                    "episode_no": 1,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "主角描述",
                            "episode_nos": [1],
                        },
                        {
                            "name": "场景",
                            "type": "scene",
                            "description": "场景描述",
                            "episode_nos": [1],
                        },
                    ],
                }
            ]
        }
    )
    subjects = [
        {
            "name": "主角",
            "type": "character",
            "description": "主角描述",
            "is_child": True,
            "episode_nos": [1],
        }
    ]
    if mode == "extra":
        subjects.extend(
            [
                {
                    "name": "场景",
                    "type": "scene",
                    "description": "场景描述",
                    "is_child": True,
                    "episode_nos": [1],
                },
                {
                    "name": "额外道具",
                    "type": "prop",
                    "description": "额外描述",
                    "is_child": True,
                    "episode_nos": [1],
                },
            ]
        )
    replace = AsyncMock()
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=extract_stage))
    monkeypatch.setattr(merge, "replace_subjects", replace)

    async def scenario() -> None:
        with pytest.raises(pipeline_contracts.ProviderContractError):
            await merge.run_merge(
                _ServiceSession(_episode()),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=FakeProvider(
                    [
                        {
                            "schema_version": "pipeline.v1",
                            "merged_subjects": subjects,
                            "total_count": len(subjects),
                        }
                    ]
                ),
                provider_attempt=3,
            )
        replace.assert_not_awaited()

    asyncio.run(scenario())


def test_merge_keyset_failure_uses_remaining_durable_budget(monkeypatch) -> None:
    extract_stage = SimpleNamespace(
        detail={
            "episodes": [
                {
                    "episode_no": 1,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "主角描述",
                            "episode_nos": [1],
                        }
                    ],
                }
            ]
        }
    )
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=extract_stage))
    monkeypatch.setattr(merge, "replace_subjects", AsyncMock())

    async def scenario() -> None:
        with pytest.raises(pipeline_contracts.RetryableProviderContractError):
            await merge.run_merge(
                _ServiceSession(_episode()),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=FakeProvider(
                    [{"merged_subjects": [], "total_count": 0}]
                ),
                provider_attempt=1,
            )

    asyncio.run(scenario())


def test_provider_budget_combines_schema_and_transport_across_durable_attempts(
    monkeypatch,
) -> None:
    class MixedFailureProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def complete_json(self, system: str, user: str) -> dict:
            self.calls += 1
            if self.calls == 2:
                raise ConnectionError("Authorization=secret")
            return {"invalid": True}

    provider = MixedFailureProvider()
    monkeypatch.setattr(
        merge, "get_stage", AsyncMock(return_value=SimpleNamespace(detail={"episodes": []}))
    )
    monkeypatch.setattr(merge, "replace_subjects", AsyncMock())

    async def scenario() -> None:
        for attempt in (1, 2):
            with pytest.raises(ConnectionError):
                await merge.run_merge(
                    _ServiceSession(_episode()),
                    "script-1",
                    episode_id="episode-1",
                    input_version=3,
                    provider=provider,
                    provider_attempt=attempt,
                )
        with pytest.raises(pipeline_contracts.ProviderContractError) as exc_info:
            await merge.run_merge(
                _ServiceSession(_episode()),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=provider,
                provider_attempt=3,
            )
        failure = PipelineFailure.from_exception(exc_info.value)
        assert failure.error_type == "provider_contract_error"
        assert failure.retryable is False
        assert provider.calls == 3

    asyncio.run(scenario())


def test_fake_provider_payloads_validate_all_five_pipeline_contracts() -> None:
    provider = FakeProvider(
        [
            {"episodes": [{"episode_no": 1, "title": "第一集", "script_text": "正文"}]},
            {
                "subjects": [
                    {
                        "name": "主角",
                        "type": "character",
                        "description": "红衣角色",
                        "is_child": True,
                        "episode_nos": [1],
                    }
                ]
            },
            {
                "merged_subjects": [
                    {
                        "name": "主角",
                        "type": "character",
                        "description": "红衣角色",
                        "is_child": True,
                        "episode_nos": [1],
                    }
                ],
                "total_count": 1,
            },
            {"resolved_prompts": {"final": _twelve_sections()}},
            {
                "parent_subjects": [
                    {
                        "name": "主角",
                        "type": "character",
                        "description": "角色集合",
                        "child_count": 1,
                        "child_ids": ["child-1"],
                    }
                ]
            },
        ]
    )

    async def validate_payloads() -> None:
        assert SplitOutput.model_validate(await provider.complete_json("", "")).episodes[0].episode_no == 1
        assert ExtractOutput.model_validate(await provider.complete_json("", "")).subjects[0].name == "主角"
        assert MergeOutput.model_validate(await provider.complete_json("", "")).total_count == 1
        assert EnrichOutput.model_validate(await provider.complete_json("", "")).resolved_prompts.final
        assert ParentsOutput.model_validate(await provider.complete_json("", "")).parent_subjects[0].child_count == 1

    asyncio.run(validate_payloads())


def test_invalid_prompt_and_sound_subject_are_not_visual_enrichment() -> None:
    with pytest.raises(ValidationError):
        EnrichOutput.model_validate(
            {
                "schema_version": "pipeline.v1",
                "resolved_prompts": {"final": "【形制描述】只有一段"},
            }
        )
    assert not should_enrich_subject("sound")
    assert should_enrich_subject("character")


def test_pipeline_metadata_parent_fk_attempt_uniqueness_reset_and_failure_shape() -> None:
    parent_fk = next(iter(Subject.__table__.c.parent_id.foreign_keys))
    assert parent_fk.target_fullname == "parent_subjects.id"
    assert any(
        {column.name for column in constraint.columns} == {"run_id", "stage", "attempt"}
        for constraint in PipelineJob.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    )
    assert {
        "dispatch_id",
        "run_id",
        "error_type",
        "error_message",
        "retryable",
        "input_client_save_seq",
        "started_at",
        "finished_at",
    } <= {
        column.name for column in PipelineJob.__table__.columns
    }
    job = PipelineJob(lease_until=datetime(2026, 1, 1, tzinfo=UTC))
    assert job.lease_expires_at == job.lease_until
    job.lease_expires_at = datetime(2026, 1, 2, tzinfo=UTC)
    assert job.lease_until == datetime(2026, 1, 2, tzinfo=UTC)

    snapshot = StageSnapshot(status="failed", detail={"old": True}, current=4, total=9, error={"x": 1})
    reset_stage(snapshot)
    assert snapshot == StageSnapshot(status="pending", detail=None, current=None, total=None, error=None)

    failure = PipelineFailure.from_exception(ValueError("bad provider response token=secret"))
    assert failure.error_type == "invalid_pipeline_data"
    assert not failure.retryable
    assert failure.to_dict() == {
        "error_type": "invalid_pipeline_data",
        "error_message": "流水线数据无效",
        "retryable": False,
    }

    timeout = PipelineFailure.from_exception(TimeoutError("credential=secret"))
    assert timeout.to_dict() == {
        "error_type": "provider_timeout",
        "error_message": "上游服务超时",
        "retryable": True,
    }


def test_late_pipeline_input_version_is_never_current() -> None:
    assert is_current_input_version(5, 5)
    assert not is_current_input_version(4, 5)
    assert not is_current_input_version(6, 5)


def test_pipeline_input_fence_requires_episode_version_and_client_save_seq() -> None:
    class _InputDb:
        async def get(self, model, row_id, **kwargs):
            return SimpleNamespace(
                episode_id=row_id,
                script_id="script-1",
                episode_version=3,
                client_save_seq=9,
            )

    async def scenario() -> None:
        db = _InputDb()
        assert await input_episode_is_current(
            db,
            "script-1",
            "episode-1",
            3,
            input_client_save_seq=9,
        )
        assert not await input_episode_is_current(
            db,
            "script-1",
            "episode-1",
            3,
            input_client_save_seq=8,
        )

    asyncio.run(scenario())


def test_active_pipeline_runs_are_superseded_before_latest_payload_is_enqueued(
    monkeypatch,
) -> None:
    class _Rows:
        def __init__(self, rows: list[object]) -> None:
            self.rows = rows

        def scalars(self):
            return self

        def all(self) -> list[object]:
            return self.rows

    class _SupersedeDb:
        def __init__(self) -> None:
            self.calls = 0
            self.flushed = False

        async def execute(self, statement):
            self.calls += 1
            if self.calls == 1:
                return _Rows(
                    [
                        SimpleNamespace(
                            run_id="run-old",
                            status="running",
                            error_type=None,
                            error_message=None,
                            retryable=None,
                            lease_owner="worker-old",
                            lease_token="token-old",
                            lease_until=object(),
                            finished_at=None,
                        )
                    ]
                )
            return _Rows(
                [
                    SimpleNamespace(
                        id="reservation-old",
                        source_type="pipeline",
                        source_id="run-old",
                        status="reserved",
                    )
                ]
            )

        async def flush(self) -> None:
            self.flushed = True

    released: list[tuple[str, str]] = []

    async def fake_release(db, reservation_id, reason, *, commit):
        released.append((reservation_id, reason))

    monkeypatch.setattr(orchestrator, "credits_release", fake_release)

    async def scenario() -> None:
        db = _SupersedeDb()
        assert await orchestrator._supersede_active_pipeline_runs(db, "script-1") == 1
        assert db.flushed
        assert released == [("reservation-old", "superseded by newer input")]

        payload = orchestrator.build_pipeline_outbox_payload(
            script_id="script-1",
            episode_id="episode-1",
            input_version=4,
            input_client_save_seq=10,
            reservation_id="reservation-new",
            estimated=12,
        )
        assert payload["input_version"] == 4
        assert payload["input_client_save_seq"] == 10
        assert orchestrator.PIPELINE_DAG_TASK_TYPE == "pipeline.dag"

    asyncio.run(scenario())


def test_trigger_pipeline_supersedes_running_run_and_schedules_latest_input(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        class _FakeOutbox:
            id = "dispatch-new"

        class _TriggerDb:
            def __init__(self) -> None:
                self.added: list[object] = []
                self.commits = 0
                self.exec_calls = 0

            async def execute(self, statement):
                self.exec_calls += 1
                account = SimpleNamespace(id="account-1")
                return SimpleNamespace(
                    scalar_one_or_none=lambda: account if self.exec_calls > 1 else None,
                    scalar=lambda: 0,
                )

            async def get(self, model, row_id, **kwargs):
                if model is Episode:
                    return SimpleNamespace(episode_version=4, client_save_seq=10)
                if model is Script:
                    return SimpleNamespace(owner_user_id="user-1")
                return None

            def add(self, row) -> None:
                self.added.append(row)

            async def flush(self) -> None:
                return None

            async def commit(self) -> None:
                self.commits += 1

        db = _TriggerDb()
        monkeypatch.setattr(orchestrator, "_supersede_active_pipeline_runs", AsyncMock(return_value=1))
        monkeypatch.setattr(orchestrator, "_is_running", AsyncMock(return_value=True))
        monkeypatch.setattr(orchestrator, "_estimate_cost", AsyncMock(return_value=12))
        monkeypatch.setattr(orchestrator, "reserve", AsyncMock(return_value="reservation-new"))
        monkeypatch.setattr(orchestrator, "get_stage", AsyncMock(return_value=SimpleNamespace()))
        monkeypatch.setattr(orchestrator, "set_stage", AsyncMock())
        monkeypatch.setattr(
            orchestrator,
            "enqueue_task",
            AsyncMock(return_value=_FakeOutbox()),
        )

        result = await orchestrator.trigger_pipeline(db, "script-1", "episode-1")

        assert result["input_version"] == 4
        assert result["input_client_save_seq"] == 10
        assert result["dispatch_id"] == "dispatch-new"
        jobs = [row for row in db.added if isinstance(row, PipelineJob)]
        assert {job.stage for job in jobs} == set(orchestrator.STAGE_ORDER)
        assert {job.input_version for job in jobs} == {4}
        assert {job.input_client_save_seq for job in jobs} == {10}
        orchestrator._supersede_active_pipeline_runs.assert_awaited_once()
        orchestrator.reserve.assert_awaited_once()
        orchestrator.enqueue_task.assert_awaited_once()
        assert db.commits == 1

    asyncio.run(scenario())


def test_episode_create_rejects_zero_and_negative_episode_numbers() -> None:
    for episode_no in (0, -1):
        with pytest.raises(ValidationError):
            EpisodeCreateIn(title="测试", episode_no=episode_no)


def test_all_five_services_retry_invalid_provider_contracts_before_writing(monkeypatch) -> None:
    async def scenario() -> None:
        stage_writes: list[str] = []

        async def record_stage(db, script_id, stage, **kwargs):
            stage_writes.append(stage)

        episode = _episode()

        split_provider, _ = _retrying_provider(
            {"episodes": [{"episode_no": 1, "title": "第一集", "script_text": "正文一"}]}
        )
        monkeypatch.setattr(split, "set_stage", record_stage)
        assert await split.run_split(
            _ServiceSession(episode),
            "script-1",
            "episode-1",
            input_version=3,
            provider=split_provider,
        ) == 1

        extract_provider, _ = _retrying_provider(
            {
                "subjects": [
                    {
                        "name": "主角",
                        "type": "character",
                        "description": "红衣角色",
                        "is_child": True,
                        "episode_nos": [1],
                    }
                ]
            }
        )
        monkeypatch.setattr(extract, "set_stage", record_stage)
        assert (
            await extract.run_extract(
                _ServiceSession(episode, [episode]),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=extract_provider,
            )
        )["episodes"]

        extract_stage = SimpleNamespace(
            detail={
                "episodes": [
                    {
                        "episode_no": 1,
                        "subjects": [
                            {
                                "name": "主角",
                                "type": "character",
                                "description": "红衣角色",
                                "episode_nos": [1],
                            }
                        ],
                    }
                ]
            }
        )
        merge_provider, _ = _retrying_provider(
            {
                "merged_subjects": [
                    {
                        "name": "主角",
                        "type": "character",
                        "description": "红衣角色",
                        "is_child": True,
                        "episode_nos": [1],
                    }
                ],
                "total_count": 1,
            }
        )
        monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=extract_stage))
        monkeypatch.setattr(merge, "replace_subjects", AsyncMock())
        monkeypatch.setattr(merge, "set_stage", record_stage)
        assert await merge.run_merge(
            _ServiceSession(episode),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=merge_provider,
        ) == 1

        subject = Subject(
            id="subject-1",
            script_id="script-1",
            name="主角",
            type="character",
            description="红衣角色",
            is_child=True,
            is_parent=False,
        )
        enrich_provider, _ = _retrying_provider(
            {"resolved_prompts": {"final": _twelve_sections()}}
        )
        monkeypatch.setattr(enrich, "set_stage", record_stage)
        assert await enrich.run_enrich(
            _ServiceSession(episode, [subject]),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=enrich_provider,
        ) == 1

        parents_provider, _ = _retrying_provider(
            {
                "parent_subjects": [
                    {
                        "name": "主角",
                        "type": "character",
                        "description": "角色集合",
                        "child_count": 1,
                        "child_ids": ["subject-1"],
                    }
                ]
            }
        )
        monkeypatch.setattr(parents, "replace_parent_subjects", AsyncMock())
        monkeypatch.setattr(parents, "set_stage", record_stage)
        assert await parents.run_parents(
            _ServiceSession(episode, [subject]),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=parents_provider,
        ) == 1

        assert stage_writes.count("split") == 2
        assert stage_writes.count("extract") == 2
        assert stage_writes.count("merge") == 1
        assert stage_writes.count("enrich") == 1
        assert stage_writes.count("parents") == 1

    asyncio.run(scenario())


def test_provider_contract_failure_stops_after_three_attempts_without_partial_write(
    monkeypatch,
) -> None:
    provider = FakeProvider([{"invalid": True}] * 3)
    replace = AsyncMock()
    set_stage = AsyncMock()
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=SimpleNamespace(detail={})))
    monkeypatch.setattr(merge, "replace_subjects", replace)
    monkeypatch.setattr(merge, "set_stage", set_stage)

    async def scenario() -> None:
        with pytest.raises(pipeline_contracts.ProviderContractError) as exc_info:
            await merge.run_merge(
                _ServiceSession(_episode()),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=provider,
            )
        failure = PipelineFailure.from_exception(exc_info.value)
        assert failure.to_dict()["error_type"] == "provider_contract_error"
        assert failure.to_dict()["retryable"] is False
        replace.assert_not_awaited()
        set_stage.assert_not_awaited()

    asyncio.run(scenario())


def test_late_episode_version_prevents_real_merge_persistence_and_stage_done(monkeypatch) -> None:
    episode = _episode()

    class LateProvider(FakeProvider):
        async def complete_json(self, system: str, user: str) -> dict:
            payload = await super().complete_json(system, user)
            episode.episode_version += 1
            return payload

    provider = LateProvider([{"merged_subjects": [], "total_count": 0}])
    replace = AsyncMock()
    set_stage = AsyncMock()
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=SimpleNamespace(detail={})))
    monkeypatch.setattr(merge, "replace_subjects", replace)
    monkeypatch.setattr(merge, "set_stage", set_stage)

    async def scenario() -> None:
        assert await merge.run_merge(
            _ServiceSession(episode),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=provider,
        ) == 0
        replace.assert_not_awaited()
        set_stage.assert_not_awaited()

    asyncio.run(scenario())


def test_split_derived_write_preserves_trigger_input_version_for_downstream(monkeypatch) -> None:
    async def scenario() -> None:
        from app.models.episode import Episode
        from app.models.scene import Scene

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Episode.__table__.create(engine)
        Scene.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="原始剧本",
                    script_text="完整正文",
                    schema_version=1,
                    episode_version=3,
                    client_save_seq=0,
                )
            )
            await db.commit()
            monkeypatch.setattr(split, "set_stage", AsyncMock())
            provider = FakeProvider(
                [
                    {
                        "episodes": [
                            {
                                "episode_no": 1,
                                "title": "第一集",
                                "script_text": "拆分后的第一集",
                            }
                        ]
                    }
                ]
            )

            assert await split.run_split(
                db,
                "script-1",
                "episode-1",
                input_version=3,
                provider=provider,
            ) == 1
            refreshed = raw_session.get(Episode, "episode-1")
            assert refreshed.script_text == "拆分后的第一集"
            assert refreshed.episode_version == 3

    asyncio.run(scenario())


def test_split_derived_write_does_not_increment_other_episode_user_version(monkeypatch) -> None:
    async def scenario() -> None:
        from app.models.episode import Episode
        from app.models.scene import Scene

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Episode.__table__.create(engine)
        Scene.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="原始剧本",
                    script_text="完整正文",
                    schema_version=1,
                    episode_version=3,
                    client_save_seq=0,
                )
            )
            db.add(
                Episode(
                    episode_id="episode-2",
                    script_id="script-1",
                    episode_no=2,
                    title="第二集",
                    script_text="用户版本",
                    schema_version=1,
                    episode_version=5,
                    client_save_seq=4,
                )
            )
            await db.commit()
            monkeypatch.setattr(split, "set_stage", AsyncMock())
            provider = FakeProvider(
                [
                    {
                        "episodes": [
                            {"episode_no": 1, "title": "第一集", "script_text": "拆分一"},
                            {"episode_no": 2, "title": "第二集", "script_text": "拆分二"},
                        ]
                    }
                ]
            )

            assert await split.run_split(
                db,
                "script-1",
                "episode-1",
                input_version=3,
                provider=provider,
            ) == 1
            refreshed = raw_session.get(Episode, "episode-2")
            assert refreshed.script_text == "用户版本"
            assert refreshed.episode_version == 5
            assert refreshed.client_save_seq == 4

    asyncio.run(scenario())


def test_extract_and_enrich_leave_commit_to_worker_and_persist_stage_totals(monkeypatch) -> None:
    async def scenario() -> None:
        episode = _episode()
        subject = Subject(
            id="subject-1",
            script_id="script-1",
            name="主角",
            type="character",
            description="红衣角色",
            is_child=True,
            is_parent=False,
        )
        extract_db = _ServiceSession(episode, [episode])
        enrich_db = _ServiceSession(episode, [subject])
        extract_stage = AsyncMock()
        enrich_stage = AsyncMock()
        monkeypatch.setattr(extract, "set_stage", extract_stage)
        monkeypatch.setattr(enrich, "set_stage", enrich_stage)

        await extract.run_extract(
            extract_db,
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=FakeProvider([{"subjects": []}]),
            commit=False,
        )
        await enrich.run_enrich(
            enrich_db,
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=FakeProvider([{"resolved_prompts": {"final": _twelve_sections()}}]),
            commit=False,
        )

        assert extract_db.commits == extract_db.rollbacks == 0
        assert enrich_db.commits == enrich_db.rollbacks == 0
        assert extract_stage.await_args.kwargs["total"] == 1
        assert enrich_stage.await_args.kwargs["total"] == 1
        assert enrich_stage.await_args.kwargs["status"] == "running"

    asyncio.run(scenario())


def test_parents_keeps_stage_running_until_credit_finalizer(monkeypatch) -> None:
    subject = Subject(
        id="subject-1",
        script_id="script-1",
        name="主角",
        type="character",
        description="红衣角色",
        is_child=True,
        is_parent=False,
    )
    stage = AsyncMock()
    monkeypatch.setattr(parents, "replace_parent_subjects", AsyncMock())
    monkeypatch.setattr(parents, "set_stage", stage)

    async def scenario() -> None:
        await parents.run_parents(
            _ServiceSession(_episode(), [subject]),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=FakeProvider(
                [
                    {
                        "parent_subjects": [
                            {
                                "name": "主角",
                                "type": "character",
                                "description": "角色集合",
                                "child_count": 1,
                                "child_ids": ["subject-1"],
                            }
                        ]
                    }
                ]
            ),
            commit=False,
        )
        assert stage.await_args.kwargs["status"] == "running"

    asyncio.run(scenario())


def test_merge_restores_complete_episode_union_from_extract_input(monkeypatch) -> None:
    extract_stage = SimpleNamespace(
        detail={
            "episodes": [
                {
                    "episode_no": 1,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "红衣角色",
                            "episode_nos": [1],
                        }
                    ],
                },
                {
                    "episode_no": 2,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "红衣角色",
                            "episode_nos": [2],
                        }
                    ],
                },
            ]
        }
    )
    replace = AsyncMock()
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=extract_stage))
    monkeypatch.setattr(merge, "replace_subjects", replace)
    monkeypatch.setattr(merge, "set_stage", AsyncMock())

    async def scenario() -> None:
        await merge.run_merge(
            _ServiceSession(_episode()),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=FakeProvider(
                [
                    {
                        "merged_subjects": [
                            {
                                "name": "主角",
                                "type": "character",
                                "description": "红衣角色",
                                "is_child": True,
                                "episode_nos": [1, 2],
                            }
                        ],
                        "total_count": 1,
                    }
                ]
            ),
            commit=False,
        )
        merged = replace.await_args.args[2]
        assert merged[0]["episode_nos"] == [1, 2]

    asyncio.run(scenario())


def test_extract_single_episode_rerun_uses_only_current_episode_and_persists_progress(
    monkeypatch,
) -> None:
    episode_one = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_text="第一集正文",
        episode_version=3,
        client_save_seq=1,
    )
    episode_two = SimpleNamespace(
        episode_id="episode-2",
        script_id="script-1",
        episode_no=2,
        title="第二集",
        script_text="第二集正文",
        episode_version=4,
        client_save_seq=2,
    )
    stage_writes: list[dict] = []

    async def record_stage(db, script_id, stage, **kwargs):
        stage_writes.append(kwargs)

    monkeypatch.setattr(extract, "set_stage", record_stage)
    provider = FakeProvider(
        [
            {
                "subjects": [
                    {
                        "name": "第二集角色",
                        "type": "character",
                        "description": "第二集角色描述",
                        "is_child": True,
                        "episode_nos": [2],
                    }
                ]
            }
        ]
    )

    async def scenario() -> None:
        result = await extract.run_extract(
            _ServiceSession(episode_two, [episode_one, episode_two]),
            "script-1",
            episode_id="episode-2",
            input_version=4,
            input_client_save_seq=2,
            provider=provider,
        )
        assert [row["episode_id"] for row in result["episodes"]] == ["episode-2"]
        assert stage_writes[0]["status"] == "running"
        assert stage_writes[0]["current"] == 1
        assert stage_writes[0]["total"] == 1
        assert stage_writes[-1]["status"] == "done"

    asyncio.run(scenario())


def test_extract_rejects_subject_episode_nos_outside_current_episode(monkeypatch) -> None:
    episode_two = SimpleNamespace(
        episode_id="episode-2",
        script_id="script-1",
        episode_no=2,
        title="第二集",
        script_text="第二集正文",
        episode_version=4,
        client_save_seq=2,
    )
    monkeypatch.setattr(extract, "set_stage", AsyncMock())

    async def scenario() -> None:
        with pytest.raises(pipeline_contracts.ProviderContractError):
            await extract.run_extract(
                _ServiceSession(episode_two, [episode_two]),
                "script-1",
                episode_id="episode-2",
                input_version=4,
                input_client_save_seq=2,
                provider=FakeProvider(
                    [
                        {
                            "subjects": [
                                {
                                    "name": "串集角色",
                                    "type": "character",
                                    "description": "错误集号",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
                            ]
                        },
                        {
                            "subjects": [
                                {
                                    "name": "串集角色",
                                    "type": "character",
                                    "description": "错误集号",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
                            ]
                        },
                        {
                            "subjects": [
                                {
                                    "name": "串集角色",
                                    "type": "character",
                                    "description": "错误集号",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
                            ]
                        },
                    ]
                ),
            )

    asyncio.run(scenario())


def test_merge_rejects_subject_episode_nos_not_seen_in_extract(monkeypatch) -> None:
    extract_stage = SimpleNamespace(
        detail={
            "episodes": [
                {
                    "episode_no": 1,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "红衣角色",
                            "episode_nos": [1],
                        }
                    ],
                }
            ]
        }
    )
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=extract_stage))
    monkeypatch.setattr(merge, "replace_subjects", AsyncMock())

    async def scenario() -> None:
        with pytest.raises(pipeline_contracts.ProviderContractError):
            await merge.run_merge(
                _ServiceSession(_episode()),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=FakeProvider(
                    [
                        {
                            "merged_subjects": [
                                {
                                    "name": "主角",
                                    "type": "character",
                                    "description": "红衣角色",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
                            ],
                            "total_count": 1,
                        },
                        {
                            "merged_subjects": [
                                {
                                    "name": "主角",
                                    "type": "character",
                                    "description": "红衣角色",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
                            ],
                            "total_count": 1,
                        },
                        {
                            "merged_subjects": [
                                {
                                    "name": "主角",
                                    "type": "character",
                                    "description": "红衣角色",
                                    "is_child": True,
                                    "episode_nos": [1, 2],
                                }
                            ],
                            "total_count": 1,
                        },
                    ]
                ),
            )

    asyncio.run(scenario())


def test_parents_rejects_parent_child_type_mismatch(monkeypatch) -> None:
    subject = Subject(
        id="subject-character",
        script_id="script-1",
        name="主角",
        type="character",
        description="角色",
        is_child=True,
        is_parent=False,
    )
    monkeypatch.setattr(parents, "replace_parent_subjects", AsyncMock())

    async def scenario() -> None:
        with pytest.raises(ValueError, match="父子主体类型必须一致"):
            await parents.run_parents(
                _ServiceSession(_episode(), [subject]),
                "script-1",
                episode_id="episode-1",
                input_version=3,
                provider=FakeProvider(
                    [
                        {
                            "parent_subjects": [
                                {
                                    "name": "错误父主体",
                                    "type": "prop",
                                    "description": "类型不一致",
                                    "child_count": 1,
                                    "child_ids": ["subject-character"],
                                }
                            ]
                        }
                    ]
                ),
            )

    asyncio.run(scenario())


def test_generation_rejects_enrich_prompt_when_any_related_episode_seq_changed() -> None:
    async def scenario() -> None:
        from app.models.pipeline_stage import PipelineStage

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Episode.__table__.create(engine)
        PipelineStage.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            raw_session.add_all(
                [
                    Episode(
                        episode_id="episode-1",
                        script_id="script-1",
                        episode_no=1,
                        title="第一集",
                        episode_version=7,
                        client_save_seq=3,
                    ),
                    Episode(
                        episode_id="episode-2",
                        script_id="script-1",
                        episode_no=2,
                        title="第二集",
                        episode_version=5,
                        client_save_seq=8,
                    ),
                    PipelineStage(
                        id="stage-enrich",
                        script_id="script-1",
                        stage="enrich",
                        status="done",
                        detail={
                            "prompts": {
                                "subject-1": {
                                    "resolved_prompts": {"final": _twelve_sections()},
                                    "episode_snapshot": {
                                        "1": {
                                            "episode_id": "episode-1",
                                            "episode_version": 7,
                                            "client_save_seq": 3,
                                        },
                                        "2": {
                                            "episode_id": "episode-2",
                                            "episode_version": 5,
                                            "client_save_seq": 7,
                                        },
                                    },
                                }
                            }
                        },
                    ),
                ]
            )
            raw_session.commit()
            with pytest.raises(ConflictError, match="enrich 快照不是当前输入版本"):
                await generation_service._load_server_resolved_prompt(
                    _AsyncSessionAdapter(raw_session),
                    "script-1",
                    "subject-1",
                    input_version=7,
                    input_client_save_seq=3,
                    episode_snapshot={
                        1: {
                            "episode_id": "episode-1",
                            "episode_version": 7,
                            "client_save_seq": 3,
                        },
                        2: {
                            "episode_id": "episode-2",
                            "episode_version": 5,
                            "client_save_seq": 8,
                        },
                    },
                )

    asyncio.run(scenario())


def test_openai_provider_uses_one_external_call_per_schema_attempt(monkeypatch) -> None:
    fake_openai = ModuleType("openai")
    fake_openai.APIConnectionError = RuntimeError
    fake_openai.APITimeoutError = TimeoutError
    fake_openai.AsyncOpenAI = object
    fake_openai.InternalServerError = RuntimeError
    fake_openai.RateLimitError = RuntimeError
    monkeypatch.setitem(sys.modules, "openai", fake_openai)
    llm_openai = importlib.import_module("app.providers.llm_openai")

    class _Completions:
        def __init__(self) -> None:
            self.calls = 0

        async def create(self, **kwargs):
            self.calls += 1
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="not-json"))]
            )

    completions = _Completions()
    provider = llm_openai.OpenAILLMProvider.__new__(llm_openai.OpenAILLMProvider)
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=completions)
    )
    provider._model = "fake"

    async def scenario() -> None:
        assert await provider.complete_json("system", "user") == {}
        assert completions.calls == 1

    asyncio.run(scenario())


def test_openai_transient_error_maps_to_single_retryable_call(monkeypatch) -> None:
    class FakeTransientError(RuntimeError):
        pass

    fake_openai = ModuleType("openai")
    fake_openai.APIConnectionError = FakeTransientError
    fake_openai.APITimeoutError = FakeTransientError
    fake_openai.AsyncOpenAI = object
    fake_openai.InternalServerError = FakeTransientError
    fake_openai.RateLimitError = FakeTransientError
    monkeypatch.setitem(sys.modules, "openai", fake_openai)
    sys.modules.pop("app.providers.llm_openai", None)
    llm_openai = importlib.import_module("app.providers.llm_openai")

    class _Completions:
        calls = 0

        async def create(self, **kwargs):
            self.calls += 1
            raise FakeTransientError("Authorization=secret")

    completions = _Completions()
    provider = llm_openai.OpenAILLMProvider.__new__(llm_openai.OpenAILLMProvider)
    provider._client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    provider._model = "fake"

    async def scenario() -> None:
        with pytest.raises(ConnectionError, match="LLM 供应商暂不可用") as exc_info:
            await provider.complete_json("system", "user")
        assert exc_info.value.__cause__ is None
        assert completions.calls == 1

    asyncio.run(scenario())


def test_worker_duplicate_stage_passes_context_so_pending_downstream_can_resume(monkeypatch) -> None:
    claims = iter(
        [
            SimpleNamespace(job_id="job-1", lease_token="token-1", attempt=1),
            None,
            SimpleNamespace(job_id="job-2", lease_token="token-2", attempt=1),
        ]
    )
    service = AsyncMock(return_value=1)
    finish = AsyncMock(return_value=True)
    heartbeat = AsyncMock(return_value=True)
    monkeypatch.setattr(pipeline_worker, "claim_pipeline_job", AsyncMock(side_effect=claims))
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", finish)
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", heartbeat)
    monkeypatch.setattr(
        pipeline_worker,
        "get_pipeline_job_status",
        AsyncMock(return_value="done"),
    )
    monkeypatch.setattr(pipeline_worker, "_resolve_service", lambda stage: service)

    class SessionContext:
        async def __aenter__(self):
            return _ServiceSession(_episode())

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    async def scenario() -> None:
        args = (
            "merge",
            "script-1",
            "episode-1",
            (),
            3,
            None,
            1,
            "dispatch-1",
            "run-1",
        )
        assert await pipeline_worker._run_stage_async(*args) == "script-1"
        duplicate_context = await pipeline_worker._run_stage_async(*args)
        assert duplicate_context == "script-1"
        propagated = await pipeline_worker._run_stage_async(
            "extract",
            duplicate_context,
            "episode-1",
            (),
            3,
            None,
            1,
            "dispatch-1",
            "run-1",
        )
        assert propagated == "script-1"
        assert service.await_count == 2
        assert finish.await_count == 2

    asyncio.run(scenario())


def test_worker_duplicate_running_stage_does_not_report_success_to_chord(monkeypatch) -> None:
    class SessionContext:
        async def __aenter__(self):
            return _ServiceSession(_episode())

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        pipeline_worker, "claim_pipeline_job", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        pipeline_worker,
        "get_pipeline_job_status",
        AsyncMock(return_value="running"),
    )
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    with pytest.raises(pipeline_worker.PipelineStageBusy):
        asyncio.run(
            pipeline_worker._run_stage_async(
                "enrich",
                "script-1",
                "episode-1",
                (),
                3,
                "reservation-1",
                3,
                "dispatch-1",
                "run-1",
            )
        )


def test_worker_stale_input_rolls_back_skip_when_credit_release_fails(monkeypatch) -> None:
    sessions: list[_ServiceSession] = []
    finish_sessions: list[_ServiceSession] = []
    release_sessions: list[_ServiceSession] = []

    class SessionContext:
        async def __aenter__(self):
            self.db = _ServiceSession(_episode())
            sessions.append(self.db)
            return self.db

        async def __aexit__(self, exc_type, exc, tb):
            if exc_type is not None:
                await self.db.rollback()
            return False

    async def finish(db, *args, **kwargs):
        finish_sessions.append(db)
        return True

    async def release(db, reservation_id, reason):
        release_sessions.append(db)
        assert db.commits == 0
        raise OSError("credit storage unavailable")

    monkeypatch.setattr(
        pipeline_worker,
        "claim_pipeline_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1", lease_token="token-1", attempt=1
            )
        ),
    )
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", finish)
    monkeypatch.setattr(
        pipeline_worker, "_input_is_current", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(pipeline_worker, "_reset_owned_running_stage", AsyncMock())
    monkeypatch.setattr(pipeline_worker, "_release_pipeline", release)
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    with pytest.raises(OSError, match="credit storage unavailable"):
        asyncio.run(
            pipeline_worker._run_stage_async(
                "merge",
                "script-1",
                "episode-1",
                (),
                3,
                "reservation-1",
                1,
                "dispatch-1",
                "run-1",
            )
        )

    assert len(sessions) == 2
    assert finish_sessions == release_sessions == [sessions[1]]
    assert sessions[1].commits == 0
    assert sessions[1].rollbacks == 1


def test_worker_stale_input_lost_lease_does_not_release_credit(monkeypatch) -> None:
    sessions: list[_ServiceSession] = []

    class SessionContext:
        async def __aenter__(self):
            self.db = _ServiceSession(_episode())
            sessions.append(self.db)
            return self.db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    release = AsyncMock()
    reset_stage = AsyncMock()
    monkeypatch.setattr(
        pipeline_worker,
        "claim_pipeline_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1", lease_token="stale-token", attempt=1
            )
        ),
    )
    monkeypatch.setattr(
        pipeline_worker, "finish_pipeline_job", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        pipeline_worker, "_input_is_current", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(pipeline_worker, "_reset_owned_running_stage", reset_stage)
    monkeypatch.setattr(pipeline_worker, "_release_pipeline", release)
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    with pytest.raises(pipeline_worker.PipelineStageBusy, match="租约已被接管"):
        asyncio.run(
            pipeline_worker._run_stage_async(
                "merge",
                "script-1",
                "episode-1",
                (),
                3,
                "reservation-1",
                1,
                "dispatch-1",
                "run-1",
            )
        )

    release.assert_not_awaited()
    reset_stage.assert_not_awaited()
    assert sessions[1].rollbacks == 1


def test_worker_newly_stale_input_commits_skip_and_credit_release_together(
    monkeypatch,
) -> None:
    sessions: list[_ServiceSession] = []
    finish_sessions: list[_ServiceSession] = []
    release_sessions: list[_ServiceSession] = []

    class SessionContext:
        async def __aenter__(self):
            self.db = _ServiceSession(_episode())
            sessions.append(self.db)
            return self.db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def finish(db, *args, **kwargs):
        finish_sessions.append(db)
        return True

    async def release(db, reservation_id, reason):
        release_sessions.append(db)

    monkeypatch.setattr(
        pipeline_worker,
        "claim_pipeline_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1", lease_token="token-1", attempt=1
            )
        ),
    )
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", finish)
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", AsyncMock())
    monkeypatch.setattr(
        pipeline_worker,
        "_input_is_current",
        AsyncMock(side_effect=[True, False]),
    )
    monkeypatch.setattr(pipeline_worker, "_mark_stage", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_reset_owned_running_stage", AsyncMock())
    monkeypatch.setattr(pipeline_worker, "_release_pipeline", release)
    monkeypatch.setattr(
        pipeline_worker, "_resolve_service", lambda name: AsyncMock(return_value=None)
    )
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    assert (
        asyncio.run(
            pipeline_worker._run_stage_async(
                "merge",
                "script-1",
                "episode-1",
                (),
                3,
                "reservation-1",
                1,
                "dispatch-1",
                "run-1",
            )
        )
        == "script-1"
    )

    assert len(sessions) == 4
    assert finish_sessions == release_sessions == [sessions[3]]
    assert sessions[2].rollbacks == 1
    assert sessions[3].commits == 1


def test_worker_late_version_resets_only_its_running_stage(monkeypatch) -> None:
    episode = _episode()
    stage = SimpleNamespace(
        status="pending",
        detail=None,
        current=1,
        total=2,
        error={"old": True},
    )
    db = _ServiceSession(episode, [stage])
    claim = SimpleNamespace(
        job_id="job-1",
        lease_token="token-1",
        script_id="script-1",
        episode_id="episode-1",
        stage="merge",
        dispatch_id="dispatch-1",
        run_id="run-1",
        input_version=3,
        attempt=1,
    )

    async def late_service(*args, **kwargs):
        episode.episode_version += 1

    monkeypatch.setattr(pipeline_worker, "claim_pipeline_job", AsyncMock(return_value=claim))
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_resolve_service", lambda name: late_service)

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    async def scenario() -> None:
        assert await pipeline_worker._run_stage_async(
            "merge",
            "script-1",
            "episode-1",
            (),
            3,
            None,
            1,
            "dispatch-1",
            "run-1",
        ) == "script-1"
        assert stage.status == "pending"
        assert stage.detail is None
        assert stage.current is None
        assert stage.total is None
        assert stage.error is None

    asyncio.run(scenario())


def test_worker_retryable_failure_requeues_without_releasing_reservation(monkeypatch) -> None:
    episode = _episode()
    stage = SimpleNamespace(
        status="pending",
        detail=None,
        current=None,
        total=None,
        error=None,
    )
    db = _ServiceSession(episode, [stage])
    claim = SimpleNamespace(
        job_id="job-1",
        lease_token="token-1",
        script_id="script-1",
        episode_id="episode-1",
        stage="merge",
        dispatch_id="dispatch-1",
        run_id="run-1",
        input_version=3,
        attempt=1,
    )
    finish = AsyncMock(return_value=True)
    release = AsyncMock()

    async def unavailable_service(*args, **kwargs):
        raise TimeoutError("provider timeout")

    monkeypatch.setattr(pipeline_worker, "claim_pipeline_job", AsyncMock(return_value=claim))
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", finish)
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_release_pipeline", release)
    monkeypatch.setattr(pipeline_worker, "_resolve_service", lambda name: unavailable_service)

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    async def scenario() -> None:
        with pytest.raises(TimeoutError):
            await pipeline_worker._run_stage_async(
                "merge",
                "script-1",
                "episode-1",
                (),
                3,
                "reservation-1",
                1,
                "dispatch-1",
                "run-1",
                retry_exhausted=False,
            )
        assert finish.await_args.args[3] == "failed"
        assert stage.status == "pending"
        retries = [row for row in db.added if isinstance(row, PipelineJob)]
        assert len(retries) == 1
        assert retries[0].attempt == 2
        assert retries[0].status == "pending"
        release.assert_not_awaited()

    asyncio.run(scenario())


def test_worker_durable_attempt_budget_stops_republished_transient_failure(monkeypatch) -> None:
    episode = _episode()
    stage = SimpleNamespace(
        status="pending",
        detail=None,
        current=None,
        total=None,
        error=None,
    )
    db = _ServiceSession(episode, [stage])
    claim = SimpleNamespace(
        job_id="job-attempt-3",
        lease_token="token-3",
        script_id="script-1",
        episode_id="episode-1",
        stage="merge",
        dispatch_id="dispatch-1",
        run_id="run-1",
        input_version=3,
        attempt=3,
    )
    release = AsyncMock()

    async def unavailable_service(*args, **kwargs):
        raise TimeoutError("provider timeout")

    monkeypatch.setattr(pipeline_worker, "claim_pipeline_job", AsyncMock(return_value=claim))
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_release_pipeline", release)
    monkeypatch.setattr(pipeline_worker, "_resolve_service", lambda name: unavailable_service)

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    async def scenario() -> None:
        with pytest.raises(TimeoutError):
            await pipeline_worker._run_stage_async(
                "merge",
                "script-1",
                "episode-1",
                (),
                3,
                "reservation-1",
                1,
                "dispatch-1",
                "run-1",
                retry_exhausted=False,
            )
        retries = [row for row in db.added if isinstance(row, PipelineJob)]
        assert retries == []
        assert stage.status == "failed"
        release.assert_awaited_once()

    asyncio.run(scenario())


def test_pipeline_tasks_autoretry_transient_failures_at_most_twice() -> None:
    for task in (
        pipeline_worker.split_task,
        pipeline_worker.extract_task,
        pipeline_worker.merge_task,
        pipeline_worker.enrich_task,
        pipeline_worker.parents_task,
    ):
        assert TimeoutError in task.autoretry_for
        assert task.max_retries == 2


@pytest.mark.parametrize("actual", [0, 7])
def test_pipeline_settle_uses_actual_without_estimate_cap(monkeypatch, actual: int) -> None:
    settle = AsyncMock()
    monkeypatch.setattr(pipeline_worker, "_compute_actual", AsyncMock(return_value=actual))
    monkeypatch.setattr(pipeline_worker, "credits_settle", settle)

    asyncio.run(
        pipeline_worker._settle_pipeline(
            _ServiceSession(_episode()), "script-1", "reservation-1", estimated=3
        )
    )

    assert settle.await_args.kwargs == {
        "reservation_id": "reservation-1",
        "actual": actual,
        "commit": False,
    }


def test_worker_service_writes_and_job_done_cas_share_one_transaction(monkeypatch) -> None:
    sessions: list[_ServiceSession] = []
    service_db: list[_ServiceSession] = []
    finish_db: list[_ServiceSession] = []

    class SessionContext:
        async def __aenter__(self):
            db = _ServiceSession(_episode())
            sessions.append(db)
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def service(db, *args, **kwargs):
        service_db.append(db)
        assert kwargs["commit"] is False

    async def finish(db, *args, **kwargs):
        finish_db.append(db)
        return True

    monkeypatch.setattr(
        pipeline_worker,
        "claim_pipeline_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1", lease_token="token-1", attempt=1
            )
        ),
    )
    monkeypatch.setattr(pipeline_worker, "finish_pipeline_job", finish)
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_mark_stage", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_input_is_current", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_resolve_service", lambda name: service)
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    asyncio.run(
        pipeline_worker._run_stage_async(
            "merge",
            "script-1",
            "episode-1",
            (),
            3,
            None,
            1,
            "dispatch-1",
            "run-1",
        )
    )

    assert service_db == finish_db
    assert service_db[0].commits == 1


def test_old_lease_token_rolls_back_service_business_writes(monkeypatch) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    PipelineJob.__table__.create(engine)

    class SessionContext:
        def __init__(self) -> None:
            self.raw_session = Session(engine, expire_on_commit=False)

        async def __aenter__(self):
            return _AsyncSessionAdapter(self.raw_session)

        async def __aexit__(self, exc_type, exc, tb):
            self.raw_session.close()
            return False

    async def service(db, *args, **kwargs):
        assert kwargs["commit"] is False
        db.add(
            PipelineJob(
                id="business-write-that-must-rollback",
                script_id="script-1",
                episode_id="episode-1",
                stage="extract",
                dispatch_id="dispatch-business",
                run_id="run-business",
                status="pending",
                input_version=3,
                attempt=1,
            )
        )
        await db.flush()

    monkeypatch.setattr(
        pipeline_worker,
        "claim_pipeline_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1", lease_token="old-token", attempt=1
            )
        ),
    )
    monkeypatch.setattr(
        pipeline_worker, "finish_pipeline_job", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_mark_stage", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_input_is_current", AsyncMock(return_value=True))
    monkeypatch.setattr(pipeline_worker, "_resolve_service", lambda name: service)
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    asyncio.run(
        pipeline_worker._run_stage_async(
            "merge",
            "script-1",
            "episode-1",
            (),
            3,
            None,
            1,
            "dispatch-1",
            "run-1",
        )
    )

    with Session(engine) as verification_session:
        assert (
            verification_session.get(
                PipelineJob, "business-write-that-must-rollback"
            )
            is None
        )


def test_worker_claim_heartbeat_and_finish_use_database_cas() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        now = datetime.now(UTC)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            rows = [
                PipelineJob(
                    id="job-pending",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="split",
                    dispatch_id="dispatch-1",
                    run_id="run-1",
                    status="pending",
                    input_version=3,
                    attempt=0,
                ),
                PipelineJob(
                    id="job-retry",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="extract",
                    dispatch_id="dispatch-1",
                    run_id="run-1",
                    status="retry",
                    input_version=3,
                    attempt=1,
                ),
                PipelineJob(
                    id="job-expired",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="merge",
                    dispatch_id="dispatch-1",
                    run_id="run-1",
                    status="running",
                    input_version=3,
                    attempt=1,
                    lease_owner="old-worker",
                    lease_token="old-token",
                    lease_until=now - timedelta(seconds=1),
                    heartbeat_at=now - timedelta(minutes=1),
                ),
            ]
            for row in rows:
                db.add(row)
            await db.commit()

            pending = await pipeline_worker.claim_pipeline_job(
                db, "run-1", "split", "worker-1", dispatch_id="dispatch-1"
            )
            retry = await pipeline_worker.claim_pipeline_job(
                db, "run-1", "extract", "worker-1", dispatch_id="dispatch-1"
            )
            reclaimed = await pipeline_worker.claim_pipeline_job(
                db, "run-1", "merge", "worker-2", dispatch_id="dispatch-1"
            )
            assert pending and retry and reclaimed
            def job_value(job_id: str, column):
                return raw_session.execute(
                    select(column).where(PipelineJob.id == job_id)
                ).scalar_one()

            pending_started_at = job_value("job-pending", PipelineJob.started_at)
            retry_started_at = job_value("job-retry", PipelineJob.started_at)
            reclaimed_started_at = job_value(reclaimed.job_id, PipelineJob.started_at)
            assert pending_started_at is not None
            assert retry_started_at is not None
            assert reclaimed_started_at is not None
            assert job_value("job-pending", PipelineJob.attempt) == 0
            assert job_value("job-retry", PipelineJob.attempt) == 1
            assert job_value("job-expired", PipelineJob.attempt) == 1
            assert job_value("job-expired", PipelineJob.status) == "failed"
            assert reclaimed.attempt == 2
            assert reclaimed.lease_token != "old-token"
            assert not await pipeline_worker.heartbeat_pipeline_job(
                db, reclaimed.job_id, "old-token"
            )
            assert await pipeline_worker.heartbeat_pipeline_job(
                db, reclaimed.job_id, reclaimed.lease_token
            )
            assert not await pipeline_worker.finish_pipeline_job(
                db, reclaimed.job_id, "old-token", "done"
            )
            assert await pipeline_worker.finish_pipeline_job(
                db, reclaimed.job_id, reclaimed.lease_token, "done"
            )
            assert (
                await pipeline_worker.claim_pipeline_job(
                    db, "run-1", "merge", "worker-3", dispatch_id="dispatch-1"
                )
                is None
            )
            assert job_value(reclaimed.job_id, PipelineJob.status) == "done"
            assert job_value(reclaimed.job_id, PipelineJob.finished_at) is not None
            assert job_value("job-pending", PipelineJob.finished_at) is None
            assert job_value("job-retry", PipelineJob.finished_at) is None
            assert job_value("job-pending", PipelineJob.started_at) == pending_started_at
            assert job_value("job-retry", PipelineJob.started_at) == retry_started_at
            assert job_value(reclaimed.job_id, PipelineJob.started_at) == reclaimed_started_at

    asyncio.run(scenario())


def test_pipeline_claim_query_uses_skip_locked_only_on_postgresql() -> None:
    query = pipeline_worker.pipeline_claim_query(
        "run-1", "merge", "dispatch-1", datetime.now(UTC)
    )
    postgres_sql = str(
        query.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    sqlite_sql = str(
        query.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert "FOR UPDATE SKIP LOCKED" in postgres_sql
    assert "FOR UPDATE" not in sqlite_sql


def test_explicit_celery_retry_creates_new_immutable_attempt() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        original_finished_at = datetime.now(UTC) - timedelta(minutes=1)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            original = PipelineJob(
                id="job-attempt-1",
                script_id="script-1",
                episode_id="episode-1",
                stage="merge",
                dispatch_id="dispatch-1",
                run_id="run-1",
                status="failed",
                input_version=3,
                attempt=1,
                error_type="provider_timeout",
                error_message="上游服务超时",
                retryable=True,
                finished_at=original_finished_at,
            )
            db.add(original)
            await db.commit()

            retry = await pipeline_worker.create_pipeline_retry_attempt(db, original)
            await db.commit()

            assert retry.id != original.id
            assert retry.attempt == 2
            assert retry.status == "pending"
            assert retry.started_at is None
            assert retry.finished_at is None
            assert original.status == "failed"
            assert original.attempt == 1
            assert original.error_type == "provider_timeout"
            assert original.finished_at == original_finished_at
            attempts = list(
                raw_session.execute(
                    select(PipelineJob.attempt)
                    .where(
                        PipelineJob.run_id == "run-1",
                        PipelineJob.stage == "merge",
                    )
                    .order_by(PipelineJob.attempt)
                ).scalars()
            )
            assert attempts == [1, 2]

    asyncio.run(scenario())


def test_run_scoped_merge_stages_subjects_without_publishing(monkeypatch) -> None:
    async def scenario() -> None:
        extract_output = {
            "episodes": [
                {
                    "episode_no": 1,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "红衣角色",
                            "episode_nos": [1],
                        }
                    ],
                }
            ]
        }
        staged = [
            {
                "id": "subject-staged",
                "name": "主角",
                "type": "character",
                "description": "红衣角色",
                "episode_nos": [1],
                "is_child": True,
                "base_subject_version": None,
                "base_enrich_version": None,
                "subject_version": 1,
                "enrich_version": 1,
            }
        ]
        publish = AsyncMock()
        stage = AsyncMock()
        monkeypatch.setattr(
            merge, "get_run_stage_output", AsyncMock(return_value=extract_output)
        )
        monkeypatch.setattr(
            merge, "_prepare_staged_subjects", AsyncMock(return_value=staged)
        )
        monkeypatch.setattr(merge, "replace_subjects", publish)
        monkeypatch.setattr(merge, "set_stage", stage)

        result = await merge.run_merge(
            _ServiceSession(_episode()),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            run_id="run-1",
            provider=FakeProvider(
                [
                    {
                        "merged_subjects": [
                            {
                                "name": "主角",
                                "type": "character",
                                "description": "红衣角色",
                                "is_child": True,
                                "episode_nos": [1],
                            }
                        ],
                        "total_count": 1,
                    }
                ]
            ),
        )

        assert result == {"count": 1, "subjects": staged}
        publish.assert_not_awaited()
        assert stage.await_args.kwargs["detail"]["count"] == 1

    asyncio.run(scenario())


def test_run_scoped_enrich_keeps_prompts_private_until_finalizer(monkeypatch) -> None:
    async def scenario() -> None:
        episode = SimpleNamespace(
            **vars(_episode()),
            client_save_seq=0,
        )
        merge_output = {
            "subjects": [
                {
                    "id": "subject-1",
                    "name": "主角",
                    "type": "character",
                    "description": "红衣角色",
                    "episode_nos": [1],
                    "subject_version": 4,
                    "enrich_version": 7,
                }
            ]
        }
        stage = AsyncMock()
        monkeypatch.setattr(
            enrich, "get_run_stage_output", AsyncMock(return_value=merge_output)
        )
        monkeypatch.setattr(enrich, "set_stage", stage)

        result = await enrich.run_enrich(
            _ServiceSession(episode, [episode]),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            input_client_save_seq=0,
            run_id="run-1",
            provider=FakeProvider(
                [{"resolved_prompts": {"final": _twelve_sections()}}]
            ),
        )

        assert isinstance(result, dict)
        assert "prompts" not in stage.await_args.kwargs["detail"]
        prompt = result["prompts"]["subject-1"]
        assert prompt["subject_version"] == 4
        assert prompt["enrich_version"] == 7

    asyncio.run(scenario())


def test_run_scoped_parents_stages_without_publishing(monkeypatch) -> None:
    async def scenario() -> None:
        merge_output = {
            "subjects": [
                {
                    "id": "subject-1",
                    "name": "主角",
                    "type": "character",
                    "description": "红衣角色",
                    "episode_nos": [1],
                }
            ]
        }
        publish = AsyncMock()
        monkeypatch.setattr(
            parents, "get_run_stage_output", AsyncMock(return_value=merge_output)
        )
        monkeypatch.setattr(parents, "replace_parent_subjects", publish)
        monkeypatch.setattr(parents, "set_stage", AsyncMock())

        result = await parents.run_parents(
            _ServiceSession(_episode()),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            run_id="run-1",
            provider=FakeProvider(
                [
                    {
                        "parent_subjects": [
                            {
                                "name": "主角",
                                "type": "character",
                                "description": "角色集合",
                                "child_count": 1,
                                "child_ids": ["subject-1"],
                            }
                        ]
                    }
                ]
            ),
        )

        assert isinstance(result, dict)
        assert result["parent_subjects"][0]["child_ids"] == ["subject-1"]
        publish.assert_not_awaited()

    asyncio.run(scenario())


def test_finalizer_settles_once_before_marking_both_leaf_jobs_done(monkeypatch) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                PipelineJob(
                    id="job-merge",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="merge",
                    dispatch_id="dispatch-1",
                    run_id="run-1",
                    status="done",
                    input_version=3,
                    attempt=1,
                    output_json={"subjects": []},
                )
            )
            for stage in ("enrich", "parents"):
                db.add(
                    PipelineJob(
                        id=f"job-{stage}",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage=stage,
                        dispatch_id="dispatch-1",
                        run_id="run-1",
                        status="ready",
                        input_version=3,
                        attempt=1,
                        output_json=(
                            {"prompts": {}}
                            if stage == "enrich"
                            else {"parent_subjects": []}
                        ),
                    )
                )
            await db.commit()
            events: list[str] = []

            async def settle(*args, **kwargs) -> None:
                events.append("settle")

            async def mark_stage(
                db, script_id, episode_id, stage, status, input_version
            ) -> bool:
                events.append(f"stage:{stage}:{status}")
                return True

            monkeypatch.setattr(
                pipeline_worker, "_input_is_current", AsyncMock(return_value=True)
            )
            monkeypatch.setattr(pipeline_worker, "_mark_stage", mark_stage)
            monkeypatch.setattr(pipeline_worker, "_settle_pipeline", settle)
            monkeypatch.setattr(pipeline_worker, "replace_subjects", AsyncMock())
            monkeypatch.setattr(pipeline_worker, "replace_parent_subjects", AsyncMock())

            assert await pipeline_worker.finalize_pipeline_jobs(
                db,
                run_id="run-1",
                script_id="script-1",
                reservation_id="reservation-1",
                estimated=3,
            )
            await db.commit()

            assert events == ["settle", "stage:enrich:done", "stage:parents:done"]
            statuses = dict(
                raw_session.execute(
                    select(PipelineJob.id, PipelineJob.status).where(
                        PipelineJob.run_id == "run-1"
                    )
                ).all()
            )
            assert statuses == {
                "job-merge": "done",
                "job-enrich": "done",
                "job-parents": "done",
            }
            assert all(
                finished_at is not None
                for finished_at in raw_session.execute(
                    select(PipelineJob.finished_at).where(
                        PipelineJob.run_id == "run-1",
                        PipelineJob.stage.in_(["enrich", "parents"]),
                    )
                ).scalars()
            )
            assert not await pipeline_worker.finalize_pipeline_jobs(
                db,
                run_id="run-1",
                script_id="script-1",
                reservation_id="reservation-1",
                estimated=3,
            )
            assert events == ["settle", "stage:enrich:done", "stage:parents:done"]

    asyncio.run(scenario())


def test_finalizer_settlement_failure_never_marks_leaf_jobs_done(monkeypatch) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                PipelineJob(
                    id="job-fail-merge",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="merge",
                    dispatch_id="dispatch-1",
                    run_id="run-fail",
                    status="done",
                    input_version=3,
                    attempt=1,
                    output_json={"subjects": []},
                )
            )
            for stage in ("enrich", "parents"):
                db.add(
                    PipelineJob(
                        id=f"job-fail-{stage}",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage=stage,
                        dispatch_id="dispatch-1",
                        run_id="run-fail",
                        status="ready",
                        input_version=3,
                        attempt=1,
                        output_json=(
                            {"prompts": {}}
                            if stage == "enrich"
                            else {"parent_subjects": []}
                        ),
                    )
                )
            await db.commit()
            monkeypatch.setattr(
                pipeline_worker, "_input_is_current", AsyncMock(return_value=True)
            )
            monkeypatch.setattr(
                pipeline_worker,
                "_settle_pipeline",
                AsyncMock(side_effect=RuntimeError("credit backend secret-response")),
            )
            publish_subjects = AsyncMock()
            publish_parents = AsyncMock()
            monkeypatch.setattr(pipeline_worker, "replace_subjects", publish_subjects)
            monkeypatch.setattr(
                pipeline_worker, "replace_parent_subjects", publish_parents
            )

            with pytest.raises(RuntimeError):
                await pipeline_worker.finalize_pipeline_jobs(
                    db,
                    run_id="run-fail",
                    script_id="script-1",
                    reservation_id="reservation-1",
                    estimated=3,
                )
            await db.rollback()

            statuses = dict(
                raw_session.execute(
                    select(PipelineJob.id, PipelineJob.status).where(
                        PipelineJob.id.in_(["job-fail-enrich", "job-fail-parents"])
                    )
                ).all()
            )
            assert statuses == {
                "job-fail-enrich": "ready",
                "job-fail-parents": "ready",
            }
            publish_subjects.assert_not_awaited()
            publish_parents.assert_not_awaited()

    asyncio.run(scenario())


def test_finalizer_uses_latest_immutable_leaf_attempt(monkeypatch) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                PipelineJob(
                    id="job-merge-latest",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="merge",
                    dispatch_id="dispatch-1",
                    run_id="run-latest",
                    status="done",
                    input_version=3,
                    attempt=1,
                    output_json={"subjects": []},
                )
            )
            for stage in ("enrich", "parents"):
                db.add(
                    PipelineJob(
                        id=f"job-{stage}-old",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage=stage,
                        dispatch_id="dispatch-1",
                        run_id="run-latest",
                        status="done",
                        input_version=3,
                        attempt=1,
                    )
                )
                db.add(
                    PipelineJob(
                        id=f"job-{stage}-latest",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage=stage,
                        dispatch_id="dispatch-1",
                        run_id="run-latest",
                        status="ready",
                        input_version=3,
                        attempt=2,
                        output_json=(
                            {"prompts": {}}
                            if stage == "enrich"
                            else {"parent_subjects": []}
                        ),
                    )
                )
            await db.commit()
            settle = AsyncMock()
            monkeypatch.setattr(
                pipeline_worker, "_input_is_current", AsyncMock(return_value=True)
            )
            monkeypatch.setattr(
                pipeline_worker, "_mark_stage", AsyncMock(return_value=True)
            )
            monkeypatch.setattr(pipeline_worker, "_settle_pipeline", settle)
            monkeypatch.setattr(pipeline_worker, "replace_subjects", AsyncMock())
            monkeypatch.setattr(pipeline_worker, "replace_parent_subjects", AsyncMock())

            assert await pipeline_worker.finalize_pipeline_jobs(
                db,
                run_id="run-latest",
                script_id="script-1",
                reservation_id="reservation-1",
                estimated=3,
            )
            await db.commit()

            settle.assert_awaited_once()
            latest_statuses = raw_session.execute(
                select(PipelineJob.stage, PipelineJob.status).where(
                    PipelineJob.run_id == "run-latest",
                    PipelineJob.attempt == 2,
                )
            ).all()
            assert dict(latest_statuses) == {"enrich": "done", "parents": "done"}

    asyncio.run(scenario())


def test_finalizer_does_not_settle_when_only_one_leaf_is_ready(monkeypatch) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                PipelineJob(
                    id="job-only-enrich",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="enrich",
                    dispatch_id="dispatch-1",
                    run_id="run-one-leaf",
                    status="ready",
                    input_version=3,
                    attempt=1,
                )
            )
            await db.commit()
            settle = AsyncMock()
            monkeypatch.setattr(pipeline_worker, "_settle_pipeline", settle)

            with pytest.raises(RuntimeError, match="leaves are not ready"):
                await pipeline_worker.finalize_pipeline_jobs(
                    db,
                    run_id="run-one-leaf",
                    script_id="script-1",
                    reservation_id="reservation-1",
                    estimated=3,
                )
            settle.assert_not_awaited()

    asyncio.run(scenario())


def test_replace_subjects_preserves_old_subject_referenced_by_asset_entry() -> None:
    reviewed = SimpleNamespace(
        id="subject-reviewed",
        script_id="script-1",
        type="character",
        name="主角",
        description="历史主角",
        is_child=True,
        is_parent=False,
        parent_id=None,
        episode_nos=[1],
        review_status="approved",
        image_entry_id="asset-approved",
        is_active=True,
    )
    archived = SimpleNamespace(
        id="subject-referenced",
        script_id="script-1",
        type="character",
        name="旧角色",
        description="历史角色",
        is_child=True,
        is_parent=False,
        parent_id=None,
        episode_nos=[1],
        review_status="approved",
        image_entry_id="asset-history",
        is_active=True,
    )

    class _ScalarRows(_RowsResult):
        pass

    class _ReplaceSession:
        def __init__(self) -> None:
            self.results = iter(
                [
                    _RowsResult([reviewed, archived]),
                    _ScalarRows([reviewed.id, archived.id]),
                ]
            )
            self.deleted: list[object] = []

        async def execute(self, statement):
            return next(self.results)

        async def delete(self, row) -> None:
            self.deleted.append(row)

        def add(self, row) -> None:
            raise AssertionError("本用例不应新增主体")

        async def flush(self) -> None:
            return None

    db = _ReplaceSession()
    asyncio.run(
        replace_subjects(
            db,
            "script-1",
            [
                {
                    "name": "主角",
                    "type": "character",
                    "description": "新主角描述",
                    "is_child": True,
                    "episode_nos": [1, 2],
                }
            ],
        )
    )
    assert db.deleted == []
    assert reviewed.id == "subject-reviewed"
    assert reviewed.review_status == "approved"
    assert reviewed.image_entry_id == "asset-approved"
    assert reviewed.is_active is True
    assert reviewed.description == "新主角描述"
    assert archived.review_status == "approved"
    assert archived.image_entry_id == "asset-history"
    assert archived.is_active is False


def test_save_episode_does_not_swallow_durable_trigger_failure(monkeypatch) -> None:
    db = _ServiceSession(_episode())
    script = SimpleNamespace(id="script-1")
    saved = _episode(version=4)
    save = AsyncMock(return_value=saved)
    monkeypatch.setattr(scripts_api.script_crud, "get_script", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts_api, "check_script_access", AsyncMock())
    monkeypatch.setattr(scripts_api.script_crud, "save_episode", save)
    monkeypatch.setattr(
        scripts_api,
        "trigger_pipeline",
        AsyncMock(side_effect=RuntimeError("outbox write failed secret=credential")),
    )
    body = SimpleNamespace(
        script_text="新正文",
        episode_version=3,
        client_save_seq=2,
        title="标题",
    )

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="outbox write failed"):
            await scripts_api.save_episode(
                "script-1",
                "episode-1",
                body,
                db,
                SimpleNamespace(user_id="user-1"),
            )

    asyncio.run(scenario())
    assert save.await_args.kwargs["commit"] is False
    assert db.rollbacks == 1


def test_real_enrich_skips_sound_and_persists_only_visual_prompt(monkeypatch) -> None:
    episode = _episode()
    visual = Subject(
        id="subject-visual",
        script_id="script-1",
        name="主角",
        type="character",
        description="红衣角色",
        is_child=True,
        is_parent=False,
    )
    sound = Subject(
        id="subject-sound",
        script_id="script-1",
        name="旁白",
        type="sound",
        description="低沉男声",
        is_child=True,
        is_parent=False,
    )
    provider = FakeProvider([{"resolved_prompts": {"final": _twelve_sections()}}])
    set_stage = AsyncMock()
    monkeypatch.setattr(enrich, "set_stage", set_stage)

    assert asyncio.run(
        enrich.run_enrich(
            _ServiceSession(episode, [visual, sound]),
            "script-1",
            episode_id="episode-1",
            input_version=3,
            provider=provider,
            commit=False,
        )
    ) == 1
    detail = set_stage.await_args.kwargs["detail"]
    assert set(detail["prompts"]) == {"subject-visual"}
    assert set_stage.await_args.kwargs["total"] == 1


def test_breakdown_returns_real_episode_subject_scene_group_and_shot_rows() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            raw_session.add_all(
                [
                    Script(
                        id="script-1",
                        title="测试剧本",
                        scope="personal",
                        owner_user_id="user-1",
                        created_by_user_id="user-1",
                    ),
                    Episode(
                        episode_id="episode-1",
                        script_id="script-1",
                        episode_no=1,
                        title="第一集",
                    ),
                    Episode(
                        episode_id="episode-2",
                        script_id="script-1",
                        episode_no=2,
                        title="第二集",
                    ),
                    Subject(
                        id="subject-1",
                        script_id="script-1",
                        type="character",
                        name="主角",
                        episode_nos=[1],
                        is_active=True,
                    ),
                    Subject(
                        id="subject-other-episode",
                        script_id="script-1",
                        type="character",
                        name="第二集角色",
                        episode_nos=[2],
                        is_active=True,
                    ),
                    Subject(
                        id="subject-archived",
                        script_id="script-1",
                        type="character",
                        name="历史角色",
                        episode_nos=[1],
                        is_active=False,
                    ),
                    Scene(
                        id="scene-1",
                        episode_id="episode-1",
                        scene_no=1,
                        scene_key="bg:室内",
                        background="室内",
                    ),
                    Scene(
                        id="scene-other",
                        episode_id="episode-2",
                        scene_no=1,
                        scene_key="bg:室外",
                        background="室外",
                    ),
                    ShotGroup(
                        shot_group_id="group-1",
                        script_id="script-1",
                        episode_id="episode-1",
                        scene_no=1,
                        group_no="G1",
                    ),
                    ShotGroup(
                        shot_group_id="group-other",
                        script_id="script-1",
                        episode_id="episode-2",
                        scene_no=1,
                        group_no="G1",
                    ),
                        Shot(
                            shot_id="shot-1",
                            shot_group_id="group-1",
                            scene_no=1,
                            group_no="G1",
                            shot_no="S1",
                            display_no=1,
                            content="镜头内容",
                            description="镜头描述",
                            shot_line="镜头台词",
                            start_seconds=0,
                            end_seconds=3,
                        ),
                        Shot(
                            shot_id="shot-other",
                            shot_group_id="group-other",
                            scene_no=1,
                            group_no="G1",
                            shot_no="S1",
                            display_no=1,
                            content="其他集镜头内容",
                            description="其他集镜头描述",
                            shot_line="其他集镜头台词",
                            start_seconds=0,
                            end_seconds=3,
                        ),
                ]
            )
            raw_session.commit()

            data = await scripts_api.script_crud.get_breakdown(
                _AsyncSessionAdapter(raw_session), "script-1", "episode-1"
            )
            assert data["episode"].episode_id == "episode-1"
            assert [row.id for row in data["subjects"]] == ["subject-1"]
            assert [row.id for row in data["scenes"]] == ["scene-1"]
            assert [row.shot_group_id for row in data["shot_groups"]] == ["group-1"]
            assert [row.shot_id for row in data["shots"]] == ["shot-1"]

    asyncio.run(scenario())


def test_archived_subjects_are_excluded_from_enrich_and_parents(monkeypatch) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            raw_session.add_all(
                [
                    Script(
                        id="script-active-subjects",
                        title="测试剧本",
                        scope="personal",
                        owner_user_id="user-1",
                        created_by_user_id="user-1",
                    ),
                    Episode(
                        episode_id="episode-active-subjects",
                        script_id="script-active-subjects",
                        episode_no=1,
                        title="第一集",
                        episode_version=1,
                    ),
                    Subject(
                        id="subject-active",
                        script_id="script-active-subjects",
                        type="character",
                        name="当前主角",
                        description="当前描述",
                        episode_nos=[1],
                        is_active=True,
                    ),
                    Subject(
                        id="subject-archived",
                        script_id="script-active-subjects",
                        type="character",
                        name="历史主角",
                        description="历史描述",
                        episode_nos=[1],
                        is_active=False,
                    ),
                ]
            )
            raw_session.commit()
            db = _AsyncSessionAdapter(raw_session)

            enrich_provider = FakeProvider(
                [{"resolved_prompts": {"final": _twelve_sections()}}]
            )
            assert await enrich.run_enrich(
                db,
                "script-active-subjects",
                episode_id="episode-active-subjects",
                input_version=1,
                provider=enrich_provider,
            ) == 1

            parent_payload = {
                "parent_subjects": [
                    {
                        "name": "当前主角",
                        "type": "character",
                        "description": "当前角色集合",
                        "child_count": 1,
                        "child_ids": ["subject-active"],
                    }
                ]
            }
            assert await parents.run_parents(
                db,
                "script-active-subjects",
                episode_id="episode-active-subjects",
                input_version=1,
                provider=FakeProvider([parent_payload]),
            ) == 1
            raw_session.flush()
            assert raw_session.get(Subject, "subject-active").parent_id is not None
            assert raw_session.get(Subject, "subject-archived").parent_id is None

    asyncio.run(scenario())


def test_heartbeat_renews_for_multiple_periods(monkeypatch) -> None:
    stop = asyncio.Event()
    sessions: list[_ServiceSession] = []
    renewals = 0

    class SessionContext:
        async def __aenter__(self):
            db = _ServiceSession(_episode())
            sessions.append(db)
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def immediate_timeout(awaitable, *, timeout):
        awaitable.close()
        raise TimeoutError

    async def heartbeat(db, job_id, token):
        nonlocal renewals
        renewals += 1
        if renewals == 3:
            stop.set()
        return True

    monkeypatch.setattr(pipeline_worker.asyncio, "wait_for", immediate_timeout)
    monkeypatch.setattr(pipeline_worker, "heartbeat_pipeline_job", heartbeat)
    monkeypatch.setattr(pipeline_worker, "async_session_factory", SessionContext)

    asyncio.run(pipeline_worker._heartbeat_loop("job-1", "token-1", stop))
    assert renewals == 3
    assert [session.commits for session in sessions] == [1, 1, 1]


def test_finalizer_commits_failure_before_release_facility_error(monkeypatch) -> None:
    first = _ServiceSession(_episode())
    leaves = [
        SimpleNamespace(
            status="ready",
            error_type=None,
            error_message=None,
            retryable=None,
            finished_at=None,
        ),
        SimpleNamespace(
            status="ready",
            error_type=None,
            error_message=None,
            retryable=None,
            finished_at=None,
        ),
    ]
    failure_db = _ServiceSession(_episode(), leaves)
    release_db = _ServiceSession(_episode())
    sessions = iter([first, failure_db, release_db])

    class SessionContext:
        def __init__(self, db) -> None:
            self.db = db

        async def __aenter__(self):
            return self.db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        pipeline_worker,
        "async_session_factory",
        lambda: SessionContext(next(sessions)),
    )
    monkeypatch.setattr(
        pipeline_worker,
        "finalize_pipeline_jobs",
        AsyncMock(side_effect=RuntimeError("credit response secret=credential")),
    )
    monkeypatch.setattr(pipeline_worker, "_mark_stage", AsyncMock(return_value=True))
    monkeypatch.setattr(
        pipeline_worker,
        "_release_pipeline",
        AsyncMock(side_effect=OSError("credit storage unavailable secret=credential")),
    )

    with pytest.raises(OSError, match="credit storage unavailable"):
        asyncio.run(
            pipeline_worker._finalize_pipeline_async(
                "script-1",
                "episode-1",
                3,
                "reservation-1",
                3,
                "run-1",
            )
        )
    assert failure_db.commits == 1
    assert all(leaf.status == "failed" for leaf in leaves)
    assert all(leaf.finished_at is not None for leaf in leaves)
    assert all(leaf.error_type == "internal_error" for leaf in leaves)
    assert all("secret" not in leaf.error_message for leaf in leaves)


def test_pipeline_claim_is_exclusive_across_two_sqlite_sessions_and_expires_to_new_attempt(
    tmp_path,
) -> None:
    async def scenario() -> None:
        engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'pipeline-claim.db'}")
        PipelineJob.__table__.create(engine)
        with (
            Session(engine, expire_on_commit=False) as first_raw,
            Session(engine, expire_on_commit=False) as second_raw,
        ):
            first = _AsyncSessionAdapter(first_raw)
            second = _AsyncSessionAdapter(second_raw)
            first.add(
                PipelineJob(
                    id="job-shared",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="merge",
                    dispatch_id="dispatch-1",
                    run_id="run-1",
                    status="pending",
                    input_version=3,
                    attempt=1,
                )
            )
            await first.commit()

            first_claim = await pipeline_worker.claim_pipeline_job(
                first, "run-1", "merge", "worker-1", dispatch_id="dispatch-1"
            )
            assert first_claim is not None
            await first.commit()
            assert (
                await pipeline_worker.claim_pipeline_job(
                    second,
                    "run-1",
                    "merge",
                    "worker-2",
                    dispatch_id="dispatch-1",
                )
                is None
            )

            first_raw.execute(
                update(PipelineJob)
                .where(PipelineJob.id == "job-shared")
                .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
            )
            await first.commit()
            reclaimed = await pipeline_worker.claim_pipeline_job(
                second, "run-1", "merge", "worker-2", dispatch_id="dispatch-1"
            )
            assert reclaimed is not None
            assert reclaimed.attempt == 2
            assert reclaimed.job_id != first_claim.job_id
            assert reclaimed.lease_token != first_claim.lease_token
            assert not await pipeline_worker.finish_pipeline_job(
                second, reclaimed.job_id, first_claim.lease_token, "done"
            )
            await second.commit()
            original_status = first_raw.execute(
                select(PipelineJob.status).where(PipelineJob.id == "job-shared")
            ).scalar_one()
            assert original_status == "failed"

    asyncio.run(scenario())
