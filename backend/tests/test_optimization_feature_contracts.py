"""F06/F10/F16/F18 优化功能的专项契约与状态机测试。"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import BadRequestError, ConflictError
from app.crud import asset as asset_crud
from app.crud import script as script_crud
from app.models.art_style import ArtStyle
from app.models.asset_entry import AssetEntry
from app.models.episode import Episode
from app.models.scene import Scene
from app.models.scene_audition import SceneAudition
from app.models.script import Script
from app.models.subject import Subject
from app.models.subject_candidate import SubjectCandidate
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.schemas.generation import CreateJobIn
from app.services import generation as generation_service
from app.services.art_styles import extract_art_style, get_art_style_definition
from app.services.scene_auditions import (
    claim_queued_audition,
    confirm_audition,
    fail_expired_auditions,
    generate_audition,
    queue_audition,
    run_queued_audition,
)


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    del type_, compiler, kwargs
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

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    async def refresh(self, row) -> None:
        self.session.refresh(row)

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
        Scene.__table__,
        Subject.__table__,
        AssetEntry.__table__,
        SubjectCandidate.__table__,
        ArtStyle.__table__,
        SceneAudition.__table__,
        TaskDispatchOutbox.__table__,
    ):
        table.create(engine)
    return engine, Session(engine, expire_on_commit=False)


def _seed_script(raw: Session) -> SimpleNamespace:
    user = User(
        user_id="user-optimization",
        username="optimization-owner",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    script = Script(
        id="script-optimization",
        title="优化专项剧本",
        scope="personal",
        owner_user_id=user.user_id,
        created_by_user_id=user.user_id,
        visual_style="live_action",
        unify_style=True,
    )
    episode = Episode(
        episode_id="episode-optimization",
        script_id=script.id,
        episode_no=1,
        title="第一集",
        script_text="雨夜，主角推门进入仓库。",
        episode_version=7,
        client_save_seq=3,
    )
    subject = Subject(
        id="subject-optimization",
        script_id=script.id,
        type="character",
        name="主角",
        pipeline_name="主角",
        description="穿深色风衣的调查员",
        visual_style=None,
        episode_nos=[1],
        review_status="not_designed",
    )
    raw.add_all([user, script, episode, subject])
    raw.commit()
    return SimpleNamespace(user=user, script=script, episode=episode, subject=subject)


async def test_subject_candidate_api_state_machine_and_replay(client, monkeypatch) -> None:
    """F06：API 创建、确认、重复确认及版本冲突保持确定性。"""

    from app.api.v1 import subject_candidates as api
    from app.core.auth import get_current_user
    from app.core.database import get_db

    _, raw = _make_db()
    seeded = _seed_script(raw)
    db = _AsyncSessionAdapter(raw)

    async def override_db():
        yield db

    async def override_user():
        yield seeded.user

    client._app.dependency_overrides[get_db] = override_db
    client._app.dependency_overrides[get_current_user] = override_user
    monkeypatch.setattr(api, "audit_log", AsyncMock(return_value=None))
    monkeypatch.setattr(
        api.script_crud,
        "get_script",
        AsyncMock(return_value=seeded.script),
    )

    created = await client.post(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates",
        json={
            "subject_type": "character",
            "name": "  新角色  ",
            "description": "线索提供者",
            "aliases": ["证人", "证人", "  "],
            "episode_nos": [2, 1, 2],
            "evidence": [{"episode_no": 1, "quote": "有人在门外"}],
        },
    )
    assert created.status_code == 201, created.text
    candidate = created.json()
    assert candidate["name"] == "新角色"
    assert candidate["aliases"] == ["证人"]
    assert candidate["episode_nos"] == [1, 2]
    assert candidate["status"] == "pending"
    assert candidate["version"] == 1

    legacy = SubjectCandidate(
        id="candidate-legacy-evidence",
        script_id=seeded.script.id,
        subject_type="prop",
        name="旧候选",
        pipeline_name="旧候选",
        evidence=[
            "  有人在门外  ",
            {"episode_no": 1, "quote": "雨夜，主角推门进入仓库。", "internal": "不得输出"},
            {"internal": "无逐字证据时整项丢弃"},
            7,
        ],
        status="pending",
        version=1,
    )
    raw.add(legacy)
    raw.commit()
    listed = await client.get(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates",
        params={"status": "all"},
    )
    assert listed.status_code == 200, listed.text
    legacy_payload = next(item for item in listed.json()["items"] if item["id"] == legacy.id)
    assert legacy_payload["evidence"] == [
        {"episode_no": None, "quote": "有人在门外"},
        {"episode_no": 1, "quote": "雨夜，主角推门进入仓库。"},
    ]
    assert "不得输出" not in listed.text

    invalid_evidence = await client.post(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates",
        json={
            "subject_type": "prop",
            "name": "不合约候选",
            "evidence": [{"quote": "证据", "internal": "禁止写入"}],
        },
    )
    assert invalid_evidence.status_code == 422

    resolved = await client.post(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates/{candidate['id']}/resolve",
        json={
            "action": "create_subject",
            "expected_version": 1,
            "visual_style": "anime",
        },
    )
    assert resolved.status_code == 200, resolved.text
    linked = resolved.json()
    assert linked["status"] == "linked"
    assert linked["version"] == 2
    assert linked["linked_subject_id"]
    created_subject = raw.get(Subject, linked["linked_subject_id"])
    assert created_subject is not None
    assert created_subject.visual_style == "anime"
    assert created_subject.pipeline_name == "新角色"

    replay = await client.post(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates/{candidate['id']}/resolve",
        json={
            "action": "create_subject",
            "expected_version": 1,
            "visual_style": "anime",
        },
    )
    assert replay.status_code == 200
    assert replay.json()["linked_subject_id"] == linked["linked_subject_id"]
    assert replay.json()["version"] == 2

    conflicting_replay = await client.post(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates/{candidate['id']}/resolve",
        json={"action": "ignore", "expected_version": 1},
    )
    assert conflicting_replay.status_code == 409
    assert "当前请求" in conflicting_replay.json()["detail"]

    stale = await client.post(
        f"/v3api/scripts/{seeded.script.id}/subject-candidates/{candidate['id']}/resolve",
        json={"action": "ignore", "expected_version": 2},
    )
    assert stale.status_code == 409
    assert "已被处理" in stale.json()["detail"]


def test_art_style_extracts_only_authorized_valid_images_and_builds_definition() -> None:
    """F10：只读取当前剧本授权图片，并持久化可直接参与生成的动态定义。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        good = AssetEntry(
            id="asset-good",
            script_id=seeded.script.id,
            subject_id=seeded.subject.id,
            kind="image",
            storage_key="upload_images/material-1/good.png",
            mime_type="image/png",
            size_bytes=8,
            source="upload_images",
            sha256="a" * 64,
            status="approved",
        )
        bad_mime = AssetEntry(
            id="asset-bad-mime",
            script_id=seeded.script.id,
            subject_id=seeded.subject.id,
            kind="image",
            storage_key="upload_images/material-1/not-image.bin",
            mime_type="application/octet-stream",
            size_bytes=8,
            source="upload_images",
            sha256="b" * 64,
            status="candidate",
        )
        foreign = AssetEntry(
            id="asset-foreign",
            script_id="another-script",
            subject_id="another-subject",
            kind="image",
            storage_key="upload_images/material-2/foreign.png",
            mime_type="image/png",
            size_bytes=8,
            source="upload_images",
            sha256="c" * 64,
            status="candidate",
        )
        raw.add_all([good, bad_mime, foreign])
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        storage = SimpleNamespace(get=AsyncMock(return_value=b"png-data"))
        provider = SimpleNamespace(
            provider_name="vision-test",
            model_name="vision-v1",
            extract_style=AsyncMock(
                return_value={
                    "prompt_zh": "克制的冷峻黑色电影风格",
                    "prompt_en": "restrained cold noir visual language",
                    "negative_prompt": "flat lighting",
                    "visual_style": "live_action",
                }
            ),
        )

        row = await extract_art_style(
            db,
            script_id=seeded.script.id,
            name="冷峻黑色电影",
            asset_ids=[good.id],
            actor_user_id=seeded.user.user_id,
            provider=provider,
            storage=storage,
        )
        assert row.source_asset_ids == [good.id]
        assert row.source_hashes == ["a" * 64]
        assert row.thumbnail_storage_key == good.storage_key
        assert row.extraction_provider == "vision-test"
        assert row.extraction_model == "vision-v1"
        storage.get.assert_awaited_once_with(good.storage_key)
        provider.extract_style.assert_awaited_once_with([(b"png-data", "image/png")])

        definition = await get_art_style_definition(
            db,
            script_id=seeded.script.id,
            art_style_id=row.id,
        )
        assert definition == {
            "id": row.id,
            "name_zh": "冷峻黑色电影",
            "target": ["image", "video"],
            "prompt_template": "restrained cold noir visual language, {prompt}",
            "negative_prompt": "flat lighting",
            "default_prompt": "restrained cold noir visual language",
            "best_for": ["项目自定义画风"],
            "visual_style": "live_action",
            "source": "art_style",
        }

        with pytest.raises(BadRequestError, match="不属于当前剧本"):
            await extract_art_style(
                db,
                script_id=seeded.script.id,
                name="越权画风",
                asset_ids=[foreign.id],
                actor_user_id=seeded.user.user_id,
                provider=provider,
                storage=storage,
            )
        with pytest.raises(BadRequestError, match="格式或大小"):
            await extract_art_style(
                db,
                script_id=seeded.script.id,
                name="非法格式",
                asset_ids=[bad_mime.id],
                actor_user_id=seeded.user.user_id,
                provider=provider,
                storage=storage,
            )

    asyncio.run(scenario())


def _audition_plans() -> dict:
    def plan(plan_id: str, name: str) -> dict:
        return {
            "plan_id": plan_id,
            "name": name,
            "audience_knowledge_timing": "开场先让观众看到钥匙",
            "alignment_target": "跟随主角",
            "performance_space": "狭窄仓库入口",
            "strongest_image": "雨水中的钥匙特写",
            "landing": "门在身后合上",
            "tradeoff": "悬念更强但空间交代较晚",
        }

    return {"plans": [plan("plan-a", "悬念优先"), plan("plan-b", "空间优先")]}


def test_scene_audition_outbox_worker_confirmation_and_source_hash_idempotency() -> None:
    """F16：同版本只投递一次，Worker 幂等，确认时同时校验版本与内容哈希。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)

        first = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        replay = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        assert replay.id == first.id
        assert replay.source_hash == first.source_hash
        outboxes = raw.scalars(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.task_type == "scene_audition.generate",
                TaskDispatchOutbox.job_id == first.id,
            )
        ).all()
        assert len(outboxes) == 1
        assert outboxes[0].payload["audition_id"] == first.id
        assert outboxes[0].payload["run_id"] == first.id

        provider = SimpleNamespace(complete_json=AsyncMock(return_value=_audition_plans()))
        ready = await run_queued_audition(db, audition_id=first.id, provider=provider)
        assert ready is not None
        assert ready.status == "ready"
        assert len(ready.plans) == 2
        again = await run_queued_audition(db, audition_id=first.id, provider=provider)
        assert again is ready
        assert provider.complete_json.await_count == 1

        selected = await confirm_audition(
            db,
            audition_id=first.id,
            plan_id="plan-a",
            expected_input_version=7,
            actor_user_id=seeded.user.user_id,
        )
        assert selected.status == "selected"
        assert selected.selected_plan_id == "plan-a"
        selected_replay = await confirm_audition(
            db,
            audition_id=first.id,
            plan_id="plan-a",
            expected_input_version=7,
            actor_user_id=seeded.user.user_id,
        )
        assert selected_replay.selected_plan_id == "plan-a"

        seeded.episode.script_text = "内容在版本号未变化时也被替换。"
        raw.commit()
        with pytest.raises(ConflictError, match="内容已变化"):
            await confirm_audition(
                db,
                audition_id=first.id,
                plan_id="plan-a",
                expected_input_version=7,
                actor_user_id=seeded.user.user_id,
            )
        assert first.status == "stale"

        with pytest.raises(ConflictError, match="分集版本已变化"):
            await queue_audition(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                scene_no=2,
                expected_episode_version=6,
                scene_type="identity_reveal",
            )

    asyncio.run(scenario())


def test_scene_audition_scene_type_change_marks_existing_record_stale() -> None:
    """F16：同一场次版本改换关键类型属于输入变化，不得静默复用或重复投递。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)

        first = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        original_hash = first.source_hash

        changed = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="relationship_reversal",
        )

        assert changed is first
        assert changed.status == "stale"
        assert changed.scene_type == "identity_reveal"
        assert changed.source_hash == original_hash
        assert raw.scalar(select(func.count()).select_from(SceneAudition)) == 1
        outboxes = raw.scalars(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.task_type == "scene_audition.generate",
                TaskDispatchOutbox.job_id == first.id,
            )
        ).all()
        assert len(outboxes) == 1

    asyncio.run(scenario())


def test_scene_audition_sync_generation_does_not_reuse_different_scene_type() -> None:
    """F16：同步兼容入口同样把关键类型纳入指纹，且不会额外调用模型。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)
        first_provider = SimpleNamespace(complete_json=AsyncMock(return_value=_audition_plans()))
        first = await generate_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
            provider=first_provider,
        )
        different_provider = SimpleNamespace(complete_json=AsyncMock(return_value=_audition_plans()))

        changed = await generate_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="climax_ending",
            provider=different_provider,
        )

        assert changed is first
        assert changed.status == "stale"
        assert changed.scene_type == "identity_reveal"
        first_provider.complete_json.assert_awaited_once()
        different_provider.complete_json.assert_not_awaited()
        assert raw.scalar(select(func.count()).select_from(SceneAudition)) == 1

    asyncio.run(scenario())


def test_scene_audition_celery_entry_commits_worker_result(monkeypatch) -> None:
    """F16：Celery 公开任务入口确实复用幂等服务并提交状态。"""

    from app.workers import scene_audition as worker

    db = SimpleNamespace(commit=AsyncMock())

    @asynccontextmanager
    async def session_context():
        yield db

    run = AsyncMock(return_value=SimpleNamespace(id="audition-worker"))
    monkeypatch.setattr(worker, "async_session_factory", session_context)
    claim = AsyncMock(return_value=(SimpleNamespace(id="audition-worker"), True))
    monkeypatch.setattr(worker, "claim_queued_audition", claim)
    monkeypatch.setattr(worker, "run_queued_audition", run)

    assert worker.generate_scene_audition_task.run("audition-worker") == "audition-worker"
    claim.assert_awaited_once_with(
        db,
        audition_id="audition-worker",
        expected_attempt=None,
    )
    run.assert_awaited_once_with(
        db,
        audition_id="audition-worker",
        allow_running=True,
    )
    assert db.commit.await_count == 2


def test_scene_audition_failure_is_safe_and_retry_uses_new_outbox_attempt() -> None:
    """F16：模型异常可安全落库，重排复用记录并生成新的事务 Outbox attempt。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)

        row = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        raw.commit()
        provider_error = "provider leaked secret: sk-sensitive-response"
        failed = await run_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=1,
            provider=SimpleNamespace(
                complete_json=AsyncMock(side_effect=RuntimeError(provider_error))
            ),
        )
        assert failed is row
        assert row.status == "failed"
        assert row.attempt_count == 1
        assert row.error_code == "model_generation_failed"
        assert row.error_message == "导演方案生成失败，请重试"
        assert provider_error not in (row.error_message or "")
        assert row.finished_at is not None
        raw.commit()

        retried = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        replay = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        assert retried is row
        assert replay is row
        assert row.status == "draft"
        assert row.attempt_count == 2
        assert row.error_code is None
        assert row.error_message is None
        attempts = raw.scalars(
            select(TaskDispatchOutbox.attempt)
            .where(
                TaskDispatchOutbox.task_type == "scene_audition.generate",
                TaskDispatchOutbox.job_id == row.id,
            )
            .order_by(TaskDispatchOutbox.attempt)
        ).all()
        assert attempts == [1, 2]

        # 延迟抵达的旧 attempt 不得执行或覆盖新一轮状态。
        old_provider = SimpleNamespace(complete_json=AsyncMock(return_value=_audition_plans()))
        ignored = await run_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=1,
            provider=old_provider,
        )
        assert ignored is row
        old_provider.complete_json.assert_not_awaited()
        assert row.status == "draft"

        ready = await run_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=2,
            provider=SimpleNamespace(complete_json=AsyncMock(return_value=_audition_plans())),
        )
        assert ready is row
        assert row.status == "ready"
        assert len(row.plans) == 2

    asyncio.run(scenario())


def test_scene_audition_rejects_ordinary_scene_without_dispatch() -> None:
    """F16：普通场次不得伪装成关键场次进入多方案和 Outbox 流程。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)

        with pytest.raises(BadRequestError, match="仅人物亮相"):
            await queue_audition(
                db,
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                scene_no=1,
                expected_episode_version=7,
                scene_type="ordinary",
            )
        assert raw.scalar(select(func.count()).select_from(SceneAudition)) == 0
        assert raw.scalar(select(func.count()).select_from(TaskDispatchOutbox)) == 0

    asyncio.run(scenario())


def test_scene_audition_invalid_response_uses_distinct_safe_error() -> None:
    """F16：合约校验失败不保存模型正文，并可由生成接口重试。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)
        row = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        failed = await run_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=1,
            provider=SimpleNamespace(
                complete_json=AsyncMock(
                    return_value={"plans": [{"model_raw": "sensitive response"}]}
                )
            ),
        )
        assert failed is row
        assert row.status == "failed"
        assert row.error_code == "invalid_model_response"
        assert row.error_message == "导演方案返回格式不符合要求，请重试"
        assert "sensitive response" not in (row.error_message or "")
        assert row.plans == []

    asyncio.run(scenario())


def test_scene_audition_worker_forwards_attempt_fence(monkeypatch) -> None:
    """F16：Celery 参数把 Outbox attempt 传给 Worker，隔离迟到的旧消息。"""

    from app.workers import scene_audition as worker

    assert worker.generate_scene_audition_task.acks_late is True
    assert worker.generate_scene_audition_task.reject_on_worker_lost is True
    db = SimpleNamespace(commit=AsyncMock())

    @asynccontextmanager
    async def session_context():
        yield db

    claim = AsyncMock(return_value=(SimpleNamespace(id="audition-worker"), True))
    run = AsyncMock(return_value=SimpleNamespace(id="audition-worker"))
    monkeypatch.setattr(worker, "async_session_factory", session_context)
    monkeypatch.setattr(worker, "claim_queued_audition", claim)
    monkeypatch.setattr(worker, "run_queued_audition", run)

    assert worker.generate_scene_audition_task.run("audition-worker", 2) == "audition-worker"
    run.assert_awaited_once_with(
        db,
        audition_id="audition-worker",
        expected_attempt=2,
        allow_running=True,
    )
    claim.assert_awaited_once_with(
        db,
        audition_id="audition-worker",
        expected_attempt=2,
    )
    assert db.commit.await_count == 2


def test_scene_audition_recovery_task_commits_safe_terminal_state(monkeypatch) -> None:
    """F16：周期入口调用恢复服务并提交终态。"""

    from app.workers import scene_audition as worker

    db = SimpleNamespace(commit=AsyncMock())

    @asynccontextmanager
    async def session_context():
        yield db

    recover = AsyncMock(return_value=2)
    monkeypatch.setattr(worker, "async_session_factory", session_context)
    monkeypatch.setattr(worker, "fail_expired_auditions", recover)

    assert worker.recover_expired_scene_auditions.run() == 2
    recover.assert_awaited_once_with(db)
    db.commit.assert_awaited_once_with()


def test_scene_audition_published_message_timeout_can_requeue() -> None:
    """F16：已发布消息在 Worker 丢失后由生成 API 超时重排，迟到旧消息被 attempt 栅栏拦截。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)
        row = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        claimed, should_generate = await claim_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=1,
        )
        assert claimed is row
        assert should_generate is True
        assert row.status == "running"
        row.started_at = datetime.now(UTC) - timedelta(minutes=11)
        raw.commit()

        retried = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        assert retried is row
        assert row.status == "draft"
        assert row.attempt_count == 2
        assert raw.scalars(
            select(TaskDispatchOutbox.attempt)
            .where(TaskDispatchOutbox.job_id == row.id)
            .order_by(TaskDispatchOutbox.attempt)
        ).all() == [1, 2]

        old_provider = SimpleNamespace(complete_json=AsyncMock(return_value=_audition_plans()))
        old_result = await run_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=1,
            provider=old_provider,
            allow_running=True,
        )
        assert old_result is row
        old_provider.complete_json.assert_not_awaited()
        assert row.status == "draft"

    asyncio.run(scenario())


def test_scene_audition_periodic_recovery_finishes_worker_lost_without_user_action() -> None:
    """F16：Worker 丢失后由 Beat 安全终结，不依赖用户再次点击且不自动调用模型。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)
        row = await queue_audition(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            scene_no=1,
            expected_episode_version=7,
            scene_type="identity_reveal",
        )
        claimed, should_generate = await claim_queued_audition(
            db,
            audition_id=row.id,
            expected_attempt=1,
        )
        assert claimed is row
        assert should_generate is True
        now = datetime.now(UTC)
        row.started_at = now - timedelta(minutes=11)
        raw.commit()

        assert await fail_expired_auditions(db, now=now) == 1
        assert row.status == "failed"
        assert row.error_code == "worker_timeout"
        assert row.error_message == "导演方案任务执行超时，请重试"
        assert row.finished_at == now
        assert await fail_expired_auditions(db, now=now) == 0

        from app.workers.celery_app import app as celery_app

        schedule = celery_app.conf.beat_schedule["recover-scene-auditions"]
        assert schedule["task"] == "scene_audition.recover-expired"
        assert schedule["schedule"] == 60.0

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("request_style", "subject_style", "script_style", "unify_style", "expected"),
    [
        ("anime", "live_action", "live_action", True, "anime"),
        (None, "anime", "live_action", True, "anime"),
        (None, None, "live_action", True, "live_action"),
        (None, None, "live_action", False, None),
    ],
)
def test_visual_style_generation_priority(
    monkeypatch,
    request_style: str | None,
    subject_style: str | None,
    script_style: str | None,
    unify_style: bool,
    expected: str | None,
) -> None:
    """F18：请求 > 主体 > 开启统一风格的剧本 > 系统默认。"""

    async def scenario() -> None:
        user = SimpleNamespace(user_id="user-style")
        material = SimpleNamespace(id="material-style")
        script = SimpleNamespace(
            id="script-style",
            visual_style=script_style,
            unify_style=unify_style,
        )
        subject = SimpleNamespace(
            id="subject-style",
            script_id=script.id,
            type="character",
            name="角色",
            description="角色描述",
            visual_style=subject_style,
            subject_version=1,
            enrich_version=1,
            episode_nos=[],
        )
        result = SimpleNamespace(scalar_one_or_none=lambda: subject)
        db = SimpleNamespace(execute=AsyncMock(return_value=result))
        create = AsyncMock(return_value=SimpleNamespace(job_id="job-style"))
        monkeypatch.setattr(
            generation_service,
            "_load_material_script",
            AsyncMock(return_value=(material, script)),
        )
        monkeypatch.setattr(generation_service, "check_script_access", AsyncMock())
        monkeypatch.setattr(
            generation_service,
            "_subject_episode_snapshot",
            AsyncMock(return_value=(None, 1, 0, {})),
        )
        monkeypatch.setattr(
            generation_service,
            "_find_idempotent_job",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            generation_service,
            "_load_credit_account",
            AsyncMock(return_value=SimpleNamespace(id="account-style")),
        )
        monkeypatch.setattr(generation_service, "_create_one_job", create)

        await generation_service.create_generation_job(
            db,
            user=user,
            payload=CreateJobIn(
                material_id=material.id,
                subject_id=subject.id,
                prompt="雨夜中的角色",
                visual_style=request_style,
            ),
            idempotency_key=f"style-{expected}-{unify_style}",
        )
        assert create.await_args.kwargs["visual_style"] == expected
        final_prompt = create.await_args.kwargs["resolved_prompt"]
        if expected == "anime":
            assert "animated production medium" in final_prompt
        elif expected == "live_action":
            assert "live-action physical production" in final_prompt
        else:
            assert "production medium" not in final_prompt

    asyncio.run(scenario())


def test_visual_style_null_clears_subject_and_script_inheritance_override() -> None:
    """F18：PATCH 显式 null 必须清除覆盖，而不是被 exclude_none 忽略。"""

    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed_script(raw)
        db = _AsyncSessionAdapter(raw)
        seeded.subject.visual_style = "anime"
        raw.commit()

        await asset_crud.update_subject(
            db,
            seeded.subject,
            {"visual_style": None},
            expected_subject_version=1,
        )
        assert seeded.subject.visual_style is None
        assert seeded.subject.subject_version == 2
        assert seeded.subject.enrich_version == 2

        await script_crud.rename_script(
            db,
            seeded.script,
            None,
            visual_style=None,
            visual_style_provided=True,
            commit=False,
        )
        assert seeded.script.visual_style is None

    asyncio.run(scenario())


def test_openapi_exposes_optimization_paths_and_new_contract_fields() -> None:
    """F06/F10/F16/F18：聚合路由与请求契约进入 OpenAPI。"""

    from app.main import create_app

    schema = create_app().openapi()
    paths = schema["paths"]
    assert "/v3api/scripts/{script_id}/subject-candidates" in paths
    assert "/v3api/scripts/{script_id}/subject-candidates/{candidate_id}/resolve" in paths
    assert "/v3api/scripts/{script_id}/art-styles/extract" in paths
    assert "/v3api/scripts/{script_id}/art-styles/{art_style_id}" in paths
    assert "/v3api/scripts/{script_id}/episodes/{episode_id}/scene-auditions/generate" in paths
    assert (
        "/v3api/scripts/{script_id}/episodes/{episode_id}/scene-auditions/{audition_id}/confirm"
        in paths
    )

    components = schema["components"]["schemas"]
    assert {"visual_style", "unify_style"} <= set(components["ScriptCreateIn"]["properties"])
    assert {"visual_style", "unify_style"} <= set(components["ScriptUpdateIn"]["properties"])
    assert {"art_style_id", "visual_style"} <= set(components["CreateJobIn"]["properties"])
    assert {"art_style_id", "visual_style"} <= set(
        components["BatchCreateJobIn"]["properties"]
    )
    assert "expected_version" in components["SubjectCandidateResolveIn"]["properties"]
    assert "expected_input_version" in components["SceneAuditionConfirmIn"]["properties"]
    generate_schema = components["SceneAuditionGenerateIn"]["properties"]["scene_type"]
    assert set(generate_schema["enum"]) == {
        "character_introduction",
        "identity_reveal",
        "relationship_reversal",
        "climax_ending",
    }
    audition_out = components["SceneAuditionOut"]["properties"]
    assert {"attempt_count", "error_code", "error_message", "started_at", "finished_at"} <= set(
        audition_out
    )
