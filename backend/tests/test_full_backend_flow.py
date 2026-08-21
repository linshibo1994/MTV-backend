"""完整后端业务链路的确定性集成验收测试。

所有 AI、对象存储与 ffmpeg 均使用进程内 fake；测试不访问网络，也不依赖 Celery worker。
"""

from __future__ import annotations

import asyncio
import copy
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.crud import asset as asset_crud
from app.crud import materials as materials_crud
from app.crud import script as script_crud
from app.crud import timeline as timeline_crud
from app.models import Base
from app.models.asset_entry import AssetEntry
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.pipeline_stage import PipelineStage
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.models.video_provider import VideoProviderConfig
from app.providers.video_gen import VideoGenerationProgress, VideoGenerationResult
from app.schemas.materials import CanvasDeltaIn
from app.services import credits
from app.services import review as review_service
from app.services import storyboard as storyboard_service
from app.services import timeline_render as timeline_render_service
from app.services import video_generation as video_generation_service
from app.services.pipeline import input_episode_is_current, set_stage
from app.services.pipeline import orchestrator as pipeline_orchestrator
from app.services.pipeline.enrich import run_enrich
from app.services.pipeline.extract import run_extract
from app.services.pipeline.merge import run_merge
from app.services.pipeline.parents import run_parents
from app.services.pipeline.split import run_split
from app.services.reference_aggregator import aggregate_references
from app.services.reference_image_audit import reference_image_hash
from app.services.shot_group_merge import merge_shots_into_groups
from app.services.task_dispatch import dispatch_outbox_batch
from app.workers import timeline_render as timeline_worker
from app.workers.video_generation import run_video_generation_once


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_: JSONB, compiler: Any, **kwargs: Any) -> str:
    del type_, compiler, kwargs
    return "JSON"


class _AsyncNested:
    def __init__(self, transaction: Any) -> None:
        self.transaction = transaction

    async def __aenter__(self) -> Any:
        return self.transaction.__enter__()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return self.transaction.__exit__(exc_type, exc, tb)


class _AsyncSessionAdapter:
    """让真实 async 服务在文件型 SQLite 会话上同步、确定性执行。"""

    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement: Any) -> Any:
        # SQLite 没有 PostgreSQL 咨询锁；单进程验收测试本身已串行。
        if "pg_advisory_xact_lock" in str(statement):
            return self.session.execute(select(1))
        return self.session.execute(statement)

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    async def refresh(
        self,
        row: Any,
        attribute_names: list[str] | None = None,
    ) -> None:
        self.session.refresh(row, attribute_names=attribute_names)

    async def delete(self, row: Any) -> None:
        self.session.delete(row)

    def add(self, row: Any) -> None:
        self.session.add(row)

    def begin_nested(self) -> _AsyncNested:
        return _AsyncNested(self.session.begin_nested())


class _SessionContext:
    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self.raw: Session | None = None

    async def __aenter__(self) -> _AsyncSessionAdapter:
        self.raw = Session(self.engine, expire_on_commit=False)
        return _AsyncSessionAdapter(self.raw)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        assert self.raw is not None
        self.raw.close()


class _SessionFactory:
    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def __call__(self) -> _SessionContext:
        return _SessionContext(self.engine)


class _StaticLLM:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls = 0

    async def complete_json(self, system: str, user: str) -> dict[str, Any]:
        assert system and user
        self.calls += 1
        return self.payload


class _FakeVideoProvider:
    provider_key = "modelsai"

    def __init__(self) -> None:
        self.request: Any | None = None

    async def generate(self, request: Any, on_progress: Any = None) -> VideoGenerationResult:
        self.request = request
        if on_progress is not None:
            on_progress(
                VideoGenerationProgress(
                    stage="generating",
                    message="fake 视频生成完成",
                    progress=100,
                    provider_task_id="fake-provider-task",
                )
            )
        return VideoGenerationResult(
            video_storage_key=f"generate_images/v5/{request.job_id}.mp4",
            provider_job_id="fake-provider-task",
            metrics={"fake": True},
        )


class _FakeStorage:
    def __init__(self, initial: dict[str, bytes]) -> None:
        self.objects = dict(initial)

    async def get(self, key: str) -> bytes:
        return self.objects[key]

    async def put(self, key: str, data: bytes) -> str:
        self.objects[key] = data
        return key

    async def promote(self, source_key: str, final_key: str) -> str:
        self.objects[final_key] = self.objects[source_key]
        self.objects.pop(source_key, None)
        return final_key

    async def delete(self, key: str) -> None:
        self.objects.pop(key, None)


class _FakeOutboxPublisher:
    """只记录真实 dispatcher 的发布调用，不启动 Celery 或外部 worker。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, task_type: str, payload: dict[str, Any]) -> None:
        self.calls.append((task_type, payload["dispatch_id"], payload["run_id"]))


async def _publish_one_outbox_idempotently(
    db: AsyncSession,
    publisher: _FakeOutboxPublisher,
    *,
    expected_task_type: str,
) -> None:
    """走真实 claim/publish 状态机，并证明重放不会再次调用 publisher。"""
    call_count = len(publisher.calls)
    assert await dispatch_outbox_batch(
        db,
        publisher,
        owner="v5-deterministic-dispatcher",
        limit=10,
    ) == 1
    assert publisher.calls[-1][0] == expected_task_type
    assert len(publisher.calls) == call_count + 1
    published = (
        await db.execute(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.task_type == expected_task_type
            )
        )
    ).scalar_one()
    assert published.status == "published"
    assert published.publish_attempts == 1
    assert published.published_at is not None
    assert await dispatch_outbox_batch(
        db,
        publisher,
        owner="v5-deterministic-dispatcher-replay",
        limit=10,
    ) == 0
    assert len(publisher.calls) == call_count + 1


def _subjects_payload() -> list[dict[str, Any]]:
    return [
        {
            "name": "阿明",
            "type": "character",
            "description": "谨慎的年轻调查员，穿深色外套",
            "is_child": True,
            "episode_nos": [1],
        },
        {
            "name": "小林",
            "type": "character",
            "description": "坐在办公桌后的同事，神情迟疑",
            "is_child": True,
            "episode_nos": [1],
        },
        {
            "name": "办公室",
            "type": "scene",
            "description": "冷色顶光的现代办公室",
            "is_child": True,
            "episode_nos": [1],
        },
        {
            "name": "钥匙",
            "type": "prop",
            "description": "有使用痕迹的黄铜钥匙",
            "is_child": True,
            "episode_nos": [1],
        },
        {
            "name": "门响",
            "type": "sound",
            "description": "木门打开时短促的吱呀声",
            "is_child": True,
            "episode_nos": [1],
        },
    ]


def _storyboard_payload() -> dict[str, Any]:
    return {
        "schema_version": "storyboard.v2",
        "scenes": [
            {
                "scene_no": 1,
                "background_ref": "BG01",
                "shot_groups": [
                    {
                        "group_no": "G1",
                        "narrative_purpose": "建立空间并交代冲突",
                        "duration_seconds": 6,
                        "shots": [
                            {
                                "display_no": 1,
                                "start_seconds": 0,
                                "end_seconds": 3,
                                "visual": {
                                    "shot_size": "全景",
                                    "composition": ["三分法", "纵深构图"],
                                    "camera_angle": "平视",
                                    "camera_movement": "推镜",
                                    "action": "阿明推门进入",
                                    "frame_description": "前景门框形成框景，阿明进入办公室",
                                    "lighting": "冷色顶光",
                                    "emotion": "紧张",
                                },
                                "narrative_purpose": "交代阿明进入并触发冲突",
                                "source_block_refs": ["B01"],
                                "character_refs": ["C01"],
                                "prop_refs": ["P01"],
                                "sound_refs": ["S01"],
                                "sound_design": "门轴轻响，室内环境声短暂停顿",
                                "dialogue": {
                                    "speaker_ref": "C01",
                                    "text": "我们谈谈",
                                    "kind": "dialogue",
                                },
                            },
                            {
                                "display_no": 2,
                                "start_seconds": 3,
                                "end_seconds": 6,
                                "visual": {
                                    "shot_size": "近景",
                                    "composition": ["单人", "三分法"],
                                    "camera_angle": "平视",
                                    "camera_movement": "固定",
                                    "action": "小林停下工作并抬头",
                                    "frame_description": "小林越过前景文件看向门口",
                                    "lighting": "冷色顶光勾勒面部",
                                    "emotion": "迟疑",
                                },
                                "narrative_purpose": "用反应镜承接前一镜",
                                "source_block_refs": ["B01"],
                                "character_refs": ["C02"],
                                "prop_refs": [],
                                "sound_refs": [],
                                "sound_design": "无",
                                "dialogue": None,
                            },
                        ],
                    }
                ],
            }
        ],
        "omitted_source_blocks": [],
    }


async def test_storyboard_scene_split_persists_plan_ordered_background_bindings(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """两场并行返回乱序时，落库仍必须按 split plan 绑定各自背景短引用。"""

    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'scene-split-bindings.db'}")
    Base.metadata.create_all(engine)
    raw = Session(engine, expire_on_commit=False)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    script_id = "scene-split-script"
    episode_id = "scene-split-episode"
    raw.add_all(
        [
            Script(
                id=script_id,
                title="场景拆分绑定",
                scope="personal",
                owner_user_id="scene-split-owner",
                created_by_user_id="scene-split-owner",
            ),
            Episode(
                episode_id=episode_id,
                script_id=script_id,
                episode_no=1,
                title="第一集",
                script_text="第一场发生在办公室。\n\n第二场转到走廊。",
                episode_version=1,
                client_save_seq=0,
            ),
            Subject(
                id="scene-split-bg-01",
                script_id=script_id,
                type="background",
                name="A办公室",
                pipeline_name="A办公室",
                description="冷色办公室",
                episode_nos=[1],
                is_active=True,
            ),
            Subject(
                id="scene-split-bg-02",
                script_id=script_id,
                type="background",
                name="B走廊",
                pipeline_name="B走廊",
                description="安静走廊",
                episode_nos=[1],
                is_active=True,
            ),
        ]
    )
    raw.commit()

    def scene_payload(background_ref: str, block_ref: str, action: str) -> dict[str, Any]:
        payload = copy.deepcopy(_storyboard_payload())
        scene = payload["scenes"][0]
        scene["background_ref"] = background_ref
        for shot in scene["shot_groups"][0]["shots"]:
            shot["source_block_refs"] = [block_ref]
            shot["character_refs"] = []
            shot["prop_refs"] = []
            shot["sound_refs"] = []
            shot["dialogue"] = None
        scene["shot_groups"][0]["shots"][0]["visual"]["action"] = action
        return payload

    completed_scene_calls: list[int] = []

    async def fake_storyboard_llm(
        system: str,
        user: str,
        **_kwargs: object,
    ) -> dict[str, Any]:
        assert system and user
        if system == storyboard_service._SCENE_SPLIT_SYSTEM_PROMPT:
            return {
                "scenes": [
                    {
                        "scene_no": 1,
                        "background_ref": "BG01",
                        "source_block_refs": ["B01"],
                        "narrative_purpose": "办公室定场",
                        "estimated_groups": 1,
                    },
                    {
                        "scene_no": 2,
                        "background_ref": "BG02",
                        "source_block_refs": ["B02"],
                        "narrative_purpose": "走廊转场",
                        "estimated_groups": 1,
                    },
                ],
                "omitted_source_blocks": [],
            }
        if "当前场景：第 1 场" in user:
            await asyncio.sleep(0.02)
            completed_scene_calls.append(1)
            return scene_payload("BG01", "B01", "镜头停留在办公室门口")
        assert "当前场景：第 2 场" in user
        completed_scene_calls.append(2)
        return scene_payload("BG02", "B02", "镜头跟随进入走廊")

    monkeypatch.setattr(storyboard_service, "_call_llm", fake_storyboard_llm)
    result = await storyboard_service.generate_storyboard(
        db,
        script_id,
        episode_id,
        expected_episode_version=1,
        expected_client_save_seq=0,
    )
    await db.commit()

    assert completed_scene_calls == [2, 1]
    assert result["scenes"] == 2
    persisted_scenes = list(
        (
            await db.execute(
                select(Scene)
                .where(Scene.episode_id == episode_id)
                .order_by(Scene.scene_no)
            )
        ).scalars()
    )
    persisted_groups = list(
        (
            await db.execute(
                select(ShotGroup)
                .where(ShotGroup.episode_id == episode_id)
                .order_by(ShotGroup.scene_no, ShotGroup.group_index)
            )
        ).scalars()
    )
    persisted_shots = list(
        (
            await db.execute(
                select(Shot)
                .join(ShotGroup, Shot.shot_group_id == ShotGroup.shot_group_id)
                .where(ShotGroup.episode_id == episode_id)
                .order_by(Shot.scene_no, Shot.display_no)
            )
        ).scalars()
    )

    assert [(scene.scene_no, scene.background) for scene in persisted_scenes] == [
        (1, "A办公室"),
        (2, "B走廊"),
    ]
    assert [group.scene_no for group in persisted_groups] == [1, 2]
    assert [group.materials["background"][0]["ref"] for group in persisted_groups] == [
        "BG01",
        "BG02",
    ]
    assert [
        (shot.scene_no, shot.structured_data["background_ref"])
        for shot in persisted_shots
    ] == [(1, "BG01"), (1, "BG01"), (2, "BG02"), (2, "BG02")]


async def test_full_backend_flow_reaches_succeeded_render_job(monkeypatch, tmp_path: Path) -> None:
    """同一剧本/分集贯通保存、AI 流水线、资产、分镜、视频和时间线合成。"""

    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'full-flow.db'}")
    Base.metadata.create_all(engine)
    raw = Session(engine, expire_on_commit=False)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))

    user_id = "user-v5"
    script_id = "script-v5"
    episode_id = "episode-v5"
    account_id = "account-v5"
    raw.add_all(
        [
            User(
                user_id=user_id,
                username="v5-owner",
                password_hash="test-only-hash",
                enabled=True,
                account_type="personal",
            ),
            Script(
                id=script_id,
                title="V5 完整链路",
                scope="personal",
                owner_user_id=user_id,
                created_by_user_id=user_id,
            ),
            Episode(
                episode_id=episode_id,
                script_id=script_id,
                episode_no=1,
                title="第一集",
                script_text="阿明走进办公室。",
                episode_version=1,
                client_save_seq=0,
            ),
            CreditAccount(
                id=account_id,
                owner_type="user",
                owner_id=user_id,
                balance_points=100_000,
                frozen_points=0,
                status="active",
            ),
            VideoProviderConfig(
                id="provider-v5",
                provider_key="modelsai",
                job_type="all_reference",
                model="fake-seedance-v5",
                is_active=True,
            ),
        ]
    )
    raw.commit()
    publisher = _FakeOutboxPublisher()

    # 1. 保存分集并证明旧输入版本会被栅栏拒绝，同时释放其预扣积分。
    saved = await script_crud.save_episode(
        db,
        script_id,
        episode_id,
        "阿明拿着钥匙进入办公室，门响，小林抬头。",
        episode_version=1,
        client_save_seq=0,
    )
    assert (saved.episode_version, saved.client_save_seq) == (2, 1)
    stale_reservation_id = await credits.reserve(
        db,
        account_id,
        "pipeline",
        "stale-run-v5",
        1,
        actor_user_id=user_id,
        commit=False,
    )
    assert not await input_episode_is_current(db, script_id, episode_id, 1, 0)
    await credits.release(
        db,
        stale_reservation_id,
        "旧输入版本被栅栏拒绝",
        actor_user_id=user_id,
        commit=False,
    )
    await db.commit()

    # 2. 触发真实流水线编排（reserve + outbox），同步执行五阶段 fake LLM。
    pipeline = await pipeline_orchestrator.trigger_pipeline(db, script_id, episode_id)
    assert (pipeline["input_version"], pipeline["input_client_save_seq"]) == (2, 1)
    await _publish_one_outbox_idempotently(
        db,
        publisher,
        expected_task_type="pipeline.dag",
    )
    subjects_payload = _subjects_payload()
    split_provider = _StaticLLM(
        {
            "schema_version": "pipeline.v1",
            "episodes": [
                {
                    "episode_no": 1,
                    "title": "第一集",
                    "script_text": "阿明拿着钥匙进入办公室，门响，小林抬头。",
                }
            ],
        }
    )
    assert (
        await run_split(
            db,
            script_id,
            episode_id,
            input_version=2,
            input_client_save_seq=1,
            provider=split_provider,
        )
        == 1
    )
    assert await run_extract(
        db,
        script_id,
        episode_id=episode_id,
        input_version=2,
        input_client_save_seq=1,
        provider=_StaticLLM({"schema_version": "pipeline.v1", "subjects": subjects_payload}),
    ) == {
        "episodes": [
            {
                "episode_id": episode_id,
                "episode_no": 1,
                "title": "第一集",
                "extract_status": "succeeded",
                "subjects": subjects_payload,
            }
        ]
    }
    assert (
        await run_merge(
            db,
            script_id,
            episode_id=episode_id,
            input_version=2,
            input_client_save_seq=1,
            provider=_StaticLLM(
                {
                    "schema_version": "pipeline.v1",
                    "merged_subjects": subjects_payload,
                    "total_count": len(subjects_payload),
                }
            ),
        )
        == 5
    )
    enrich_provider = _StaticLLM({"schema_version": "pipeline.v1"})
    assert (
        await run_enrich(
            db,
            script_id,
            episode_id=episode_id,
            input_version=2,
            input_client_save_seq=1,
            provider=enrich_provider,
        )
        == 0
    )
    assert enrich_provider.calls == 0
    assert (
        await run_parents(
            db,
            script_id,
            episode_id=episode_id,
            input_version=2,
            input_client_save_seq=1,
            provider=_StaticLLM({"schema_version": "pipeline.v1", "parent_subjects": []}),
        )
        == 0
    )
    await set_stage(db, script_id, "enrich", status="done")
    await set_stage(db, script_id, "parents", status="done")
    jobs = list(
        (await db.execute(select(PipelineJob).where(PipelineJob.run_id == pipeline["run_id"]))).scalars()
    )
    for job in jobs:
        job.status = "done"
        job.finished_at = datetime.now(UTC)
    await credits.settle(
        db,
        pipeline["reservation_id"],
        pipeline["estimated"],
        actor_user_id=user_id,
        commit=False,
    )
    await db.commit()
    assert len(jobs) == 5

    # 3. 生成候选并通过审核；随后画布节点只能采用服务端可信主体字段。
    subject_rows = list((await db.execute(select(Subject).where(Subject.script_id == script_id))).scalars())
    by_name = {subject.name: subject for subject in subject_rows}
    for name in ("阿明", "小林", "办公室", "钥匙"):
        subject = by_name[name]
        candidate = await asset_crud.add_candidate(
            db,
            script_id,
            subject.id,
            "image",
            f"generate_images/v5/{subject.id}.png",
            "image/png",
            100,
            "generate_images",
            sha256=f"{subject.id.replace('-', ''):0<64}"[:64],
            subject=subject,
        )
        await db.commit()
        approved, _ = await review_service.approve_image_with_lock(db, subject.id, candidate.id, confirm=True)
        assert approved.review_status == "approved"
    for name in ("阿明", "门响"):
        subject = by_name[name]
        candidate = await asset_crud.add_candidate(
            db,
            script_id,
            subject.id,
            "audio",
            f"generate_audio/v5/{subject.id}.wav",
            "audio/wav",
            100,
            "generate_audio",
            sha256=f"{subject.id.replace('-', ''):0<64}"[:64],
            subject=subject,
        )
        await db.commit()
        approved = await review_service.approve_audio_with_lock(db, subject.id, candidate.id)
        assert any(entry.asset_id == candidate.id for entry in approved.audio_entries)

    script = await db.get(Script, script_id)
    assert script is not None
    materials, created = await asset_crud.ensure_materials(db, script)
    assert created
    canvas = await materials_crud.apply_delta(
        db,
        materials.id,
        CanvasDeltaIn.model_validate(
            {
                "version": 0,
                "upsert_nodes": [
                    {
                        "id": "node-aming-v5",
                        "type": "imageUpload",
                        "position": {"x": 10, "y": 20},
                        "data": {
                            "subjectId": by_name["阿明"].id,
                            "subjectName": "客户端伪造名称",
                            "subjectType": "prop",
                            "subjectDescription": "客户端伪造描述",
                            "prompt": "前端传入的阿明角色设定最终生图提示词",
                        },
                    }
                ],
            }
        ),
    )
    await db.commit()
    assert canvas.version == 1
    assert canvas.nodes[0].data is not None
    assert canvas.nodes[0].data["subjectName"] == "阿明"
    assert canvas.nodes[0].data["subjectType"] == "character"

    # 4. fake 分镜、fake 视频生成均走真实服务和积分/任务状态机。
    async def fake_storyboard_llm(system: str, user: str) -> dict[str, Any]:
        assert system and user
        if "场景划分" in user:
            return {
                "scenes": [
                    {
                        "scene_no": 1,
                        "background_ref": "BG01",
                        "source_block_refs": ["B01"],
                        "narrative_purpose": "建立空间并交代冲突",
                        "estimated_groups": 1,
                    }
                ],
                "omitted_source_blocks": [],
            }
        return _storyboard_payload()

    monkeypatch.setattr(storyboard_service, "_call_llm", fake_storyboard_llm)
    storyboard_result = await storyboard_service.generate_storyboard(
        db,
        script_id,
        episode_id,
        expected_episode_version=2,
        expected_client_save_seq=1,
    )
    await db.commit()
    assert storyboard_result["script_id"] == script_id
    assert storyboard_result["episode_id"] == episode_id
    assert storyboard_result["scenes"] == 1
    assert storyboard_result["shot_groups"] == 1
    assert storyboard_result["shots"] == 2
    draft_group = (
        await db.execute(select(ShotGroup).where(ShotGroup.episode_id == episode_id))
    ).scalar_one()
    storyboard_shots = list((await db.execute(select(Shot))).scalars())
    assert len(storyboard_shots) == 2
    merge_result = await merge_shots_into_groups(
        db,
        script_id=script_id,
        episode_id=episode_id,
        shot_ids=[shot.shot_id for shot in storyboard_shots],
        expected_episode_version=2,
    )
    await db.commit()
    assert merge_result.removed_shot_group_ids == (draft_group.shot_group_id,)
    shot_group = (
        await db.execute(select(ShotGroup).where(ShotGroup.episode_id == episode_id))
    ).scalar_one()
    assert shot_group.unit_id == f"group:{shot_group.shot_group_id}"
    aggregated = await aggregate_references(db, shot_group.shot_group_id)
    shot_group.reference_image_audit = {
        "status": "approved",
        "reference_hash": reference_image_hash(aggregated),
        "results": [],
    }
    await db.commit()

    monkeypatch.setattr(video_generation_service.settings, "video_provider_selection_mode", "database")
    video_job = await video_generation_service.create_video_job(
        db,
        script_id=script_id,
        episode_id=episode_id,
        shot_group_id=shot_group.shot_group_id,
        user_id=user_id,
        idempotency_key="v5-video",
    )
    await _publish_one_outbox_idempotently(
        db,
        publisher,
        expected_task_type="video.generate",
    )
    fake_video = _FakeVideoProvider()

    async def fake_last_frame(db_: Any, unit_id: str, video_key: str, token: str) -> str:
        assert db_ is db and unit_id and video_key and token
        return "generate_images/v5/last-frame.png"

    await run_video_generation_once(
        db,
        video_job.job_id,
        owner="v5-video-worker",
        provider=fake_video,
        extract_last_frame=fake_last_frame,
    )
    raw.expire_all()
    finished_video = raw.get(VideoGenerationJob, video_job.job_id)
    assert finished_video is not None
    assert finished_video.status == "succeeded", (
        finished_video.error_type,
        finished_video.error_message,
        finished_video.output_json,
    )
    assert finished_video.output_json is not None
    video_storage_key = finished_video.output_json["video_storage_key"]
    assert video_storage_key == f"generate_images/v5/{video_job.job_id}.mp4"
    assert finished_video.input_version == 3
    assert fake_video.request is not None
    assert fake_video.request.reference_images
    assert fake_video.request.reference_audios

    # 5. 时间线触发再次 reserve + outbox；worker 注入存储/ffmpeg 后产出最终 RenderJob。
    arrangement = await timeline_crud.add_arrangement(
        db,
        script_id=script_id,
        episode_id=episode_id,
        unit_runtime_key=shot_group.unit_runtime_key,
        job_id=video_job.job_id,
    )
    assert arrangement.row_order == 0
    await db.commit()
    user = await db.get(User, user_id)
    assert user is not None
    render_created = await timeline_render_service.render(
        db,
        script_id,
        episode_id,
        user,
        idempotency_key="v5-render",
    )
    await db.commit()
    await _publish_one_outbox_idempotently(
        db,
        publisher,
        expected_task_type="timeline.render",
    )
    render_id = render_created["job_id"]
    raw.close()

    storage = _FakeStorage({video_storage_key: b"fake-video-bytes"})
    monkeypatch.setattr(timeline_worker, "async_session_factory", _SessionFactory(engine))
    monkeypatch.setattr(timeline_worker, "get_storage", lambda: storage)

    async def fake_trim(
        src: str,
        start: float,
        end: float | None,
        out: str,
        *,
        job_id: str,
        token: str,
        output_limit_bytes: int | None,
    ) -> None:
        assert job_id == render_id and token and output_limit_bytes
        assert start == 0.0 and end is None
        shutil.copyfile(src, out)

    async def fake_concat(
        inputs: list[str],
        out: str,
        *,
        job_id: str,
        token: str,
        output_limit_bytes: int | None,
    ) -> None:
        assert job_id == render_id and token and output_limit_bytes
        Path(out).write_bytes(b"".join(Path(path).read_bytes() for path in inputs))

    monkeypatch.setattr(timeline_worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(timeline_worker, "_concat_segments", fake_concat)
    render_result = await timeline_worker.run_render_job(
        render_id, owner="v5-render-worker", temp_root=tmp_path
    )
    assert render_result["status"] == "succeeded"

    # 6. 最终不变量：版本一致、outbox 可追踪、积分无冻结且三类流水都出现。
    with Session(engine, expire_on_commit=False) as verify:
        render_job = verify.get(TimelineRenderJob, render_id)
        assert render_job is not None
        assert render_job.status == "succeeded"
        assert render_job.input_version == 3
        assert render_job.output_json is not None
        assert render_job.output_json["storage_key"] == render_job.output_storage_key
        assert render_job.output_storage_key in storage.objects
        assert storage.objects[render_job.output_storage_key] == b"fake-video-bytes"

        outboxes = list(verify.scalars(select(TaskDispatchOutbox)))
        assert {row.task_type for row in outboxes} == {
            "pipeline.dag",
            "video.generate",
            "timeline.render",
        }
        assert all(row.status == "published" for row in outboxes)
        assert all(row.publish_attempts == 1 for row in outboxes)
        assert all(row.published_at is not None for row in outboxes)
        assert pipeline["dispatch_id"] in {row.id for row in outboxes}
        assert [task_type for task_type, _dispatch_id, _run_id in publisher.calls] == [
            "pipeline.dag",
            "video.generate",
            "timeline.render",
        ]
        assert len({dispatch_id for _task_type, dispatch_id, _run_id in publisher.calls}) == 3

        stages = list(verify.scalars(select(PipelineStage)))
        assert {stage.stage for stage in stages} == {
            "split",
            "extract",
            "merge",
            "enrich",
            "parents",
        }
        assert all(stage.status == "done" for stage in stages)
        assert all(job.status == "done" for job in verify.scalars(select(PipelineJob)))

        reservations = list(verify.scalars(select(CreditReservation)))
        by_source = {(item.source_type, item.source_id): item for item in reservations}
        assert by_source[("pipeline", "stale-run-v5")].status == "released"
        assert by_source[("pipeline", pipeline["run_id"])].status == "captured"
        assert by_source[("generation_job", video_job.job_id)].status == "captured"
        assert by_source[("timeline_render_job", render_id)].status == "captured"
        assert {row.log_type for row in verify.scalars(select(CreditLog))} == {
            "reserve",
            "settle",
            "release",
        }
        account = verify.get(CreditAccount, account_id)
        assert account is not None and account.frozen_points == 0
        assert account.balance_points < 100_000

        approved_entries = list(verify.scalars(select(AssetEntry).where(AssetEntry.is_approved.is_(True))))
        assert len(approved_entries) == 6
