"""整集小分镜自动组合的最小验收。"""

from __future__ import annotations

import os
import secrets
from types import SimpleNamespace

import pytest


def test_partition_preserves_llm_narrative_boundaries() -> None:
    from app.services.shot_group_merge import _partition_scene_shots

    shots = [SimpleNamespace(shot_id=f"shot-{index}") for index in range(1, 5)]
    durations = {"shot-1": 3, "shot-2": 2, "shot-3": 2, "shot-4": 2}

    groups = _partition_scene_shots(
        shots,
        duration_by_shot=durations,
        preferred_boundaries={2, 4},
    )

    assert [[shot.shot_id for shot in group] for group in groups] == [
        ["shot-1", "shot-2"],
        ["shot-3", "shot-4"],
    ]
    assert [sum(durations[shot.shot_id] for shot in group) for group in groups] == [5, 4]


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 镜头组合测试",
)
def test_postgres_merge_all_storyboard_shots_into_video_units() -> None:
    import asyncio

    from sqlalchemy import delete, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.errors import BadRequestError, ConflictError
    from app.core.ids import new_id
    from app.crud.shot import get_shot_hierarchy, get_video_workbench
    from app.models.episode import Episode
    from app.models.scene import Scene
    from app.models.script import Script
    from app.models.shot import Shot
    from app.models.shot_group import ShotGroup
    from app.models.subject import Subject
    from app.models.video_generation_job import VideoGenerationJob
    from app.services.shot_group_merge import merge_shots_into_groups
    from app.services.video_generation import create_video_job

    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(5)
        script_id = f"script-merge-{suffix}"
        episode_id = f"episode-merge-{suffix}"
        group_ids = [f"draft-{index}-{suffix}" for index in range(1, 4)]
        shot_ids = [f"shot-{index}-{suffix}" for index in range(1, 7)]
        group_specs = (
            (1, group_ids[0], (3, 2), "建立办公室冲突"),
            (1, group_ids[1], (2, 2), "推进人物行动"),
            (2, group_ids[2], (3, 3), "切换到街道"),
        )

        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="整集镜头组合测试",
                        scope="personal",
                        owner_user_id="user-merge",
                        created_by_user_id="user-merge",
                    )
                )
                await db.flush()
                db.add(
                    Episode(
                        episode_id=episode_id,
                        script_id=script_id,
                        episode_no=1,
                        title="第一集",
                        episode_version=1,
                    )
                )
                subjects = [
                    Subject(id=new_id(), script_id=script_id, type="background", name="办公室"),
                    Subject(id=new_id(), script_id=script_id, type="background", name="街道"),
                    Subject(id=new_id(), script_id=script_id, type="character", name="老姜"),
                    Subject(id=new_id(), script_id=script_id, type="prop", name="手机"),
                    Subject(id=new_id(), script_id=script_id, type="sound", name="环境音乐"),
                ]
                db.add_all(subjects)
                db.add_all(
                    [
                        Scene(
                            id=new_id(),
                            episode_id=episode_id,
                            scene_no=scene_no,
                            scene_key=f"bg:{background}",
                            background=background,
                            label=f"场 {scene_no} · {background}",
                            shot_group_count=0,
                            pending_shot_count=4 if scene_no == 1 else 2,
                        )
                        for scene_no, background in ((1, "办公室"), (2, "街道"))
                    ]
                )
                await db.flush()

                shot_index = 0
                for group_index, (scene_no, group_id, durations, narrative) in enumerate(
                    group_specs, start=1
                ):
                    background = "办公室" if scene_no == 1 else "街道"
                    db.add(
                        ShotGroup(
                            shot_group_id=group_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            scene_no=scene_no,
                            group_index=group_index,
                            group_no=f"G{group_index}",
                            unit_id=f"draft:{group_id}",
                            unit_runtime_key=f"{episode_id}:{scene_no}:draft:{group_id}",
                            duration_seconds=sum(durations),
                            video_duration_seconds=sum(durations),
                            description_text=narrative,
                        )
                    )
                    await db.flush()
                    cursor = 0
                    for display_no, duration in enumerate(durations, start=1):
                        shot_id = shot_ids[shot_index]
                        shot_index += 1
                        db.add(
                            Shot(
                                shot_id=shot_id,
                                scene_no=scene_no,
                                shot_group_id=group_id,
                                group_no=f"G{group_index}",
                                shot_no=f"old-{display_no}",
                                display_no=display_no,
                                content=f"镜头{shot_index}",
                                description=f"镜头{shot_index}描述",
                                shot_line=(
                                    f"【镜头{display_no}:{cursor}s～{cursor + duration}s】"
                                    f"场景是{background}，老姜查看手机"
                                ),
                                start_seconds=cursor,
                                end_seconds=cursor + duration,
                                characters=["老姜"],
                                props=["手机"],
                                sounds=["环境音乐"],
                                dialogue_character="老姜" if display_no == 1 else "",
                            )
                        )
                        cursor += duration
                await db.commit()

            async with session_factory() as db:
                hierarchy = await get_shot_hierarchy(db, script_id, episode_id)
                assert [len(scene.groups) for scene in hierarchy.scenes] == [2, 1]
                assert [
                    sum(len(group.shots) for group in scene.groups) for scene in hierarchy.scenes
                ] == [4, 2]
                assert [scene.ungrouped_shots for scene in hierarchy.scenes] == [[], []]
                before = await get_video_workbench(db, script_id, episode_id)
                assert [scene.shot_groups for scene in before.scenes] == [[], []]
                assert [len(scene.pending_shots) for scene in before.scenes] == [4, 2]

                with pytest.raises(ConflictError, match="先完成镜头组合并"):
                    await create_video_job(
                        db,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_group_id=group_ids[0],
                        user_id="user-merge",
                        idempotency_key=f"draft-block-{suffix}",
                    )
                await db.rollback()

                with pytest.raises(BadRequestError, match="全部分镜 ID"):
                    await merge_shots_into_groups(
                        db,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_ids=shot_ids[:-1],
                        expected_episode_version=1,
                    )
                await db.rollback()

            async with session_factory() as db:
                result = await merge_shots_into_groups(
                    db,
                    script_id=script_id,
                    episode_id=episode_id,
                    shot_ids=list(reversed(shot_ids)),
                    expected_episode_version=1,
                )
                await db.commit()

            async with session_factory() as db:
                episode = await db.get(Episode, episode_id)
                assert episode is not None and episode.episode_version == 2
                assert len(result.shot_group_ids) == 3
                assert result.removed_shot_group_ids == tuple(group_ids)

                groups = list(
                    (
                        await db.execute(
                            select(ShotGroup)
                            .where(ShotGroup.episode_id == episode_id)
                            .order_by(ShotGroup.group_index)
                        )
                    ).scalars().all()
                )
                assert [group.duration_seconds for group in groups] == [5, 4, 6]
                assert [group.scene_no for group in groups] == [1, 1, 2]
                assert [group.group_no for group in groups] == ["G1", "G2", "G1"]
                assert all(group.unit_id == f"group:{group.shot_group_id}" for group in groups)
                assert all(3 <= group.duration_seconds <= 15 for group in groups)

                ordered_ids: list[str] = []
                for group in groups:
                    merged_shots = list(
                        (
                            await db.execute(
                                select(Shot)
                                .where(Shot.shot_group_id == group.shot_group_id)
                                .order_by(Shot.display_no)
                            )
                        ).scalars().all()
                    )
                    ordered_ids.extend(shot.shot_id for shot in merged_shots)
                    assert merged_shots[0].start_seconds == 0
                    assert merged_shots[-1].end_seconds == group.duration_seconds
                    assert "老姜" in group.prompt and "手机" in group.prompt
                    assert [item["name"] for item in group.materials["character"]] == ["老姜"]
                    assert [item["name"] for item in group.materials["prop"]] == ["手机"]
                    assert group.audio_elements["sounds"][0]["name"] == "环境音乐"
                assert ordered_ids == shot_ids

                workbench = await get_video_workbench(db, script_id, episode_id)
                assert [len(scene.shot_groups) for scene in workbench.scenes] == [2, 1]
                assert [scene.pending_shots for scene in workbench.scenes] == [[], []]
                job_count = (
                    await db.execute(
                        select(VideoGenerationJob.job_id).where(
                            VideoGenerationJob.episode_id == episode_id
                        )
                    )
                ).scalars().all()
                assert job_count == []
        finally:
            async with session_factory() as db:
                await db.execute(
                    delete(VideoGenerationJob).where(VideoGenerationJob.episode_id == episode_id)
                )
                await db.execute(delete(Shot).where(Shot.shot_id.in_(shot_ids)))
                await db.execute(delete(ShotGroup).where(ShotGroup.episode_id == episode_id))
                await db.execute(delete(Scene).where(Scene.episode_id == episode_id))
                await db.execute(delete(Subject).where(Subject.script_id == script_id))
                await db.execute(delete(Episode).where(Episode.episode_id == episode_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())
