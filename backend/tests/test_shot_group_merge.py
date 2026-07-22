"""镜头组合并的最小真实数据库验收。"""

from __future__ import annotations

import os
import secrets

import pytest


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 镜头合并测试",
)
def test_postgres_merge_contiguous_shots_and_protect_video_jobs() -> None:
    import asyncio

    from sqlalchemy import delete, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.errors import ConflictError
    from app.core.ids import new_id
    from app.models.episode import Episode
    from app.models.scene import Scene
    from app.models.script import Script
    from app.models.shot import Shot
    from app.models.shot_group import ShotGroup
    from app.models.video_generation_job import VideoGenerationJob
    from app.services.shot_group_merge import merge_shots_into_group

    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(5)
        script_id = f"script-merge-{suffix}"
        episode_id = f"episode-merge-{suffix}"
        group_ids = [f"group-{index}-{suffix}" for index in range(1, 4)]
        shot_ids = [f"shot-{index}-{suffix}" for index in range(1, 10)]
        blocking_job_id = f"video-merge-{suffix}"
        group_shot_indexes = ((0, 1, 2, 3), (4, 5), (6, 7, 8))

        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="镜头组合并测试",
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
                await db.flush()
                db.add(
                    Scene(
                        id=new_id(),
                        episode_id=episode_id,
                        scene_no=1,
                        scene_key="bg:办公室",
                        background="办公室",
                        label="场 1 · 办公室",
                        shot_group_count=3,
                        pending_shot_count=9,
                    )
                )
                await db.flush()
                for group_index, (group_id, indexes) in enumerate(
                    zip(group_ids, group_shot_indexes, strict=True), start=1
                ):
                    db.add(
                        ShotGroup(
                            shot_group_id=group_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            scene_no=1,
                            group_index=group_index,
                            group_no=f"G{group_index}",
                            duration_seconds=len(indexes) * 2,
                            video_duration_seconds=len(indexes) * 2,
                            description_text=f"叙事目的{group_index}",
                        )
                    )
                    await db.flush()
                    for display_no, shot_index in enumerate(indexes, start=1):
                        start = (display_no - 1) * 2
                        db.add(
                            Shot(
                                shot_id=shot_ids[shot_index],
                                scene_no=1,
                                shot_group_id=group_id,
                                group_no=f"G{group_index}",
                                shot_no=f"old-{display_no}",
                                display_no=display_no,
                                content=f"镜头{shot_index + 1}",
                                description=f"镜头{shot_index + 1}描述",
                                shot_line=(
                                    f"【镜头{display_no}:{start}s～{start + 2}s】"
                                    f"镜头{shot_index + 1}"
                                ),
                                start_seconds=start,
                                end_seconds=start + 2,
                                characters=[],
                                props=[],
                                sounds=[],
                                dialogue_character="",
                            )
                        )
                db.add(
                    VideoGenerationJob(
                        job_id=blocking_job_id,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_group_id=group_ids[1],
                        scene_no=1,
                        unit_id=f"group:{group_ids[1]}",
                        unit_runtime_key=f"{episode_id}:1:group:{group_ids[1]}",
                        provider_key="modelsai",
                        job_type="all_reference",
                        status="failed",
                    )
                )
                await db.commit()

            selected = [shot_ids[index] for index in (6, 5, 4, 3, 2)]
            async with session_factory() as db:
                with pytest.raises(ConflictError, match="已有视频任务"):
                    await merge_shots_into_group(
                        db,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_ids=selected,
                        expected_episode_version=1,
                    )
                await db.rollback()

            async with session_factory() as db:
                await db.execute(
                    delete(VideoGenerationJob).where(
                        VideoGenerationJob.job_id == blocking_job_id
                    )
                )
                await db.commit()

            async with session_factory() as db:
                result = await merge_shots_into_group(
                    db,
                    script_id=script_id,
                    episode_id=episode_id,
                    shot_ids=selected,
                    expected_episode_version=1,
                )
                await db.commit()

            async with session_factory() as db:
                with pytest.raises(ConflictError, match="episode_version 冲突"):
                    await merge_shots_into_group(
                        db,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_ids=selected,
                        expected_episode_version=1,
                    )
                await db.rollback()

            async with session_factory() as db:
                episode = await db.get(Episode, episode_id)
                assert episode is not None and episode.episode_version == 2
                groups = list(
                    (
                        await db.execute(
                            select(ShotGroup)
                            .where(ShotGroup.episode_id == episode_id)
                            .order_by(ShotGroup.group_index)
                        )
                    ).scalars().all()
                )
                assert [group.group_no for group in groups] == ["G1", "G2", "G3"]
                assert groups[1].shot_group_id == result.shot_group_id
                assert result.removed_shot_group_ids == (group_ids[1],)
                merged_shots = list(
                    (
                        await db.execute(
                            select(Shot)
                            .where(Shot.shot_group_id == result.shot_group_id)
                            .order_by(Shot.display_no)
                        )
                    ).scalars().all()
                )
                assert [shot.shot_id for shot in merged_shots] == [
                    shot_ids[index] for index in (2, 3, 4, 5, 6)
                ]
                assert [
                    (shot.display_no, shot.start_seconds, shot.end_seconds)
                    for shot in merged_shots
                ] == [(1, 0, 2), (2, 2, 4), (3, 4, 6), (4, 6, 8), (5, 8, 10)]
                assert groups[1].duration_seconds == 10
                assert "【镜头5:8s～10s】" in groups[1].prompt
                assert groups[1].reference_frames == {
                    "first_frame": None,
                    "last_frame": None,
                    "reference_frame": None,
                }
        finally:
            async with session_factory() as db:
                await db.execute(
                    delete(VideoGenerationJob).where(
                        VideoGenerationJob.episode_id == episode_id
                    )
                )
                await db.execute(
                    delete(Shot).where(Shot.shot_id.in_(shot_ids))
                )
                await db.execute(
                    delete(ShotGroup).where(ShotGroup.episode_id == episode_id)
                )
                await db.execute(delete(Scene).where(Scene.episode_id == episode_id))
                await db.execute(delete(Episode).where(Episode.episode_id == episode_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())
