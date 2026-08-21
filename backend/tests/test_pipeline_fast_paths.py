"""剧本流水线快速路径专项测试。"""

import asyncio
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.models.subject import Subject
from app.services.pipeline import extract, merge, parents, split


class _Rows:
    def __init__(self, rows: list[object], *, rowcount: int = 0) -> None:
        self._rows = rows
        self.rowcount = rowcount

    def scalars(self) -> "_Rows":
        return self

    def all(self) -> list[object]:
        return self._rows

    def scalar_one_or_none(self) -> object | None:
        return self._rows[0] if self._rows else None


class _SplitSession:
    def __init__(self, episode: object) -> None:
        self.episode = episode
        self.select_count = 0
        self.added: list[object] = []
        self.rollbacks = 0

    async def get(self, model, row_id, **kwargs):
        return self.episode

    async def execute(self, statement):
        if getattr(statement, "__visit_name__", "") == "update":
            return _Rows([], rowcount=1)
        self.select_count += 1
        if self.select_count in {1, 2}:
            return _Rows([self.episode])
        return _Rows([])

    def add(self, row: object) -> None:
        self.added.append(row)

    async def flush(self) -> None:
        return None

    async def rollback(self) -> None:
        self.rollbacks += 1


class _QuerySession:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows
        self.rollbacks = 0

    async def execute(self, statement):
        return _Rows(self.rows)

    async def rollback(self) -> None:
        self.rollbacks += 1


def _subject(
    subject_id: str,
    name: str,
    subject_type: str,
    description: str,
) -> Subject:
    return Subject(
        id=subject_id,
        script_id="script-1",
        name=name,
        pipeline_name=name,
        type=subject_type,
        description=description,
        is_child=True,
        is_parent=False,
        is_active=True,
        episode_nos=[1],
        subject_version=4,
        name_locked=False,
        description_locked=False,
    )


def test_split_explicit_headers_uses_local_path_without_provider(monkeypatch) -> None:
    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="完整剧本",
        script_text="《测试剧》\r\n第1集：相遇\r\n第一集正文\r\n第 2 集 重逢\r\n第二集正文",
        episode_version=3,
    )
    db = _SplitSession(episode)
    stages: list[dict] = []

    async def current(*args, **kwargs) -> bool:
        return True

    async def record_stage(db, script_id, stage, **kwargs) -> None:
        stages.append(kwargs)

    class ForbiddenProvider:
        async def complete_json(self, system: str, user: str) -> dict:
            raise AssertionError("可靠标题拆分不得调用模型")

    monkeypatch.setattr(split, "input_episode_is_current", current)
    monkeypatch.setattr(split, "set_stage", record_stage)

    async def scenario() -> None:
        count = await split.run_split(
            db,
            "script-1",
            "episode-1",
            input_version=3,
            provider=ForbiddenProvider(),
        )
        assert count == 2
        assert db.rollbacks == 0
        assert len(db.added) == 1
        assert stages[-1]["detail"]["split_strategy"] == "explicit_headers"

    asyncio.run(scenario())


def test_extract_runs_with_bounded_concurrency_and_stable_order(monkeypatch) -> None:
    episodes = [
        SimpleNamespace(
            episode_id=f"episode-{episode_no}",
            episode_no=episode_no,
            title=f"第{episode_no}集",
            script_text=f"正文{episode_no}",
        )
        for episode_no in (3, 1, 2)
    ]
    db = _QuerySession(episodes)
    stage_writes: list[dict] = []

    async def current(*args, **kwargs) -> bool:
        return True

    async def record_stage(db, script_id, stage, **kwargs) -> None:
        stage_writes.append(kwargs)

    class ConcurrentProvider:
        def __init__(self) -> None:
            self.active = 0
            self.peak = 0

        async def complete_json(self, system: str, user: str) -> dict:
            episode_no = int(re.search(r"正文(\d+)", user).group(1))  # type: ignore[union-attr]
            self.active += 1
            self.peak = max(self.peak, self.active)
            await asyncio.sleep(0.01 * (4 - episode_no))
            self.active -= 1
            return {
                "schema_version": "pipeline.v1",
                "subjects": [
                    {
                        "name": f"角色{episode_no}",
                        "type": "character",
                        "description": f"第{episode_no}集角色",
                        "is_child": True,
                        "episode_nos": [episode_no],
                    }
                ],
            }

    provider = ConcurrentProvider()
    monkeypatch.setattr(extract, "input_episode_is_current", current)
    monkeypatch.setattr(extract, "set_stage", record_stage)
    monkeypatch.setattr(extract.settings, "pipeline_extract_concurrency", 2)

    async def scenario() -> None:
        result = await extract.run_extract(db, "script-1", provider=provider)
        assert provider.peak == 2
        assert db.rollbacks == 0
        assert [item["episode_no"] for item in result["episodes"]] == [1, 2, 3]
        assert [item["episode_no"] for item in stage_writes[-1]["detail"]["episodes"]] == [
            1,
            2,
            3,
        ]

    asyncio.run(scenario())


def test_extract_recovers_completed_episode_from_same_input_checkpoint() -> None:
    snapshots = [
        {
            "episode_id": "episode-1",
            "episode_no": 1,
            "title": "新标题",
            "script_text": "正文一",
        },
        {
            "episode_id": "episode-2",
            "episode_no": 2,
            "title": "第二集",
            "script_text": "正文二",
        },
    ]
    detail = {
        "input_version": 7,
        "input_client_save_seq": 9,
        "episodes": [
            {
                "episode_id": "episode-1",
                "episode_no": 1,
                "title": "旧标题",
                "extract_status": "succeeded",
                "subjects": [{"name": "已完成角色"}],
            }
        ],
    }

    recovered = extract._recover_completed_episodes(
        detail,
        snapshots,
        input_version=7,
        input_client_save_seq=9,
    )

    assert list(recovered) == [(1, "episode-1")]
    assert recovered[(1, "episode-1")]["title"] == "新标题"
    assert (
        extract._recover_completed_episodes(
            detail,
            snapshots,
            input_version=8,
            input_client_save_seq=9,
        )
        == {}
    )


def test_merge_defaults_to_deterministic_baseline_without_model(monkeypatch) -> None:
    extract_stage = SimpleNamespace(
        detail={
            "episodes": [
                {
                    "episode_no": 2,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "红衣主角，神情坚定",
                        }
                    ],
                },
                {
                    "episode_no": 1,
                    "subjects": [
                        {
                            "name": "主角",
                            "type": "character",
                            "description": "红衣主角",
                        }
                    ],
                },
            ]
        }
    )
    published: list[dict] = []
    stage = AsyncMock()

    async def current(*args, **kwargs) -> bool:
        return True

    async def prepare(db, script_id, subjects):
        published.extend(subjects)
        return subjects

    monkeypatch.setattr(merge, "input_episode_is_current", current)
    monkeypatch.setattr(merge, "get_stage", AsyncMock(return_value=extract_stage))
    monkeypatch.setattr(merge, "_prepare_staged_subjects", prepare)
    monkeypatch.setattr(merge.asset_crud, "persist_subject_suggestions", AsyncMock())
    monkeypatch.setattr(merge, "replace_subjects", AsyncMock())
    monkeypatch.setattr(merge, "set_stage", stage)

    async def scenario() -> None:
        assert await merge.run_merge(_QuerySession([]), "script-1") == 1
        assert published[0]["episode_nos"] == [1, 2]
        assert published[0]["description"] == "红衣主角，神情坚定"
        assert stage.await_args.kwargs["detail"]["merge_mode"] == "deterministic"

    asyncio.run(scenario())


def test_merge_stages_only_subject_version_without_enrich_snapshot() -> None:
    subject = _subject("subject-1", "主角", "character", "红衣角色")
    db = _QuerySession([subject])

    async def scenario() -> None:
        unchanged = await merge._prepare_staged_subjects(
            db,
            "script-1",
            [
                {
                    "name": "主角",
                    "type": "character",
                    "description": "红衣角色",
                    "is_child": True,
                    "episode_nos": [1],
                }
            ],
        )
        changed = await merge._prepare_staged_subjects(
            db,
            "script-1",
            [
                {
                    "name": "主角",
                    "type": "character",
                    "description": "红衣角色，神情坚定",
                    "is_child": True,
                    "episode_nos": [1],
                }
            ],
        )
        assert unchanged[0]["subject_version"] == 4
        assert changed[0]["subject_version"] == 5
        assert {"base_enrich_version", "enrich_version"}.isdisjoint(unchanged[0])
        assert {"base_enrich_version", "enrich_version"}.isdisjoint(changed[0])

    asyncio.run(scenario())


def test_parents_defaults_to_safe_deterministic_grouping(monkeypatch) -> None:
    subjects = [
        _subject("character-young", "王贵发（青年）", "character", "青年形态"),
        _subject("character-old", "王贵发（老年）", "character", "老年形态"),
        _subject("scene-day", "办公室-内-日", "scene", "白天办公室"),
        _subject("scene-night", "办公室-内-夜", "scene", "夜间办公室"),
        _subject("prop-key", "钥匙", "prop", "铜钥匙"),
    ]
    publish = AsyncMock()
    stage = AsyncMock()

    async def current(*args, **kwargs) -> bool:
        return True

    monkeypatch.setattr(parents, "input_episode_is_current", current)
    monkeypatch.setattr(parents, "replace_parent_subjects", publish)
    monkeypatch.setattr(parents, "set_stage", stage)

    async def scenario() -> None:
        assert await parents.run_parents(_QuerySession(subjects), "script-1") == 3
        grouped = publish.await_args.args[2]
        by_name = {item["name"]: item for item in grouped}
        assert by_name["王贵发"]["child_ids"] == [
            "character-old",
            "character-young",
        ]
        assert by_name["办公室"]["type"] == "background"
        assert by_name["办公室"]["child_count"] == 2
        assert by_name["钥匙"]["child_ids"] == ["prop-key"]
        assert stage.await_args.kwargs["detail"]["grouping_mode"] == "deterministic"

    asyncio.run(scenario())
