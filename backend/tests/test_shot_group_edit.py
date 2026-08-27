"""正式镜头组编辑接口的最小专项验收。"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.core.errors import BadRequestError, ConflictError
from app.crud.shot import _job_matches_group_snapshot
from app.models.shot_group import shot_group_content_hash
from app.schemas.shot import ShotGroupUpdateIn
from app.services.shot_group_edit import (
    _normalise_audio_elements,
    _normalise_materials,
    _retime_shots,
    _rewrite_prompt_timeline,
    update_shot_group,
)


class _Result:
    def __init__(self, *, scalar=None, scalars=None):
        self.scalar = scalar
        self.scalar_values = scalars

    def scalar_one_or_none(self):
        return self.scalar

    def scalars(self):
        return self

    def all(self):
        return list(self.scalar_values or [])


def _shot(display_no: int, start: int, end: int):
    return SimpleNamespace(
        shot_id=f"shot-{display_no}",
        display_no=display_no,
        start_seconds=start,
        end_seconds=end,
        shot_line=f"【镜头{display_no}:{start}s～{end}s】镜头内容{display_no}",
        content=f"镜头内容{display_no}",
    )


def test_shot_group_update_schema_requires_real_edit() -> None:
    with pytest.raises(ValidationError):
        ShotGroupUpdateIn(expected_episode_version=2)
    with pytest.raises(ValidationError):
        ShotGroupUpdateIn(expected_episode_version=2, duration_seconds=16)
    with pytest.raises(ValidationError):
        ShotGroupUpdateIn(expected_episode_version=2, prompt=None)

    payload = ShotGroupUpdateIn(
        expected_episode_version=2,
        materials={"character": [{"subject_id": "subject-1"}]},
        audio_elements=[],
    )
    assert payload.materials is not None
    assert payload.materials.character[0].subject_id == "subject-1"


def test_retime_shots_and_prompt_timeline_stay_consistent() -> None:
    shots = [_shot(1, 0, 3), _shot(2, 3, 5)]

    _retime_shots(shots, 8)
    prompt = _rewrite_prompt_timeline(
        "【镜头1:0s～3s】第一段\n【镜头2:3s～5s】第二段",
        shots,
    )

    assert [(shot.start_seconds, shot.end_seconds) for shot in shots] == [(0, 5), (5, 8)]
    assert shots[0].shot_line.startswith("【镜头1:0s～5s】")
    assert shots[1].shot_line.startswith("【镜头2:5s～8s】")
    assert "【镜头1:0s～5s】" in prompt
    assert "【镜头2:5s～8s】" in prompt

    with pytest.raises(BadRequestError, match="至少需要 2 秒"):
        _retime_shots(shots, 1)


def test_succeeded_job_validity_uses_per_group_snapshot() -> None:
    updated_at = datetime(2026, 7, 24, 10, 30, tzinfo=UTC)
    group = SimpleNamespace(
        updated_at=updated_at,
        prompt="当前提示词",
        duration_seconds=5,
        video_duration_seconds=5,
        materials={"character": []},
        audio_elements={"characters": [], "sounds": []},
    )
    content_hash = shot_group_content_hash(group)
    assert len(content_hash) == 64

    assert _job_matches_group_snapshot(
        {"shot_group_version": {"shot_group_content_hash": content_hash}},
        group,
    )
    group.prompt = "已编辑提示词"
    assert not _job_matches_group_snapshot(
        {"shot_group_version": {"shot_group_content_hash": content_hash}},
        group,
    )
    assert _job_matches_group_snapshot(
        {"shot_group_version": {"shot_group_updated_at": "旧创建快照"}},
        group,
        finished_at=datetime(2026, 7, 24, 10, 31, tzinfo=UTC),
    )


def test_material_and_audio_references_resolve_approved_assets() -> None:
    character = SimpleNamespace(
        id="subject-character",
        name="老姜",
        type="character",
        image_entry_id="image-approved",
    )
    sound = SimpleNamespace(
        id="subject-sound",
        name="环境音乐",
        type="sound",
        image_entry_id=None,
    )
    image = SimpleNamespace(
        id="image-approved",
        subject_id=character.id,
        kind="image",
    )
    audio = SimpleNamespace(
        id="audio-approved",
        subject_id=sound.id,
        kind="audio",
    )

    materials = _normalise_materials(
        {"character": [{"name": "老姜"}]},
        subjects=[character, sound],
        approved_assets=[image, audio],
    )
    audio_elements = _normalise_audio_elements(
        [{"type": "sound", "name": "环境音乐"}],
        subjects=[character, sound],
        approved_assets=[image, audio],
    )

    assert materials["character"] == [
        {
            "name": "老姜",
            "subject_id": "subject-character",
            "asset_entry_id": "image-approved",
        }
    ]
    assert audio_elements["sounds"] == [
        {
            "name": "环境音乐",
            "type": "sound",
            "subject_id": "subject-sound",
            "asset_entry_id": "audio-approved",
        }
    ]


def test_audio_normalisation_preserves_multiple_approved_entries_for_one_subject() -> None:
    character = SimpleNamespace(
        id="subject-character",
        name="老姜",
        type="character",
        image_entry_id=None,
    )
    first_audio = SimpleNamespace(
        id="audio-approved-1",
        subject_id=character.id,
        kind="audio",
    )
    second_audio = SimpleNamespace(
        id="audio-approved-2",
        subject_id=character.id,
        kind="audio",
    )

    audio_elements = _normalise_audio_elements(
        [
            {
                "type": "character",
                "subject_id": character.id,
                "asset_entry_id": first_audio.id,
            },
            {
                "type": "character",
                "subject_id": character.id,
                "asset_entry_id": second_audio.id,
            },
        ],
        subjects=[character],
        approved_assets=[first_audio, second_audio],
    )

    assert audio_elements["characters"] == [
        {
            "name": "老姜",
            "type": "character",
            "subject_id": "subject-character",
            "asset_entry_id": "audio-approved-1",
        },
        {
            "name": "老姜",
            "type": "character",
            "subject_id": "subject-character",
            "asset_entry_id": "audio-approved-2",
        },
    ]


async def test_update_shot_group_persists_fields_and_increments_version(monkeypatch) -> None:
    from app.services.runtime_models import RuntimeVideoModel

    monkeypatch.setattr(
        "app.services.runtime_models.resolve_video_model",
        AsyncMock(
            return_value=RuntimeVideoModel(
                provider="volcengine",
                model="doubao-seedance-test",
                reference_image_audit_required=True,
                task_provider_key="modelsai",
            )
        ),
    )
    episode = SimpleNamespace(episode_version=4)
    group = SimpleNamespace(
        shot_group_id="group-1",
        unit_id="group:group-1",
        duration_seconds=5,
        video_duration_seconds=5,
        prompt="【镜头1:0s～3s】第一段\n【镜头2:3s～5s】第二段",
        materials={},
        audio_elements={},
    )
    shots = [_shot(1, 0, 3), _shot(2, 3, 5)]
    character = SimpleNamespace(
        id="subject-1",
        name="老姜",
        type="character",
        image_entry_id="image-1",
    )
    image = SimpleNamespace(
        id="image-1",
        subject_id="subject-1",
        kind="image",
    )
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=episode),
                _Result(scalar=group),
                _Result(scalars=shots),
                _Result(scalars=[character]),
                _Result(scalars=[image]),
                _Result(scalar=None),
            ]
        ),
        flush=AsyncMock(),
        refresh=AsyncMock(),
    )

    result = await update_shot_group(
        db,
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="group-1",
        expected_episode_version=4,
        fields_set={"duration_seconds", "prompt", "materials", "audio_elements"},
        duration_seconds=8,
        prompt="【镜头1:0s～3s】新第一段\n【镜头2:3s～5s】新第二段",
        materials={"character": [{"name": "老姜"}]},
        audio_elements=[],
    )

    assert result.episode_version == 5
    assert episode.episode_version == 5
    assert group.duration_seconds == group.video_duration_seconds == 8
    assert shots[-1].end_seconds == 8
    assert "【镜头2:5s～8s】" in group.prompt
    assert group.materials["character"][0]["subject_id"] == "subject-1"
    assert group.audio_elements == {"characters": [], "sounds": []}
    db.flush.assert_awaited_once()
    db.refresh.assert_awaited_once_with(group, attribute_names=["updated_at"])


async def test_update_shot_group_same_value_is_noop() -> None:
    episode = SimpleNamespace(episode_version=4)
    group = SimpleNamespace(
        shot_group_id="group-1",
        unit_id="group:group-1",
        duration_seconds=5,
        video_duration_seconds=5,
        prompt="不变的提示词",
    )
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=episode),
                _Result(scalar=group),
                _Result(scalars=[_shot(1, 0, 5)]),
            ]
        ),
        flush=AsyncMock(),
        refresh=AsyncMock(),
    )

    result = await update_shot_group(
        db,
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="group-1",
        expected_episode_version=4,
        fields_set={"duration_seconds", "prompt"},
        duration_seconds=5,
        prompt="不变的提示词",
    )

    assert result.episode_version == 4
    assert result.changed_fields == ()
    db.flush.assert_not_awaited()
    db.refresh.assert_not_awaited()


async def test_update_shot_group_rejects_version_conflict_and_active_video() -> None:
    version_db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=SimpleNamespace(episode_version=5)),
            ]
        )
    )
    with pytest.raises(ConflictError, match="episode_version"):
        await update_shot_group(
            version_db,
            script_id="script-1",
            episode_id="episode-1",
            shot_group_id="group-1",
            expected_episode_version=4,
            fields_set={"prompt"},
            prompt="新提示词",
        )

    group = SimpleNamespace(
        shot_group_id="group-1",
        unit_id="group:group-1",
        prompt="旧提示词",
    )
    active_db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=SimpleNamespace(episode_version=4)),
                _Result(scalar=group),
                _Result(scalars=[_shot(1, 0, 5)]),
                _Result(scalar="job-processing"),
            ]
        )
    )
    with pytest.raises(ConflictError, match="正在生成视频"):
        await update_shot_group(
            active_db,
            script_id="script-1",
            episode_id="episode-1",
            shot_group_id="group-1",
            expected_episode_version=4,
            fields_set={"prompt"},
            prompt="新提示词",
        )
