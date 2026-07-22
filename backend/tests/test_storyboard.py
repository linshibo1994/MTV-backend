"""T7 分镜 remediation 行为测试。

仅使用 fake / SQLite 级别对象，不连接真实 DB、Redis、AI 或网络。
"""

from __future__ import annotations

import logging
import traceback
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError

from app.core.errors import ConflictError, ForbiddenError, InsufficientCreditsError
from app.models.scene import Scene
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.storyboard_job import StoryboardJob


def _constraint_columns(model) -> list[set[str]]:
    return [
        {column.name for column in constraint.columns}
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]


def _valid_storyboard_payload() -> dict:
    return {
        "scenes": [
            {
                "scene_no": 1,
                "background": "办公室",
                "shot_groups": [
                    {
                        "group_no": "G1",
                        "narrative_purpose": "交代冲突",
                        "duration_seconds": 6,
                        "shots": [
                            {
                                "display_no": 1,
                                "content": "阿明推门进入办公室。",
                                "description": "在【办公室】中采用【平视】拍摄视角，画面主体【阿明】的关键动作是【推门进入】，整体表现的画面是【阿明走进办公室】，表达了【紧张】，整体画面的光影效果是【冷色顶光】，整个镜头的叙事目的是【交代冲突】。",
                                "shot_line": "【镜头1:0s～3s】场景是办公室，全景·平视，固定，冷色顶光，阿明推门进入，紧张，阿明:我们谈谈",
                                "start_seconds": 0,
                                "end_seconds": 3,
                                "characters": ["阿明"],
                                "dialogue_character": "阿明",
                                "props": ["钥匙"],
                                "sounds": ["门响"],
                            },
                            {
                                "display_no": 2,
                                "content": "小林抬头看向阿明。",
                                "description": "在【办公室】中采用【平视】拍摄视角，画面主体【小林】的关键动作是【抬头看向阿明】，整体表现的画面是【小林停下工作】，表达了【迟疑】，整体画面的光影效果是【冷色顶光】，整个镜头的叙事目的是【交代冲突】。",
                                "shot_line": "【镜头2:3s～6s】场景是办公室，中景·平视，固定，冷色顶光，小林抬头，迟疑，小林:",
                                "start_seconds": 3,
                                "end_seconds": 6,
                                "characters": ["小林"],
                                "dialogue_character": "",
                                "props": [],
                                "sounds": [],
                            },
                        ],
                    }
                ],
            }
        ]
    }


def _subject_catalog() -> dict[str, str]:
    return {
        "办公室": "background",
        "阿明": "character",
        "小林": "character",
        "钥匙": "prop",
        "门响": "sound",
    }


def test_storyboard_models_declare_required_state_and_uniqueness() -> None:
    assert {
        "episode_version",
        "input_version",
        "attempt",
        "idempotency_key",
        "lease_owner",
        "lease_token",
        "lease_until",
        "heartbeat_at",
        "started_at",
        "finished_at",
        "error_type",
        "retryable",
        "canceled_at",
        "reservation_id",
        "input_client_save_seq",
    }.issubset(StoryboardJob.__table__.columns.keys())

    assert {"episode_id", "attempt"} in _constraint_columns(StoryboardJob)
    assert {"episode_id", "idempotency_key"} in _constraint_columns(StoryboardJob)
    assert {"episode_id", "scene_no"} in _constraint_columns(Scene)
    assert {"episode_id", "group_index"} in _constraint_columns(ShotGroup)
    assert {"shot_group_id", "display_no"} in _constraint_columns(Shot)
    assert {"shot_group_id", "shot_no"} in _constraint_columns(Shot)


def test_storyboard_contract_accepts_valid_payload_and_rejects_extra_fields() -> None:
    from app.schemas.storyboard_contracts import StoryboardContract, validate_storyboard_contract

    contract = validate_storyboard_contract(_valid_storyboard_payload(), _subject_catalog())
    assert isinstance(contract, StoryboardContract)

    invalid = _valid_storyboard_payload()
    invalid["unexpected"] = True
    with pytest.raises(ValueError):
        validate_storyboard_contract(invalid, _subject_catalog())


def test_storyboard_contract_rejects_empty_scene_groups_and_empty_global_product() -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_contract

    scene_without_groups = _valid_storyboard_payload()
    scene_without_groups["scenes"][0]["shot_groups"] = []
    with pytest.raises(ValueError, match="shot_group"):
        validate_storyboard_contract(scene_without_groups, _subject_catalog())

    empty_product = {"scenes": []}
    with pytest.raises(ValueError, match="scenes"):
        validate_storyboard_contract(empty_product, _subject_catalog())


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: p["scenes"][0].update(scene_no=2), "scene_no"),
        (lambda p: p["scenes"][0]["shot_groups"][0].update(group_no="G2"), "group_no"),
        (lambda p: p["scenes"][0]["shot_groups"][0].update(duration_seconds=16), "duration"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][1].update(display_no=3), "display_no"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][1].update(start_seconds=4), "continuous"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(start_seconds=0.5), "start_seconds"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(end_seconds=0), "greater"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(characters=["外部角色"]), "subject"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(description="自由发挥"), "description"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(shot_line="镜头1 0-3"), "shot_line"),
    ],
)
def test_storyboard_contract_rejects_strict_scene_group_shot_rules(mutate, message) -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_contract

    payload = _valid_storyboard_payload()
    mutate(payload)
    with pytest.raises(ValueError, match=message):
        validate_storyboard_contract(payload, _subject_catalog())


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: p["scenes"][0].update(background="阿明"), "background"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(characters=["门响"]), "character"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(dialogue_character="门响"), "character"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(props=["办公室"]), "prop"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(sounds=["钥匙"]), "sound"),
    ],
)
def test_storyboard_contract_rejects_subject_type_mismatch(mutate, message) -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_contract

    payload = _valid_storyboard_payload()
    mutate(payload)
    with pytest.raises(ValueError, match=message):
        validate_storyboard_contract(payload, _subject_catalog())


def test_prompt_contains_all_nine_constraints() -> None:
    from app.services.storyboard import PROMPT_CONSTRAINTS, _build_group_prompt

    prompt = _build_group_prompt(
        ["【镜头1:0s～3s】场景是办公室，全景·平视，固定，冷色顶光，阿明进入，紧张，阿明:我们谈谈"],
        ["办公室", "阿明"],
        "交代冲突",
    )
    assert len(PROMPT_CONSTRAINTS) == 9
    for index, constraint in enumerate(PROMPT_CONSTRAINTS, start=1):
        assert f"{index}. {constraint}" in prompt


async def test_storyboard_llm_call_uses_shared_provider_factory(monkeypatch) -> None:
    from app.services import storyboard as storyboard_service

    class Provider:
        async def complete_json(self, system: str, user: str) -> dict:
            assert system == "system"
            assert user == "user"
            return {"scenes": []}

    monkeypatch.setattr("app.providers.get_llm_provider", lambda: Provider())
    assert await storyboard_service._call_llm("system", "user") == {"scenes": []}


async def test_storyboard_retries_invalid_contract_within_one_job(monkeypatch) -> None:
    from app.services import storyboard as storyboard_service

    invalid_display_no = _valid_storyboard_payload()
    invalid_display_no["scenes"][0]["shot_groups"][0]["shots"][1]["display_no"] = 3
    invalid_fractional_time = _valid_storyboard_payload()
    invalid_fractional_time["scenes"][0]["shot_groups"][0]["shots"][0]["end_seconds"] = 2.5
    call_llm = AsyncMock(
        side_effect=[
            invalid_display_no,
            invalid_fractional_time,
            _valid_storyboard_payload(),
        ]
    )
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)

    contract = await storyboard_service._call_llm_with_contract(
        "system",
        "user",
        _subject_catalog(),
    )

    assert contract.scenes[0].shot_groups[0].shots[1].display_no == 2
    assert call_llm.await_count == 3


async def test_storyboard_contract_exhaustion_redacts_provider_payload(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from app.schemas.pipeline_contracts import ProviderContractError
    from app.services import storyboard as storyboard_service

    sensitive_sentinel = "STORYBOARD_PRIVATE_DIALOGUE_92f5c4c7"
    invalid_payload = _valid_storyboard_payload()
    invalid_payload["scenes"][0]["shot_groups"][0]["shots"][0][
        "start_seconds"
    ] = sensitive_sentinel
    call_llm = AsyncMock(return_value=invalid_payload)
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)
    caplog.set_level(logging.WARNING, logger=storyboard_service.__name__)

    with pytest.raises(ProviderContractError) as exc_info:
        await storyboard_service._call_llm_with_contract(
            "system",
            "user",
            _subject_catalog(),
        )

    assert call_llm.await_count == 3
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert sensitive_sentinel not in "".join(traceback.format_exception(exc_info.value))
    assert sensitive_sentinel not in caplog.text
    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == storyboard_service.__name__
    ] == [
        "分镜 Provider 第 1/3 次响应未通过契约",
        "分镜 Provider 第 2/3 次响应未通过契约",
        "分镜 Provider 第 3/3 次响应未通过契约",
    ]


def test_storyboard_prompt_and_response_schema_require_integer_continuous_timing() -> None:
    from app.services import storyboard as storyboard_service

    assert "display_no 必须严格为 1, 2, ..., N" in storyboard_service._SYSTEM_PROMPT
    assert "所有时间字段只能使用整数秒" in storyboard_service._SYSTEM_PROMPT
    user_prompt = storyboard_service._build_user_prompt(
        SimpleNamespace(episode_no=1, title="第一集", script_text="剧情"),
        [],
    )
    assert '"display_no": 2' in user_prompt
    assert '"start_seconds": 4' in user_prompt
    assert '"end_seconds": 7' in user_prompt
    shot_properties = storyboard_service._RESPONSE_SCHEMA["properties"]["scenes"]["items"][
        "properties"
    ]["shot_groups"]["items"]["properties"]["shots"]["items"]["properties"]
    assert shot_properties["start_seconds"] == {"type": "integer", "minimum": 0}
    assert shot_properties["end_seconds"] == {"type": "integer", "minimum": 0}


async def test_invalid_provider_contract_does_not_delete_old_storyboard(monkeypatch) -> None:
    from app.schemas.pipeline_contracts import ProviderContractError
    from app.services import storyboard as storyboard_service

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_text="阿明进入办公室。",
        episode_version=7,
    )
    subjects = [
        SimpleNamespace(id="subject-bg", name="办公室", type="background", episode_nos=[1], description="", image_entry_id="img-bg"),
        SimpleNamespace(id="subject-c", name="阿明", type="character", episode_nos=[1], description="", image_entry_id="img-c"),
    ]

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return self._values

    class _Result:
        def __init__(self, scalar=None, scalars=None):
            self._scalar = scalar
            self._scalars = scalars or []

        def scalar_one_or_none(self):
            return self._scalar

        def scalars(self):
            return _Scalars(self._scalars)

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[_Result(episode), _Result(scalars=subjects)]),
        flush=AsyncMock(),
        add=lambda _: None,
    )
    cleanup = AsyncMock()
    call_llm = AsyncMock(return_value={"scenes": []})
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)
    monkeypatch.setattr(storyboard_service, "_cleanup_existing_storyboard", cleanup)

    with pytest.raises(ProviderContractError):
        await storyboard_service.generate_storyboard(db, "script-1", "episode-1", expected_episode_version=7)

    assert call_llm.await_count == 3
    cleanup.assert_not_awaited()
    db.flush.assert_not_awaited()


async def test_materials_are_typed_and_sound_is_audio_not_visual_material() -> None:
    from app.services.storyboard import _classify_materials, _extract_audio_elements

    subjects = [
        SimpleNamespace(id="bg", name="办公室", type="background", image_entry_id="img-bg"),
        SimpleNamespace(id="c1", name="阿明", type="character", image_entry_id="img-c1"),
        SimpleNamespace(id="p1", name="钥匙", type="prop", image_entry_id="img-p1"),
        SimpleNamespace(id="s1", name="门响", type="sound", image_entry_id="img-s1"),
    ]
    materials = _classify_materials(["办公室", "阿明", "钥匙", "门响"], subjects)

    assert [item["name"] for item in materials["background"]] == ["办公室"]
    assert [item["name"] for item in materials["character"]] == ["阿明"]
    assert [item["name"] for item in materials["prop"]] == ["钥匙"]
    assert materials["other"] == []
    assert "门响" not in {item["name"] for group in materials.values() for item in group}

    class _Scalars:
        def all(self):
            return [
                SimpleNamespace(id="audio-c1", subject_id="c1"),
                SimpleNamespace(id="audio-s1", subject_id="s1"),
            ]

    class _Result:
        def scalars(self):
            return _Scalars()

    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    audio = await _extract_audio_elements(
        db,
        [
            {"dialogue_character": "阿明", "sounds": ["门响"]},
            {"dialogue_character": "", "sounds": ["门响"]},
        ],
        subjects,
    )
    assert audio == [
        {"subject_id": "c1", "name": "阿明", "asset_entry_id": "audio-c1", "type": "character"},
        {"subject_id": "s1", "name": "门响", "asset_entry_id": "audio-s1", "type": "sound"},
    ]


async def test_audio_elements_use_approved_audio_asset_not_subject_image(monkeypatch) -> None:
    from app.services import storyboard as storyboard_service

    subjects = [
        SimpleNamespace(id="c1", name="阿明", type="character", image_entry_id="img-c1"),
        SimpleNamespace(id="s1", name="门响", type="sound", image_entry_id="img-sound-wrong"),
    ]
    audio_entries = [
        SimpleNamespace(
            id="audio-c1", subject_id="c1", kind="audio", is_approved=True,
            status="approved", superseded=False,
        ),
        SimpleNamespace(
            id="audio-s1", subject_id="s1", kind="audio", is_approved=True,
            status="approved", superseded=False,
        ),
    ]

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return self._values

    class _Result:
        def scalars(self):
            return _Scalars(audio_entries)

    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    audio = await storyboard_service._extract_audio_elements(
        db,
        [
            {"dialogue_character": "阿明", "sounds": ["门响"]},
            {"dialogue_character": "", "sounds": ["门响"]},
        ],
        subjects,
    )

    assert audio == [
        {"subject_id": "c1", "name": "阿明", "asset_entry_id": "audio-c1", "type": "character"},
        {"subject_id": "s1", "name": "门响", "asset_entry_id": "audio-s1", "type": "sound"},
    ]
    assert "img-sound-wrong" not in {item.get("asset_entry_id") for item in audio}


def test_workbench_materials_normalize_legacy_strings_and_split_audio() -> None:
    from app.crud.shot import _normalise_audio_elements, _to_materials

    materials = _to_materials(
        {
            "character": ["阿明", {"subject_id": "c2", "name": "小林", "asset_entry_id": "img-c2"}],
            "sound": ["门响"],
        }
    )
    assert [item.name for item in materials.character] == ["阿明", "小林"]
    assert materials.character[0].subject_id is None
    assert materials.background == []
    assert not hasattr(materials, "sound")

    assert _normalise_audio_elements({"characters": ["阿明"], "sounds": [{"name": "门响"}]}) == [
        {"name": "阿明", "type": "character"},
        {"name": "门响", "type": "sound"},
    ]


def test_group_sort_key_orders_g2_before_g10() -> None:
    from app.crud.shot import _group_sort_key

    groups = [SimpleNamespace(group_no="G10", group_index=10), SimpleNamespace(group_no="G2", group_index=2)]
    assert [g.group_no for g in sorted(groups, key=_group_sort_key)] == ["G2", "G10"]


async def test_validate_script_episode_distinguishes_cross_script_403_from_missing_404(monkeypatch) -> None:
    from app.api.v1 import shots

    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    cross_episode = SimpleNamespace(episode_id="episode-1", script_id="other-script", episode_version=1)
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(execute=AsyncMock(side_effect=[_Result(script), _Result(None), _Result(cross_episode)]))
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())

    with pytest.raises(ForbiddenError):
        await shots._validate_script_episode(db, user, "script-1", "episode-1")

    missing_db = SimpleNamespace(execute=AsyncMock(side_effect=[_Result(script), _Result(None), _Result(None)]))
    with pytest.raises(HTTPException) as exc_info:
        await shots._validate_script_episode(missing_db, user, "script-1", "missing")
    assert exc_info.value.status_code == 404


async def test_create_storyboard_job_reserves_and_enqueues_in_one_transaction(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    account = SimpleNamespace(id="account-1", status="active")
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

        def scalar_one(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(account),
                _Result(0),
            ]
        ),
        add=lambda _: None,
        flush=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        shots,
        "create_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1",
                attempt=1,
                status="pending",
                reservation_id=None,
                detail=None,
                task_id=None,
            )
        ),
    )
    monkeypatch.setattr(shots, "reserve", AsyncMock(return_value="reservation-1"))
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock(return_value=SimpleNamespace(id="dispatch-1")))

    response = await shots.generate_storyboard(
        "script-1",
        "episode-1",
        3,
        "idem-1",
        db=db,
        user=user,
    )

    shots.reserve.assert_awaited_once()
    assert shots.reserve.await_args.kwargs["commit"] is False
    shots.enqueue_task.assert_awaited_once()
    db.commit.assert_awaited_once()
    assert response.task_id == "dispatch-1"
    assert response.job_id


async def test_generate_storyboard_without_credit_account_rejects_before_enqueue(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(None),
            ]
        ),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(shots, "reserve", AsyncMock())
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    with pytest.raises(InsufficientCreditsError):
        await shots.generate_storyboard(
            "script-1",
            "episode-1",
            3,
            "idem-1",
            db=db,
            user=user,
        )

    shots.reserve.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


async def test_generate_storyboard_requires_nonempty_idempotency_key_before_credit_lookup(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[_Result(script), _Result(episode), _Result(None)]),
        rollback=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(shots, "reserve", AsyncMock())
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    with pytest.raises(ConflictError, match="Idempotency-Key"):
        await shots.generate_storyboard(
            "script-1",
            "episode-1",
            3,
            "   ",
            db=db,
            user=user,
        )

    shots.reserve.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


async def test_storyboard_idempotency_unique_conflict_recovers_winner(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=9,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    account = SimpleNamespace(id="account-1", status="active")
    user = SimpleNamespace(user_id="user-1", enabled=True)
    winner = SimpleNamespace(
        job_id="job-winner",
        task_id="dispatch-winner",
        episode_id="episode-1",
        script_id="script-1",
        status="pending",
        request_hash=shots.storyboard_request_hash(
            script_id="script-1",
            episode_id="episode-1",
            episode_version=3,
            client_save_seq=9,
        ),
    )

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

        def scalar_one(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(account),
                _Result(0),
                _Result(winner),
            ]
        ),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        shots,
        "create_job",
        AsyncMock(side_effect=IntegrityError("insert", {}, Exception())),
    )
    monkeypatch.setattr(shots, "reserve", AsyncMock())
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    response = await shots.generate_storyboard(
        "script-1",
        "episode-1",
        3,
        "idem-race",
        db=db,
        user=user,
    )

    assert response.job_id == "job-winner"
    assert response.task_id == "dispatch-winner"
    shots.reserve.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


def test_storyboard_generate_route_requires_standard_idempotency_header_only() -> None:
    from fastapi.routing import APIRoute

    from app.api.v1.shots import router

    route = next(
        route
        for route in router.routes
        if isinstance(route, APIRoute)
        and route.path == "/scripts/{script_id}/episodes/{episode_id}/generate-storyboard"
    )
    header_params = {param.alias for param in route.dependant.header_params}
    assert "Idempotency-Key" in header_params
    assert "X-Idempotency-Key" not in header_params


async def test_storyboard_job_list_total_counts_all_rows(monkeypatch) -> None:
    from app.api.v1 import shots

    user = SimpleNamespace(user_id="user-1", enabled=True)
    jobs = [
        SimpleNamespace(
            job_id="job-2", task_id=None, episode_id="episode-1", script_id="script-1",
            status="pending", progress=0, progress_stage=None, progress_message=None,
            error_message=None, detail=None,
        )
    ]
    monkeypatch.setattr(shots, "_validate_script_episode", AsyncMock())
    monkeypatch.setattr(shots, "list_jobs_for_episode", AsyncMock(return_value=jobs))
    monkeypatch.setattr(shots, "count_jobs_for_episode", AsyncMock(return_value=3))

    out = await shots.list_storyboard_jobs(
        "script-1",
        "episode-1",
        limit=1,
        offset=1,
        db=SimpleNamespace(),
        user=user,
    )

    assert len(out.items) == 1
    assert out.total == 3


async def test_storyboard_claim_lease_cas_and_cancel() -> None:
    from app.crud.storyboard import cancel_job, claim_job, complete_job

    now = datetime(2026, 7, 17, tzinfo=UTC)
    job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        episode_version=1,
        input_version=1,
        attempt=1,
        status="pending",
        progress=0,
        lease_owner=None,
        lease_token=None,
        lease_until=None,
        heartbeat_at=None,
        started_at=None,
        canceled_at=None,
        finished_at=None,
        detail=None,
    )

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(return_value=_Result(job)),
        get=AsyncMock(return_value=job),
        commit=AsyncMock(),
        flush=AsyncMock(),
    )

    token_1 = await claim_job(db, "job-1", owner="worker-a", now=now, lease_seconds=10)
    assert token_1 and job.status == "running"
    job.lease_until = now + timedelta(seconds=1)
    token_2 = await claim_job(db, "job-1", owner="worker-b", now=now + timedelta(seconds=2), lease_seconds=10)
    assert token_2 and token_2 != token_1
    assert not await complete_job(db, "job-1", token_1, status="succeeded", detail={})
    assert await complete_job(db, "job-1", token_2, status="succeeded", detail={})
    assert job.progress == 100
    assert job.progress_stage == "completed"
    assert job.progress_message == "分镜生成完成"

    job.status = "running"
    job.lease_token = "cancel-token"
    assert await cancel_job(db, "job-1", reason="user request")
    assert job.status == "canceled"
    assert not await complete_job(db, "job-1", "cancel-token", status="succeeded", detail={})


async def test_worker_checks_input_version_before_provider_and_before_commit(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    stale_job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        input_version=1,
        input_client_save_seq=1,
        episode_version=1,
        status="running",
        lease_token="token-1",
        canceled_at=None,
    )
    episode = SimpleNamespace(episode_version=2, client_save_seq=1)
    db = SimpleNamespace(
        get=AsyncMock(return_value=stale_job),
        execute=AsyncMock(),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    generate = AsyncMock()
    monkeypatch.setattr(storyboard_worker.storyboard_service, "generate_storyboard", generate)

    with pytest.raises(RuntimeError, match="input changed"):
        await storyboard_worker._run_claimed_storyboard_job(db, stale_job, "token-1")
    generate.assert_not_awaited()


async def test_worker_rolls_back_when_terminal_cas_fails(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
        input_client_save_seq=4,
        episode_version=7,
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id="reservation-1",
        detail={},
        triggered_by_user_id="user-1",
    )
    episode = SimpleNamespace(episode_version=7, client_save_seq=4)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    monkeypatch.setattr(
        storyboard_worker.storyboard_service,
        "generate_storyboard",
        AsyncMock(return_value={"scenes": 1, "shot_groups": 1, "shots": 2}),
    )
    events: list[str] = []

    async def lock_current(*args, **kwargs):
        events.append("job_lock")
        return job

    async def settle(*args, **kwargs):
        events.append("settle")

    async def terminal_cas(*args, **kwargs):
        events.append("terminal_cas")
        return False

    monkeypatch.setattr(storyboard_worker, "_lock_current_storyboard_job", lock_current)
    monkeypatch.setattr(storyboard_worker, "settle", settle)
    monkeypatch.setattr(storyboard_worker, "heartbeat_job", AsyncMock(return_value=True))
    monkeypatch.setattr(storyboard_worker, "complete_job", terminal_cas)

    with pytest.raises(RuntimeError, match="lease"):
        await storyboard_worker._run_claimed_storyboard_job(db, job, "token-current")

    db.rollback.assert_awaited()
    db.commit.assert_not_awaited()
    assert events == ["job_lock", "settle", "terminal_cas"]


async def test_worker_heartbeats_before_and_after_provider(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
        input_client_save_seq=4,
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id=None,
        detail={},
        triggered_by_user_id="user-1",
    )
    episode = SimpleNamespace(episode_version=7, client_save_seq=4)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    monkeypatch.setattr(
        storyboard_worker.storyboard_service,
        "generate_storyboard",
        AsyncMock(return_value={"scenes": 1, "shot_groups": 1, "shots": 2}),
    )
    heartbeat = AsyncMock(return_value=True)
    monkeypatch.setattr(storyboard_worker, "heartbeat_job", heartbeat)
    monkeypatch.setattr(
        storyboard_worker,
        "_lock_current_storyboard_job",
        AsyncMock(return_value=job),
    )
    monkeypatch.setattr(storyboard_worker, "complete_job", AsyncMock(return_value=True))

    await storyboard_worker._run_claimed_storyboard_job(db, job, "token-current")

    assert heartbeat.await_count >= 2
    assert heartbeat.await_args_list[0].args[:3] == (db, "job-1", "token-current")


async def test_worker_failure_path_does_not_release_or_mark_failed_for_stale_token(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    stale_job = SimpleNamespace(
        job_id="job-1",
        reservation_id="reservation-1",
        lease_token="new-token",
        status="running",
        canceled_at=None,
    )
    sessions = [
        SimpleNamespace(get=AsyncMock(return_value=stale_job), rollback=AsyncMock()),
    ]
    monkeypatch.setattr(storyboard_worker, "release", AsyncMock())

    released = await storyboard_worker._release_reserved_storyboard(
        sessions[0],
        stale_job,
        "分镜生成失败",
        lease_token="old-token",
    )

    assert released is False
    storyboard_worker.release.assert_not_awaited()
    sessions[0].get.assert_awaited_once()


async def test_storyboard_failure_is_atomic_and_stale_worker_cannot_release(
    monkeypatch,
) -> None:
    from app.workers import storyboard as storyboard_worker

    stale_job = SimpleNamespace(
        job_id="job-1",
        status="running",
        lease_token="new-token",
        canceled_at=None,
        reservation_id="reservation-1",
        detail={},
        triggered_by_user_id="user-1",
    )
    db = SimpleNamespace(
        get=AsyncMock(return_value=stale_job),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return None

    release_credit = AsyncMock()
    fail_job = AsyncMock(return_value=True)
    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "release", release_credit)
    monkeypatch.setattr(storyboard_worker, "fail_job_if_current_token", fail_job)

    completed = await storyboard_worker._mark_failed_after_rollback(
        "job-1", RuntimeError("provider failed"), "old-token"
    )

    assert completed is False
    release_credit.assert_not_awaited()
    fail_job.assert_not_awaited()
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


async def test_storyboard_failure_locks_job_before_release_and_checks_terminal_cas(
    monkeypatch,
) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-1",
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id="reservation-1",
        detail={},
        triggered_by_user_id="user-1",
        financial_cleanup_status=None,
        financial_cleanup_error=None,
    )
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return None

    events: list[str] = []

    async def lock_current(*args, **kwargs):
        events.append("job_lock")
        return job

    async def release_credit(*args, **kwargs):
        events.append("release")
        assert args[0] is db

    async def fail_job(*args, **kwargs):
        events.append("terminal_cas")
        assert args[0] is db
        return False

    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "_lock_current_storyboard_job", lock_current)
    monkeypatch.setattr(storyboard_worker, "release", release_credit)
    monkeypatch.setattr(storyboard_worker, "fail_job_if_current_token", fail_job)

    completed = await storyboard_worker._mark_failed_after_rollback(
        "job-1", RuntimeError("provider failed"), "token-current"
    )

    assert completed is False
    assert events == ["job_lock", "release", "terminal_cas"]
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


def test_celery_storyboard_entry_uses_job_id_safe_path(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    safe_runner = AsyncMock(return_value={"ok": True})
    unsafe_runner = AsyncMock()
    monkeypatch.setattr(storyboard_worker, "_run_storyboard_job_by_id", safe_runner, raising=False)
    monkeypatch.setattr(storyboard_worker, "_run_storyboard_generation", unsafe_runner)

    assert storyboard_worker._generate_storyboard_task("script-1", "episode-1", "user-1", "job-1") == {"ok": True}
    safe_runner.assert_awaited_once_with("job-1")
    unsafe_runner.assert_not_awaited()


def test_storyboard_outbox_canvas_signature_helper() -> None:
    from app.workers.storyboard import STORYBOARD_OUTBOX_TASK_TYPE, build_storyboard_canvas

    class _Task:
        def s(self, *args):
            return ("signature", args)

    payload = {
        "script_id": "script-1",
        "episode_id": "episode-1",
        "user_id": "user-1",
        "job_id": "job-1",
    }
    assert STORYBOARD_OUTBOX_TASK_TYPE == "storyboard.generate"
    assert build_storyboard_canvas(payload, _Task()) == (
        "signature",
        ("script-1", "episode-1", "user-1", "job-1"),
    )


def test_cover_uses_true_first_group_and_does_not_skip_to_later_groups() -> None:
    from app.crud.shot import _cover_from_ordered_groups

    groups = [
        SimpleNamespace(reference_frames={"first_frame": None}),
        SimpleNamespace(reference_frames={"first_frame": "later.png"}),
    ]
    assert _cover_from_ordered_groups(groups) is None


def test_cover_falls_back_to_first_group_last_frame_after_first_video_finishes() -> None:
    from app.crud.shot import _cover_from_ordered_groups

    groups = [
        SimpleNamespace(reference_frames={"first_frame": None, "last_frame": "first-video.png"}),
        SimpleNamespace(reference_frames={"first_frame": "later.png", "last_frame": "later-last.png"}),
    ]
    assert _cover_from_ordered_groups(groups) == "first-video.png"


def test_docker_compose_declares_worker_and_beat_topology() -> None:
    from pathlib import Path

    compose = Path("docker-compose.yml").read_text()
    assert "celery-worker:" in compose
    assert "celery-beat:" in compose
    assert "celery -A app.workers.celery_app worker -l info" in compose
    assert "celery -A app.workers.celery_app beat -l info" in compose
