"""主体列表场景编号排序测试。"""

from types import SimpleNamespace

import pytest

from app.services.subject_ordering import scene_number_from_name, sort_scene_subjects


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("场景 7：雨夜码头", 7),
        ("场景编号10-室内", 10),
        ("场景#2", 2),
        ("Scene 12: office", 12),
        ("雨夜码头", None),
        ("场景七：老街", None),
    ],
)
def test_scene_number_from_name(name: str, expected: int | None) -> None:
    assert scene_number_from_name(name) == expected


def test_sort_scene_subjects_uses_numeric_order_and_preserves_other_types() -> None:
    subjects = [
        SimpleNamespace(type="character", name="主角"),
        SimpleNamespace(type="scene", name="场景 10：天台"),
        SimpleNamespace(type="scene", name="场景 2：街道"),
        SimpleNamespace(type="prop", name="手机"),
        SimpleNamespace(type="background", name="场景 7：室内"),
        SimpleNamespace(type="scene", name="未命名场景"),
    ]

    ordered = sort_scene_subjects(subjects)

    assert [subject.name for subject in ordered] == [
        "主角",
        "场景 2：街道",
        "场景 7：室内",
        "手机",
        "场景 10：天台",
        "未命名场景",
    ]


def test_sort_scene_subjects_keeps_equal_numbers_stable() -> None:
    subjects = [
        SimpleNamespace(type="scene", name="场景 2：A"),
        SimpleNamespace(type="scene", name="场景 2：B"),
        SimpleNamespace(type="scene", name="场景 1：C"),
    ]

    ordered = sort_scene_subjects(subjects)

    assert [subject.name for subject in ordered] == [
        "场景 1：C",
        "场景 2：A",
        "场景 2：B",
    ]
