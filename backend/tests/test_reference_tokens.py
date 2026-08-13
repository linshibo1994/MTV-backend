"""稳定参考图 token 映射块测试。"""

from app.services.reference_tokens import (
    bind_reference_tokens_to_prompt,
    remove_reference_token_block,
)


def test_reference_token_block_preserves_final_order_and_cross_type_names() -> None:
    token_map = {
        "图1": {"ref": None, "name": "首帧参考", "type": "first_frame"},
        "图2": {"ref": "C01", "name": "晨曦", "type": "character"},
        "图3": {"ref": "BG01", "name": "晨曦", "type": "background"},
    }
    prompt = bind_reference_tokens_to_prompt("镜头正文\n【约束】保持一致", token_map)
    assert prompt.index("图1: 首帧参考") < prompt.index("图2: 晨曦")
    assert prompt.index("图2: 晨曦") < prompt.index("图3: 晨曦")
    assert "短引用 C01；类型 character" in prompt
    assert "短引用 BG01；类型 background" in prompt
    assert prompt.index("## 图片内容说明") < prompt.index("镜头正文")
    assert prompt.index("镜头正文") < prompt.index("【约束】")


def test_reference_token_block_replaces_stale_mapping_and_omits_missing_image() -> None:
    stale = bind_reference_tokens_to_prompt(
        "镜头正文\n【约束】保持一致",
        {"图1": {"ref": "P01", "name": "缺图道具", "type": "prop"}},
    )
    rebound = bind_reference_tokens_to_prompt(
        stale,
        {"图1": {"ref": "C01", "name": "有图角色", "type": "character"}},
    )
    assert rebound.count("## 图片内容说明\n") == 1
    assert "P01" not in rebound
    assert "C01" in rebound
    assert remove_reference_token_block(rebound) == "镜头正文\n【约束】保持一致"
