"""流水线 LLM 输出的 Pydantic v2 合约。"""

from collections.abc import Callable
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)


class ProviderContractError(ValueError):
    """供应商连续返回不符合阶段合约的 JSON。"""

    error_type = "provider_contract_error"
    retryable = False


class RetryableProviderContractError(ConnectionError):
    """尚有持久 attempt 预算的供应商合约错误。"""

    error_type = "provider_contract_error"
    retryable = True


async def complete_json[ContractT: BaseModel](
    provider,
    system: str,
    user: str,
    schema: type[ContractT],
    *,
    max_attempts: int = 3,
    durable_attempt: int | None = None,
    validator: Callable[[ContractT], None] | None = None,
) -> ContractT:
    """调用供应商并校验结构；worker 模式下每个持久 attempt 仅外呼一次。"""
    if max_attempts < 1:
        raise ValueError("max_attempts 必须大于 0")
    if durable_attempt is not None and not 1 <= durable_attempt <= max_attempts:
        raise ValueError("durable_attempt 超出 Provider 总预算")
    last_error: ValueError | None = None
    calls = 1 if durable_attempt is not None else max_attempts
    for _ in range(calls):
        payload = await provider.complete_json(system, user)
        try:
            contract = schema.model_validate(payload)
            if validator is not None:
                validator(contract)
            return contract
        except (ValidationError, ValueError) as exc:
            last_error = exc
    if last_error is None:
        raise RuntimeError("Provider 合约校验未执行")
    if durable_attempt is not None and durable_attempt < max_attempts:
        raise RetryableProviderContractError(
            f"供应商第 {durable_attempt} 次响应不符合 {schema.__name__} 合约"
        ) from last_error
    raise ProviderContractError(
        f"供应商响应在 {max_attempts} 次总预算内不符合 {schema.__name__} 合约"
    ) from last_error

SubjectType = Literal["character", "scene", "prop", "sound"]
ParentType = Literal["character", "background", "scene", "prop", "sound"]
PipelineVersion = Literal["pipeline.v1"]
NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
ShortNonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]
PositiveEpisodeNo = Annotated[int, Field(strict=True, ge=1)]
_SECTIONS = (
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
)


class StrictPipelineModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class SplitEpisode(StrictPipelineModel):
    episode_no: PositiveEpisodeNo
    title: ShortNonEmptyText
    script_text: NonEmptyText


class SplitOutput(StrictPipelineModel):
    schema_version: PipelineVersion
    episodes: list[SplitEpisode] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_episode_numbers(self) -> "SplitOutput":
        numbers = [episode.episode_no for episode in self.episodes]
        expected = list(range(1, len(numbers) + 1))
        if numbers != expected:
            raise ValueError("episode_no 必须从 1 开始连续编号")
        return self


class PipelineSubject(StrictPipelineModel):
    name: ShortNonEmptyText
    type: SubjectType
    description: NonEmptyText
    is_child: bool
    episode_nos: list[PositiveEpisodeNo] = Field(min_length=1)

    @model_validator(mode="after")
    def child_and_episode_numbers_are_valid(self) -> "PipelineSubject":
        if not self.is_child:
            raise ValueError("流水线输出必须是子主体")
        if len(self.episode_nos) != len(set(self.episode_nos)):
            raise ValueError("episode_nos 不得重复")
        return self


def _subject_keys_are_unique(subjects: list[PipelineSubject]) -> None:
    keys = [(subject.type, subject.name) for subject in subjects]
    if len(keys) != len(set(keys)):
        raise ValueError("同名同类主体不得重复")


class ExtractOutput(StrictPipelineModel):
    schema_version: PipelineVersion
    subjects: list[PipelineSubject]

    @model_validator(mode="after")
    def subjects_are_unique(self) -> "ExtractOutput":
        _subject_keys_are_unique(self.subjects)
        return self


class MergeOutput(StrictPipelineModel):
    schema_version: PipelineVersion
    merged_subjects: list[PipelineSubject]
    total_count: int = Field(ge=0)

    @model_validator(mode="after")
    def total_matches_subjects(self) -> "MergeOutput":
        if self.total_count != len(self.merged_subjects):
            raise ValueError("total_count 必须与 merged_subjects 数量一致")
        _subject_keys_are_unique(self.merged_subjects)
        return self


class ResolvedPrompts(StrictPipelineModel):
    final: NonEmptyText

    @model_validator(mode="after")
    def contains_twelve_nonempty_ordered_sections(self) -> "ResolvedPrompts":
        positions: list[int] = []
        for label in _SECTIONS:
            marker = f"【{label}】"
            position = self.final.find(marker)
            if position < 0:
                raise ValueError(f"缺少 prompt 段落：{label}")
            positions.append(position)
        if positions != sorted(positions):
            raise ValueError("prompt 段落顺序错误")
        for index, position in enumerate(positions):
            next_position = positions[index + 1] if index + 1 < len(positions) else len(self.final)
            if not self.final[position + len(_SECTIONS[index]) + 2 : next_position].strip():
                raise ValueError(f"prompt 段落内容为空：{_SECTIONS[index]}")
        return self


class EnrichOutput(StrictPipelineModel):
    schema_version: PipelineVersion
    resolved_prompts: ResolvedPrompts


class ParentSubjectContract(StrictPipelineModel):
    name: ShortNonEmptyText
    type: ParentType
    description: NonEmptyText
    child_count: int = Field(ge=1)
    child_ids: list[NonEmptyText] = Field(min_length=1)

    @model_validator(mode="after")
    def children_match_count(self) -> "ParentSubjectContract":
        if len(self.child_ids) != self.child_count or len(set(self.child_ids)) != len(self.child_ids):
            raise ValueError("child_ids 必须唯一且与 child_count 一致")
        return self


class ParentsOutput(StrictPipelineModel):
    schema_version: PipelineVersion
    parent_subjects: list[ParentSubjectContract]


def should_enrich_subject(subject_type: str) -> bool:
    """声音主体不进入视觉提示词增强。"""
    return subject_type != "sound"
