"""流水线 LLM 输出的 Pydantic v2 合约。"""

from collections.abc import Callable, Sequence
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

    def __init__(
        self,
        message: str,
        *,
        diagnostic: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


class RetryableProviderContractError(ConnectionError):
    """尚有持久 attempt 预算的供应商合约错误。"""

    error_type = "provider_contract_error"
    retryable = True

    def __init__(
        self,
        message: str,
        *,
        diagnostic: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


_PROVIDER_DIAGNOSTIC_CATEGORIES = frozenset({"invalid_json", "non_object_json"})
_DIAGNOSTIC_FIELDS = frozenset(
    {
        "schema_version",
        "episodes",
        "episode_no",
        "title",
        "script_text",
        "subjects",
        "merged_subjects",
        "supplemental_subjects",
        "total_count",
        "name",
        "type",
        "description",
        "is_child",
        "episode_nos",
        "evidence",
        "quote",
        "parent_subjects",
        "child_count",
        "child_ids",
    }
)


def _diagnostic_path(loc: tuple[object, ...]) -> str:
    """只保留合约字段和数组位置，避免错误诊断泄露模型输出。"""
    parts: list[str] = []
    for item in loc:
        if isinstance(item, int):
            if parts:
                parts[-1] = f"{parts[-1]}[]"
            continue
        if isinstance(item, str) and item in _DIAGNOSTIC_FIELDS:
            parts.append(item)
        else:
            parts.append("<unknown_field>")
    return ".".join(parts) or "<root>"


def _contract_diagnostic(schema: type[BaseModel], exc: ValueError) -> dict[str, object]:
    """构造可展示但不含输入值、异常原文或提示词的合约失败摘要。"""
    provider_diagnostic = getattr(exc, "diagnostic", None)
    if (
        isinstance(provider_diagnostic, dict)
        and provider_diagnostic.get("category") in _PROVIDER_DIAGNOSTIC_CATEGORIES
        and isinstance(provider_diagnostic.get("response"), dict)
    ):
        response = provider_diagnostic["response"]
        return {
            "version": 1,
            "category": provider_diagnostic["category"],
            "schema": schema.__name__,
            "response": {
                "kind": response.get("kind"),
                "length_bucket": response.get("length_bucket"),
                "top_level_kind": response.get("top_level_kind"),
            },
        }
    if isinstance(exc, ValidationError):
        errors = exc.errors(include_url=False)
        codes = sorted({str(item.get("type", "validation_error")) for item in errors})[:8]
        paths = list(dict.fromkeys(_diagnostic_path(tuple(item.get("loc", ()))) for item in errors))[:8]
        error_count = min(len(errors), 8)
        category = "schema_validation"
    else:
        codes = ["semantic_validation"]
        paths = ["<root>"]
        error_count = 1
        category = "semantic_validation"
    return {
        "version": 1,
        "category": category,
        "schema": schema.__name__,
        "validation": {
            "error_count": error_count,
            "codes": codes,
            "paths": paths,
        },
    }


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
    last_diagnostic: dict[str, object] | None = None
    calls = 1 if durable_attempt is not None else max_attempts
    for _ in range(calls):
        try:
            payload = await provider.complete_json(system, user)
            contract = schema.model_validate(payload)
            if validator is not None:
                validator(contract)
            return contract
        except (ValidationError, ValueError) as exc:
            last_error = exc
            last_diagnostic = _contract_diagnostic(schema, exc)
    if last_error is None:
        raise RuntimeError("Provider 合约校验未执行")
    if durable_attempt is not None and durable_attempt < max_attempts:
        raise RetryableProviderContractError(
            f"供应商第 {durable_attempt} 次响应不符合 {schema.__name__} 合约",
            diagnostic=last_diagnostic,
        ) from last_error
    raise ProviderContractError(
        f"供应商响应在 {max_attempts} 次总预算内不符合 {schema.__name__} 合约",
        diagnostic=last_diagnostic,
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


class SupplementalEvidence(StrictPipelineModel):
    """合并阶段补漏主体的原文证据。"""

    episode_no: PositiveEpisodeNo
    quote: NonEmptyText


class SupplementalSubject(PipelineSubject):
    """合并阶段从原文补出的主体，必须具备可验证证据。"""

    evidence: list[SupplementalEvidence] = Field(min_length=1)


def _subject_keys_are_unique(subjects: Sequence[PipelineSubject]) -> None:
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
