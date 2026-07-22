"""生图失败分类与安全日志回归测试。"""

import logging

from app.models.generation_job import GenerationJob
from app.providers.registry import ProviderConfigurationError
from app.workers import generation as generation_worker


def test_generation_error_type_distinguishes_provider_configuration() -> None:
    assert (
        generation_worker._generation_error_type(
            ProviderConfigurationError("Authorization=secret-token")
        )
        == "provider_configuration_error"
    )
    assert (
        generation_worker._generation_error_type(RuntimeError("upstream body"))
        == "provider_or_storage_error"
    )


def test_generation_exception_log_omits_exception_message(caplog) -> None:
    secret_message = "Authorization=" + "secret-token /private/provider/path"
    with caplog.at_level(logging.ERROR, logger=generation_worker.__name__):
        try:
            raise RuntimeError(secret_message)
        except RuntimeError as exc:
            generation_worker._safe_log_generation_exception(
                exc,
                job_id="job-1",
                model="banana-2",
                stage="execute",
            )

    output = caplog.text
    assert "job_id=job-1" in output
    assert "model=banana-2" in output
    assert "error_class=RuntimeError" in output
    assert "Authorization=secret-token" not in output
    assert "/private/provider/path" not in output


def test_provider_configuration_error_has_safe_public_message() -> None:
    from app.api.v1 import generation_jobs as generation_api

    job = GenerationJob(
        job_id="job-1",
        owner_user_id="user-1",
        material_id="material-1",
        script_id="script-1",
        subject_id="subject-1",
        idempotency_key="key-1",
        request_hash="hash-1",
        input_version=1,
        attempt=1,
        job_type="generate_images",
        status="failed",
        progress=0,
        error_type="provider_configuration_error",
        error_message="Authorization=secret-token",
    )

    assert generation_api._to_out(job).error_message == "生成服务配置异常，请联系管理员"
