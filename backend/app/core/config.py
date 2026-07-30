"""应用配置 — 通过环境变量注入，默认值与 .env.example 对齐。"""

import ipaddress
from collections import Counter
from functools import lru_cache
from math import log2
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, PrivateAttr, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    _explicit_fields: frozenset[str] = PrivateAttr(default_factory=frozenset)

    # 应用
    app_env: Literal["dev", "test", "prod", "production"] = "dev"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    cors_methods: list[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
    )
    cors_headers: list[str] = Field(
        default_factory=lambda: [
            "Accept",
            "Authorization",
            "Content-Type",
            "Idempotency-Key",
            "Range",
            "X-Requested-With",
        ]
    )

    # 数据库 / 缓存
    database_url: str = "postgresql+asyncpg://flow:flow@localhost:55432/flowapp"
    database_sync_url: str = "postgresql+psycopg2://flow:flow@localhost:55432/flowapp"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # 会话
    session_secret: str = "dev-only-change-me"
    session_cookie_name: str = "flowapp_session"
    session_max_age: int = 86400
    tgw_l7_route_cookie: str = "flowapp-tgw"

    # 存储
    storage_driver: Literal["local", "s3", "meizi"] = "local"
    storage_root: str = "./storage"
    s3_endpoint: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "flowapp"
    s3_region: str = "us-east-1"
    # 公司公共媒资中心。业务服务不持有 OSS AK/SK，仅使用 MeiZi 签发的临时上传凭证。
    meizi_base_url: str = ""
    # 切换 MeiZi 后仅为“尚无远端映射”的历史对象保留本地读/删兼容；
    # 所有新 put/put_file 仍强制上传 MeiZi，完成存量迁移后可关闭。
    meizi_legacy_local_fallback: bool = False
    meizi_upload_user_name: str = ""
    meizi_upload_tenant_id: int = 0
    meizi_upload_group_id: int = 0
    meizi_upload_source: int = 0
    # Java TokenRequest 将上传盐硬编码在源码；Python 改为 SecretStr 环境注入。
    meizi_upload_salt: SecretStr = SecretStr("")
    # info/shiftDelete 使用 frontupload 身份，不能与上传身份混为一套配置。
    meizi_front_user_name: str = ""
    meizi_front_tenant_id: int = 0
    meizi_front_group_id: int = 0
    meizi_front_source: int = 0
    meizi_front_salt: SecretStr = SecretStr("")
    # 为空时接受任意通过公网 DNS/IP 校验的 HTTPS 主机；生产可进一步收窄。
    meizi_upload_allowed_hosts: list[str] = Field(default_factory=list)
    meizi_public_url_allowed_hosts: list[str] = Field(default_factory=list)
    meizi_lib_type_file: int = 704
    meizi_lib_type_image: int = 703
    meizi_lib_type_video: int = 701
    meizi_lib_type_audio: int = 702
    meizi_connect_timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    meizi_read_timeout_seconds: float = Field(default=600.0, gt=0, le=1800)
    meizi_write_timeout_seconds: float = Field(default=600.0, gt=0, le=1800)
    meizi_processing_poll_attempts: int = Field(default=10, ge=1, le=60)
    meizi_processing_poll_interval_seconds: float = Field(default=2.0, ge=0, le=30)
    # uploadToken 身份已落库但进程中断，或远端删除暂时失败时，后台任务会在
    # 该静默窗口后重新领取并删除；默认一小时高于单次上传/处理超时。
    meizi_cleanup_stale_seconds: int = Field(default=3600, ge=60, le=86400)
    meizi_cleanup_batch_size: int = Field(default=20, ge=1, le=200)

    # OpenAI (旧字段，保留作新能力配置为空时的回退)
    openai_api_key: str = ""
    openai_llm_model: str = "gpt-4o"
    openai_image_model: str = "gpt-image-2"

    # Google Gemini (旧字段，图片 / Veo 视频)
    google_api_key: str = ""
    google_image_model: str = "gemini-3.1-flash-image"
    google_video_model: str = "veo-3.1-generate-preview"

    # 字节豆包 Seedance（旧 AK/SK 字段已废弃，代码不再读取，仅保留用于存量 .env 兼容）
    volcengine_access_key: str = ""
    volcengine_secret_key: str = ""
    volcengine_region: str = "cn-north-1"
    # veImageX 单图审核（视频生成参考图预检）。使用独立 ImageX AK/SK，绝不复用
    # 方舟视频 API Key；审核失败或配置缺失时默认阻断生成。
    volc_image_audit_enabled: bool = True
    volc_imagex_access_key: str = ""
    volc_imagex_secret_key: str = ""
    volc_imagex_service_id: str = ""
    # 仅用于部署独立的、允许 veImageX 拉取的审核下载服务；本地鉴权媒体代理
    # 的相对 URL 不会自动拼接为公网地址，必须提供真正的 HTTPS 预签名 URL。
    volc_image_audit_public_base_url: str = ""
    # 火山方舟 Ark 大模型专用 API Key（Bearer Token 鉴权，Seedance 2.0 视频用）
    volcengine_api_key: str = ""
    volcengine_ark_base_url: str = "https://ark.cn-beijing.volces.com/api/v3"
    seedance_model: str = "doubao-seedance-2-0-260128"
    video_default_provider: str = "modelsai"

    # ===== 统一能力×厂商适配层（每个能力独立配置厂商 URL+Key） =====
    # 厂商切换：在 .env 设 *_PROVIDER 即可，无需改代码。
    # 对应厂商：openai | deepseek | bailian | claude | gemini
    # base_url 留空则使用 provider 内置默认值（见各 adapter 的 _DEFAULT_BASE_URL）。

    # 文本 LLM —— 结构化单轮补全，用于 pipeline split/extract/merge/enrich/parents
    text_llm_provider: str = "openai"
    text_llm_base_url: str = ""
    text_llm_api_key: str = ""
    text_llm_model: str = ""

    # 对话 LLM —— 多轮对话 (chat)
    chat_llm_provider: str = "openai"
    chat_llm_base_url: str = ""
    chat_llm_api_key: str = ""
    chat_llm_model: str = ""

    # 图片生成 —— openai | google | bailian
    image_gen_provider: str = "openai"
    image_gen_base_url: str = ""
    image_gen_api_key: str = ""
    image_gen_model: str = ""

    # 视频生成 —— volcengine | bailian | openai | gemini
    # database 保持历史后台切换；env 让 VIDEO_GEN_* 成为权威配置。
    video_provider_selection_mode: Literal["database", "env"] = "database"
    video_gen_provider: str = "volcengine"
    video_gen_base_url: str = ""
    video_gen_api_key: str = ""
    video_gen_model: str = ""
    # 百炼 DashScope（文生图/视频/LLM 共用同一 API Key）
    dashscope_api_key: str = ""
    dashscope_base_url: str = "https://dashscope.aliyuncs.com"
    dashscope_video_model: str = "wanx2.1-t2v-turbo"
    # Anthropic Claude
    anthropic_api_key: str = ""
    anthropic_base_url: str = "https://api.anthropic.com"
    anthropic_model: str = "claude-sonnet-5"
    # DeepSeek
    deepseek_api_key: str = ""
    deepseek_model: str = "deepseek-v4-flash"
    # Gemini（LLM 走 OpenAI 兼容端点时用）
    gemini_api_key: str = ""
    gemini_llm_model: str = "gemini-2.5-flash"
    openai_video_model: str = "sora-2"

    # 积分定价
    price_extract_per_episode: int = 10
    price_merge: int = 20
    price_enrich_per_subject: int = 5
    price_image_gen: int = 30
    price_storyboard_per_episode: int = 50
    price_video_gen: int = 900
    # 剧本分集主体提取的模型并发上限，避免串行等待同时限制供应商压力。
    pipeline_extract_concurrency: int = Field(default=4, ge=1, le=16)
    # 视觉主体增强的模型并发上限；可按供应商限流能力在部署环境中调整。
    pipeline_enrich_concurrency: int = Field(default=6, ge=1, le=16)
    # 视频生成冻结量（doc 08 §2.4：reserved frozen=210、settle actual=784，冻结与实际分离）
    price_video_gen_frozen: int = 210

    # 媒体代理
    media_proxy_prefix: str = "/api/x/cctv/zhihui/flow-app"
    # 上传体积上限（字节），超出提前 413 拒绝，避免一次性读入内存导致 OOM
    max_upload_bytes: int = 512 * 1024 * 1024  # 默认 512MiB
    # 代理读取体积上限（字节），超出拒绝整文件读取；Range 请求仍按分段流式返回
    max_media_read_bytes: int = 2 * 1024 * 1024 * 1024  # 默认 2GiB

    # 权限（08§1.3 矩阵"可配置"项）
    # Editor 审核资产(REVIEW)默认禁止，部署侧可经环境变量放开
    editor_can_review: bool = False

    @model_validator(mode="after")
    def resolve_provider_fallbacks(self) -> "Settings":
        """新能力配置为空时回退到旧厂商字段，保证存量 .env 不破。

        文本与对话能力优先使用通用字段，未显式配置时再按厂商读取专属
        Key、模型和基础地址；图片与视频能力同样保留旧字段兼容。
        """
        # 在回退赋值前保存真实输入字段；Pydantic 对 validator 内 setattr 也会更新
        # model_fields_set，因此不能在工厂阶段直接用 model_fields_set 判断显式配置。
        self._explicit_fields = frozenset(self.model_fields_set)

        self.text_llm_provider = self.text_llm_provider.strip().lower()
        self.chat_llm_provider = self.chat_llm_provider.strip().lower()
        self.image_gen_provider = self.image_gen_provider.strip().lower()
        self.video_gen_provider = self.video_gen_provider.strip().lower()

        llm_defaults = {
            "openai": (self.openai_api_key, self.openai_llm_model, ""),
            "deepseek": (self.deepseek_api_key, self.deepseek_model, ""),
            "bailian": (
                self.dashscope_api_key,
                "qwen-plus",
                f"{self.dashscope_base_url.rstrip('/')}/compatible-mode/v1",
            ),
            "claude": (
                self.anthropic_api_key,
                self.anthropic_model,
                self.anthropic_base_url,
            ),
            "anthropic": (
                self.anthropic_api_key,
                self.anthropic_model,
                self.anthropic_base_url,
            ),
            "gemini": (self.gemini_api_key, self.gemini_llm_model, ""),
            "google": (self.gemini_api_key or self.google_api_key, self.gemini_llm_model, ""),
        }
        for capability in ("text_llm", "chat_llm"):
            provider = getattr(self, f"{capability}_provider")
            defaults = llm_defaults.get(provider)
            if defaults is None:
                continue
            default_key, default_model, default_base_url = defaults
            if not getattr(self, f"{capability}_api_key"):
                setattr(self, f"{capability}_api_key", default_key)
            if not getattr(self, f"{capability}_model"):
                setattr(self, f"{capability}_model", default_model)
            if not getattr(self, f"{capability}_base_url") and default_base_url:
                setattr(self, f"{capability}_base_url", default_base_url)

        if self.image_gen_provider == "openai":
            if not self.image_gen_api_key:
                self.image_gen_api_key = self.openai_api_key
            if not self.image_gen_model:
                self.image_gen_model = self.openai_image_model
        elif self.image_gen_provider == "google":
            if not self.image_gen_api_key:
                self.image_gen_api_key = self.google_api_key or self.gemini_api_key
            if not self.image_gen_model:
                self.image_gen_model = self.google_image_model
        elif self.image_gen_provider == "gemini":
            if not self.image_gen_api_key:
                self.image_gen_api_key = self.gemini_api_key or self.google_api_key
            if not self.image_gen_model:
                self.image_gen_model = self.google_image_model
        elif self.image_gen_provider == "bailian":
            if not self.image_gen_api_key:
                self.image_gen_api_key = self.dashscope_api_key
            if not self.image_gen_base_url:
                self.image_gen_base_url = self.dashscope_base_url
            if not self.image_gen_model:
                self.image_gen_model = "wan2.1-t2i-plus"
        video_defaults = {
            "volcengine": (
                self.volcengine_api_key,
                self.seedance_model,
                self.volcengine_ark_base_url,
            ),
            "doubao": (
                self.volcengine_api_key,
                self.seedance_model,
                self.volcengine_ark_base_url,
            ),
            "seedance": (
                self.volcengine_api_key,
                self.seedance_model,
                self.volcengine_ark_base_url,
            ),
            "bailian": (
                self.dashscope_api_key,
                self.dashscope_video_model,
                self.dashscope_base_url,
            ),
            "openai": (self.openai_api_key, self.openai_video_model, ""),
            "sora": (self.openai_api_key, self.openai_video_model, ""),
            "gemini": (
                self.gemini_api_key or self.google_api_key,
                self.google_video_model,
                "",
            ),
            "google": (
                self.google_api_key or self.gemini_api_key,
                self.google_video_model,
                "",
            ),
        }
        video_default = video_defaults.get(self.video_gen_provider)
        if video_default is not None:
            default_key, default_model, default_base_url = video_default
            if not self.video_gen_api_key:
                self.video_gen_api_key = default_key
            if not self.video_gen_model:
                self.video_gen_model = default_model
            if not self.video_gen_base_url:
                self.video_gen_base_url = default_base_url
        return self

    @model_validator(mode="after")
    def validate_security_settings(self) -> "Settings":
        if not self.cors_origins:
            raise ValueError("CORS_ORIGINS 必须配置至少一个可信来源")

        try:
            self.cors_origins = [
                _normalize_cors_origin(origin, allow_wildcard=not self.is_prod)
                for origin in self.cors_origins
            ]
        except ValueError as exc:
            raise ValueError(f"CORS_ORIGINS 必须配置合法的 http/https origin：{exc}") from exc

        audit_public_origin = self.volc_image_audit_public_base_url.strip()
        if audit_public_origin:
            try:
                normalized_audit_origin = _normalize_cors_origin(
                    audit_public_origin,
                    allow_wildcard=False,
                )
                parsed_audit_origin = urlsplit(normalized_audit_origin)
                if parsed_audit_origin.scheme != "https":
                    raise ValueError("必须使用 HTTPS")
                if _is_non_public_origin_host(parsed_audit_origin.hostname or ""):
                    raise ValueError("必须使用公网主机")
            except ValueError as exc:
                raise ValueError(
                    f"VOLC_IMAGE_AUDIT_PUBLIC_BASE_URL 配置不安全：{exc}"
                ) from exc
            self.volc_image_audit_public_base_url = normalized_audit_origin

        for field_name in (
            "text_llm_base_url",
            "chat_llm_base_url",
            "image_gen_base_url",
            "video_gen_base_url",
            "dashscope_base_url",
            "anthropic_base_url",
            "meizi_base_url",
        ):
            value = getattr(self, field_name).strip()
            if not value:
                continue
            try:
                normalized = _normalize_provider_base_url(
                    value,
                    allow_local_http=not self.is_prod,
                )
            except ValueError as exc:
                env_name = field_name.upper()
                raise ValueError(f"{env_name} 配置不安全：{exc}") from exc
            setattr(self, field_name, normalized)

        if self.storage_driver == "meizi":
            missing = [
                field_name.upper()
                for field_name in ("meizi_base_url", "meizi_upload_user_name")
                if not str(getattr(self, field_name)).strip()
            ]
            if not self.meizi_upload_salt.get_secret_value().strip():
                missing.append("MEIZI_UPLOAD_SALT")
            if not self.meizi_front_salt.get_secret_value().strip():
                missing.append("MEIZI_FRONT_SALT")
            if missing:
                raise ValueError(f"MeiZi 存储配置缺失：{', '.join(missing)}")
            if urlsplit(self.meizi_base_url).scheme != "https":
                raise ValueError("MEIZI_BASE_URL 必须使用 HTTPS")
            positive_fields = (
                "meizi_upload_tenant_id",
                "meizi_upload_group_id",
                "meizi_upload_source",
                "meizi_front_tenant_id",
                "meizi_front_source",
                "meizi_lib_type_file",
                "meizi_lib_type_image",
                "meizi_lib_type_video",
                "meizi_lib_type_audio",
            )
            invalid = [
                field_name.upper()
                for field_name in positive_fields
                if int(getattr(self, field_name)) <= 0
            ]
            if self.meizi_front_group_id < 0:
                invalid.append("MEIZI_FRONT_GROUP_ID")
            if invalid:
                raise ValueError(f"MeiZi 存储配置必须为正整数：{', '.join(invalid)}")
            lib_types = (
                self.meizi_lib_type_file,
                self.meizi_lib_type_image,
                self.meizi_lib_type_video,
                self.meizi_lib_type_audio,
            )
            if len(set(lib_types)) != len(lib_types):
                raise ValueError("MEIZI_LIB_TYPE_* 必须互不相同")
            self.meizi_upload_allowed_hosts = _normalize_host_allowlist(
                self.meizi_upload_allowed_hosts,
                field_name="MEIZI_UPLOAD_ALLOWED_HOSTS",
            )
            self.meizi_public_url_allowed_hosts = _normalize_host_allowlist(
                self.meizi_public_url_allowed_hosts,
                field_name="MEIZI_PUBLIC_URL_ALLOWED_HOSTS",
            )

        if self.is_prod:
            secret = self.session_secret.strip()
            if (
                secret == "dev-only-change-me"
                or len(secret) < 32
                or _is_low_strength_secret(secret)
            ):
                raise ValueError(
                    "生产环境 SESSION_SECRET 至少需要 32 个字符、不得使用默认值或低强度值"
                )
        return self

    @property
    def is_prod(self) -> bool:
        return self.app_env in {"prod", "production"}

    @property
    def is_dev(self) -> bool:
        return self.app_env == "dev"

    @property
    def cors_allow_credentials(self) -> bool:
        """通配符来源不能与浏览器凭据模式同时使用。"""
        return "*" not in self.cors_origins

    def is_explicitly_configured(self, field_name: str) -> bool:
        """判断字段是否来自环境变量或 Settings 显式入参，而不是类默认值。"""
        return field_name in self._explicit_fields


def _normalize_cors_origin(origin: str, *, allow_wildcard: bool) -> str:
    value = origin.strip()
    if not value or value.lower() == "null":
        raise ValueError("不能留空或使用 null")
    if value == "*":
        if allow_wildcard:
            return value
        raise ValueError("生产环境不能使用通配符")

    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        parsed.port  # 访问端口以触发非法端口校验
    except ValueError as exc:
        raise ValueError("来源格式无效") from exc

    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.netloc
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or "?" in value
        or "#" in value
        or any(char.isspace() or char in "\\/" for char in parsed.netloc)
        or parsed.netloc.endswith(":")
    ):
        raise ValueError("必须是无用户信息、查询、片段和非根路径的 http/https origin")

    return parsed._replace(scheme=parsed.scheme.lower(), path="").geturl()


def _normalize_provider_base_url(value: str, *, allow_local_http: bool) -> str:
    """校验携带模型密钥的出站 Base URL，防止凭据经明文 HTTP 或用户信息外泄。"""
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise ValueError("URL 格式无效") from exc

    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.netloc
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(char.isspace() or char == "\\" for char in parsed.netloc)
    ):
        raise ValueError("必须是不含用户信息、查询参数或片段的 http/https URL")

    scheme = parsed.scheme.lower()
    if scheme == "http" and not (
        allow_local_http and _is_local_development_host(hostname)
    ):
        raise ValueError("携带 API Key 的远程 Base URL 必须使用 HTTPS")
    return parsed._replace(scheme=scheme).geturl().rstrip("/")


def _is_local_development_host(hostname: str) -> bool:
    host = hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _is_non_public_origin_host(hostname: str) -> bool:
    """拒绝 localhost 及不具备公网路由属性的 IP 字面量。"""
    host = hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return not ipaddress.ip_address(host).is_global
    except ValueError:
        return False


def _normalize_host_allowlist(values: list[str], *, field_name: str) -> list[str]:
    """规范化可选的出站主机白名单，禁止端口、路径和通配符。"""
    normalized: list[str] = []
    for raw in values:
        value = raw.strip().rstrip(".").lower()
        if (
            not value
            or "*" in value
            or "/" in value
            or "\\" in value
            or ":" in value
            or any(char.isspace() for char in value)
        ):
            raise ValueError(f"{field_name} 包含非法主机")
        try:
            ascii_host = value.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError(f"{field_name} 包含非法主机") from exc
        if len(ascii_host) > 253:
            raise ValueError(f"{field_name} 包含非法主机")
        normalized.append(ascii_host)
    return list(dict.fromkeys(normalized))


def _is_low_strength_secret(secret: str) -> bool:
    """拒绝明显重复或有效字符熵过低的会话签名密钥。"""
    for unit_length in range(1, len(secret) // 2 + 1):
        if len(secret) % unit_length == 0:
            unit = secret[:unit_length]
            if unit * (len(secret) // unit_length) == secret:
                return True

    counts = Counter(secret)
    entropy = -sum(
        (count / len(secret)) * log2(count / len(secret))
        for count in counts.values()
    )
    return len(counts) < 8 or entropy < 3.0


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


settings = get_settings()
