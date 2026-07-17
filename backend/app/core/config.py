"""应用配置 — 通过环境变量注入，默认值与 .env.example 对齐。"""

from collections import Counter
from functools import lru_cache
from math import log2
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # 应用
    app_env: Literal["dev", "test", "prod", "production"] = "dev"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://localhost:5173"]
    )
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
    database_url: str = "postgresql+asyncpg://flow:flow@localhost:5432/flowapp"
    database_sync_url: str = "postgresql+psycopg2://flow:flow@localhost:5432/flowapp"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # 会话
    session_secret: str = "dev-only-change-me"
    session_cookie_name: str = "flowapp_session"
    session_max_age: int = 86400
    tgw_l7_route_cookie: str = "flowapp-tgw"

    # 存储
    storage_driver: Literal["local", "s3"] = "local"
    storage_root: str = "./storage"
    s3_endpoint: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "flowapp"
    s3_region: str = "us-east-1"

    # OpenAI (旧字段，保留作新能力配置为空时的回退)
    openai_api_key: str = ""
    openai_llm_model: str = "gpt-4o"
    openai_image_model: str = "gpt-image-2"

    # Google Gemini (旧字段，banana-2 / Nano Banana)
    google_api_key: str = ""
    google_image_model: str = "gemini-2.5-flash-image"

    # 字节豆包 Seedance (旧字段)
    volcengine_access_key: str = ""
    volcengine_secret_key: str = ""
    volcengine_region: str = "cn-north-1"
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

    # 视频生成 —— volcengine | bailian | openai
    video_gen_provider: str = "volcengine"
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
            "gemini": (self.gemini_api_key, self.gemini_llm_model, ""),
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
                self.image_gen_api_key = self.google_api_key
            if not self.image_gen_model:
                self.image_gen_model = self.google_image_model
        elif self.image_gen_provider == "bailian":
            if not self.image_gen_api_key:
                self.image_gen_api_key = self.dashscope_api_key
            if not self.image_gen_base_url:
                self.image_gen_base_url = self.dashscope_base_url
            if not self.image_gen_model:
                self.image_gen_model = "wan2.1-t2i-plus"
        if not self.video_gen_model:
            self.video_gen_model = {
                "volcengine": self.seedance_model,
                "doubao": self.seedance_model,
                "seedance": self.seedance_model,
                "bailian": self.dashscope_video_model,
                "openai": self.openai_video_model,
                "sora": self.openai_video_model,
            }.get(self.video_gen_provider, self.seedance_model)
        return self

    @model_validator(mode="after")
    def validate_security_settings(self) -> "Settings":
        if not self.cors_origins:
            raise ValueError("CORS_ORIGINS 必须配置至少一个可信来源")

        try:
            self.cors_origins = [_normalize_cors_origin(origin) for origin in self.cors_origins]
        except ValueError as exc:
            raise ValueError(f"CORS_ORIGINS 必须配置合法的 http/https origin：{exc}") from exc

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


def _normalize_cors_origin(origin: str) -> str:
    value = origin.strip()
    if not value or value == "*" or value.lower() == "null":
        raise ValueError("不能留空、使用通配符或使用 null")

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
