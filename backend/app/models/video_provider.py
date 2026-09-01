"""VideoProviderConfig（视频供应商配置）ORM 模型。"""

from sqlalchemy import Boolean, CheckConstraint, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPKMixin


class VideoProviderConfig(UUIDPKMixin, Base):
    """全局视频供应商配置 — 决定 provider_key + model 路由，仅一行 active。"""

    __tablename__ = "video_provider_configs"
    __table_args__ = (
        UniqueConstraint("provider_key", "job_type", name="uq_video_provider_key_job"),
        CheckConstraint(
            "provider_key IN "
            "('jianyu', 'modelsai', 'volcengine', 'doubao', 'neolink', 'bailian', 'openai', "
            "'gemini', 'entpilot')",
            name="ck_video_provider_configs_provider_key",
        ),
        CheckConstraint(
            "job_type IN ('all_reference')",
            name="ck_video_provider_configs_job_type",
        ),
    )

    # jianyu | modelsai | neolink | bailian | openai | gemini | entpilot
    provider_key: Mapped[str] = mapped_column(String(32), nullable=False)
    # all_reference（可扩展）
    job_type: Mapped[str] = mapped_column(String(32), nullable=False)
    # 实际模型，如 doubao-seedance-2-0-260128
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
