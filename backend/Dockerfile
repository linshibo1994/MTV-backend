FROM ghcr.io/astral-sh/uv:0.8.20-python3.12-bookworm-slim

# 构建期使用国内 apt / PyPI 源，避免 deb.debian.org 与官方 PyPI 超时。
ENV UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple
RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources; \
    elif [ -f /etc/apt/sources.list ]; then \
      sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list; \
    fi; \
    apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 将依赖安装在不受开发目录挂载影响的位置；阿里云 OSS SDK 按镜像用途选装。
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
COPY pyproject.toml uv.lock ./
ARG INSTALL_ALIYUN_OSS=true
RUN if [ "$INSTALL_ALIYUN_OSS" = "true" ]; then \
        uv sync --frozen --no-dev --extra aliyun-oss --no-install-project \
          --default-index https://pypi.tuna.tsinghua.edu.cn/simple; \
    else \
        uv sync --frozen --no-dev --no-install-project \
          --default-index https://pypi.tuna.tsinghua.edu.cn/simple; \
    fi

# 生产部署不依赖宿主机源码挂载；.dockerignore 会排除真实 .env 与本地媒体。
COPY alembic ./alembic
COPY app ./app
COPY alembic.ini ./

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
