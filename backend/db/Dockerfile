# Multi-stage build for the FoodAtlas database tooling image.
#
# This image runs ad-hoc ETL data loads via ECS task overrides. It is not
# a long-running service. Stage 1 installs dependencies into a uv-managed
# .venv. Stage 2 copies only the venv plus the source tree into a slim
# runtime image.

FROM python:3.12-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cached layer) using only lockfile + manifest
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Copy source and project metadata, then install the project
COPY src ./src
COPY main.py README.md ./
RUN uv sync --frozen --no-dev


FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app && useradd --system --gid app --home /app app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/src ./src
COPY --from=builder /app/main.py ./

RUN chown -R app:app /app
USER app

# This image is invoked ad-hoc with command overrides from ECS, e.g.:
#   python main.py load --parquet-dir s3://bucket/kg/
# The default CMD prints help so a bare `docker run` is informative.
CMD ["python", "main.py", "--help"]
