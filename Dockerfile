FROM python:3.13.18-slim AS builder
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
WORKDIR /app
RUN pip install --no-cache-dir uv==0.13.7
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.13.18-slim AS runtime
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN useradd --uid 10001 --create-home app
COPY --from=builder /opt/venv /opt/venv
COPY alembic.ini ./
COPY migrations ./migrations
USER app
CMD ["uvicorn", "mkk_basis.bootstrap.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

FROM builder AS test
RUN uv sync --frozen --no-editable
COPY alembic.ini ./
COPY migrations ./migrations
COPY tests ./tests
RUN useradd --uid 10001 --create-home app && chown -R app:app /app
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
USER app
CMD ["pytest", "-q"]
