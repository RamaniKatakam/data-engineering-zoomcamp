FROM python:3.13-slim


COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

COPY pyproject.toml .python-version uv.lock .

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

RUN uv sync --locked

COPY pipeline.py .

ENTRYPOINT ["uv", "run","python", "pipeline.py"]