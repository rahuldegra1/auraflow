# Stage 1: Build & wheels generation
FROM python:3.10-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip build wheel
COPY src/ ./src/
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /build/wheels -e .

# Stage 2: Final runtime container
FROM python:3.10-slim AS runner

WORKDIR /app

# Hardened unprivileged system user
RUN groupadd -g 10001 auraflow && \
    useradd -u 10001 -g auraflow -s /bin/bash -m auraflow

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels

COPY src/ /app/src/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

USER auraflow

EXPOSE 8000 50051

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "auraflow.main:app", "--host", "0.0.0.0", "--port", "8000"]