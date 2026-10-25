FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch CPU first to avoid heavy CUDA wheels
RUN pip install --no-cache-dir torch --extra-index-url https://download.pytorch.org/whl/cpu

# Ensure core web runner dependencies are installed directly
COPY requirements.txt .
RUN pip install --no-cache-dir "uvicorn[standard]>=0.28.0" fastapi -r requirements.txt
RUN pip install --no-cache-dir httpx
# Copy source and install package
COPY pyproject.toml .
COPY src/ ./src/
RUN pip install --no-cache-dir -e .

EXPOSE 8000 50051

CMD ["python", "-m", "uvicorn", "auraflow.main:app", "--host", "0.0.0.0", "--port", "8000"]
