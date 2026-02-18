FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install CPU-only PyTorch first (smaller than default with CUDA)
RUN uv pip install --system --no-cache torch --index-url https://download.pytorch.org/whl/cpu

# Copy project files and install dependencies
COPY pyproject.toml ./
RUN uv pip install --system --no-cache .

# Copy application code
COPY . .

# Create data directory for SQLite
RUN mkdir -p /app/data

# Expose port
EXPOSE 8000

# Run with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
