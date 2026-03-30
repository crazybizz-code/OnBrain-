FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# SQLite DB stored on Koyeb persistent volume (mount at /data in Koyeb dashboard)
# Falls back to /app/google_tokens.db if no volume is mounted
ENV SQLITE_TOKEN_DB=/data/google_tokens.db

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY bot.py .
COPY data_indexing_service.py .
COPY google_drive_service.py .

# Create necessary directories
# /data is where Koyeb persistent volume will be mounted (keeps SQLite across deploys)
RUN mkdir -p excel_files /data

# Tell Koyeb/Docker which port the app listens on
EXPOSE 8080

# Health check — Koyeb and Render both check /health on PORT
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/health || exit 1

# Run the bot
CMD ["python", "bot.py"]
