FROM python:3.11-slim

# System deps needed by PyMuPDF and pdfplumber
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached layer)
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source
COPY backend/ /app/

# Copy frontend static files
COPY frontend/ /app/frontend/

# Create volume mount points
RUN mkdir -p /app/uploads /app/output

EXPOSE 5000

ENV OUTPUT_DIR=/app/output \
    UPLOAD_DIR=/app/uploads \
    FRONTEND_DIR=/app/frontend \
    PROMPTS_CONFIG_PATH=/app/prompts_config.json \
    OLLAMA_HOST=http://host.docker.internal:11434 \
    OLLAMA_MODEL=deepseek-v3.1:671b-cloud \
    PORT=5000

CMD ["python", "app.py"]
