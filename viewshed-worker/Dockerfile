FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.2@sha256:35cf3b42728f568b911a120d8644b5e5fb7d277b8d3ed07e4b66c0af19c957af

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Venv with system-site-packages so GDAL Python bindings + numpy are inherited
RUN python3 -m venv /opt/venv --system-site-packages
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py link_queue_v3.py uk_mainland.json ./
COPY rf ./rf

CMD ["python3", "-u", "worker.py"]
