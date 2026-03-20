FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Venv with system-site-packages so GDAL Python bindings + numpy are inherited
RUN python3 -m venv /opt/venv --system-site-packages
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py uk_mainland.json ./
COPY rf ./rf

CMD ["python3", "-u", "worker.py"]
