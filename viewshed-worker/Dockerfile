FROM ghcr.io/osgeo/gdal:ubuntu-full-3.13.3@sha256:2dd0f81ef927ff4c3d4dbe4f73c029dc86d4073974c0564f8e196f6e1412e2e0

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-pip python3-venv python3-shapely \
    && rm -rf /var/lib/apt/lists/*

# Venv with system-site-packages so GDAL Python bindings + numpy are inherited
RUN python3 -m venv /opt/venv --system-site-packages
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

COPY worker.py worker_metrics.py link_queue_v3.py viewshed_queue_v2.py queue_admin.py backfill_profiles.py dry_run_coverage.py uk_mainland.json ./
COPY rf ./rf
COPY tests ./tests

RUN useradd --system --uid 10001 --home-dir /app meshcore \
    && mkdir -p /data/srtm \
    && chown -R meshcore:meshcore /app /data/srtm /opt/venv
USER 10001:10001
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/gadgethd/ukmesh"
EXPOSE 9091
CMD ["python3", "-u", "worker.py"]
