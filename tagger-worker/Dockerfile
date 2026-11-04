FROM python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/gadgethd/ukmesh"
WORKDIR /app
COPY tagger_worker.py /app/tagger_worker.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN useradd -m -u 10001 tagger && chown -R tagger:tagger /app
USER tagger
CMD ["python3", "-u", "/app/tagger_worker.py"]
