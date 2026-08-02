FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/gadgethd/ukmesh"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
# The Debian Python base marks perl-base essential for package-management
# scripts, but the sealed learner runtime never executes Perl or apt. Removing
# it after all installs eliminates an otherwise unused interpreter surface.
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get purge -y --allow-remove-essential perl-base \
    && rm -rf /var/lib/apt/lists/*

COPY worker.py .
COPY tests ./tests

RUN useradd --system --uid 10001 --home-dir /app meshcore \
    && chown -R meshcore:meshcore /app
USER 10001:10001
CMD ["python3", "-u", "worker.py"]
