FROM python:3.12-slim
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
WORKDIR /app
COPY tagger_worker.py /app/tagger_worker.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN useradd -m -u 10001 tagger && chown -R tagger:tagger /app
USER tagger
CMD ["python3", "-u", "/app/tagger_worker.py"]
