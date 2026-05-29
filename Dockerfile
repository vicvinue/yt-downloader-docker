FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    yt-dlp \
    flask \
    gunicorn

COPY app.py .

ENV BASE_PATH=/yt-downloader

EXPOSE 7788

CMD ["gunicorn", "app:app", \
     "--bind", "0.0.0.0:7788", \
     "--workers", "1", \
     "--worker-class", "gthread", \
     "--threads", "8", \
     "--timeout", "0"]
