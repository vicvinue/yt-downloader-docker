# yt-downloader-docker

Build Docker de [YT-Downloader](https://github.com/vicvinue/yt-pydownloader) pensado para servidores. Los archivos se descargan de YouTube, se entregan al navegador vía streaming HTTP y se eliminan del servidor inmediatamente — **nada queda almacenado**.

## Requisitos

- Docker
- Docker Compose
- Un reverse proxy (Caddy, Nginx, Traefik…)

## Despliegue

```bash
git clone https://github.com/vicvinue/yt-downloader-docker.git
cd yt-downloader-docker
docker compose up -d
```

El servidor queda escuchando en `127.0.0.1:7788` (solo localhost, el reverse proxy lo expone al exterior).

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `BASE_PATH` | `/yt-downloader` | Subpath en el que se monta la app |

## Reverse proxy

### Caddy

Agrega esto dentro de tu bloque `server {}` en el `Caddyfile`:

```caddy
@yt-downloader path /yt-downloader /yt-downloader/*
handle @yt-downloader {
    reverse_proxy localhost:7788 {
        flush_interval -1
    }
}
```

> `flush_interval -1` es necesario para que SSE (barra de progreso en tiempo real) funcione correctamente.

### Nginx

```nginx
location /yt-downloader {
    proxy_pass         http://127.0.0.1:7788;
    proxy_set_header   Host $host;
    proxy_buffering    off;
    proxy_cache        off;
    proxy_read_timeout 600s;
    proxy_http_version 1.1;
    proxy_set_header   Connection "";
}
```

## Cómo funciona

1. El usuario pega un enlace de YouTube y elige el formato (MP3, WAV, video 720p/1080p/original).
2. El servidor descarga el archivo a un directorio temporal en `/tmp`.
3. Al completar, el archivo se entrega al navegador vía streaming HTTP con `Content-Disposition: attachment`.
4. El archivo temporal se elimina automáticamente tras la descarga.
5. Los archivos no descargados se limpian pasada **1 hora**.

## Formatos disponibles

| Formato | Códec | Contenedor |
|---|---|---|
| Audio MP3 | MP3 (máxima calidad) | `.mp3` |
| Audio WAV | WAV (sin pérdida) | `.wav` |
| Video 720p | H.264 + AAC | `.mp4` |
| Video 1080p | H.264 + AAC | `.mp4` |
| Video original | AV1/VP9 + Opus | `.mkv` |

## Aviso legal

Ver sección de [propiedad intelectual en yt-pydownloader](https://github.com/vicvinue/yt-pydownloader/releases/tag/v1.0.0).
