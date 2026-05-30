# yt-downloader-docker

Build Docker de [YT-Downloader](https://github.com/vicvinue/yt-pydownloader) pensado para servidores. El archivo viaja directo desde los CDNs de YouTube hasta el navegador del usuario — **el servidor nunca toca disco**.

## Cómo funciona

1. yt-dlp extrae las URLs de los streams de YouTube (video y audio por separado).
2. ffmpeg los descarga simultáneamente desde el CDN y los muxea al vuelo.
3. El resultado se entrega al navegador vía streaming HTTP con `Content-Disposition: attachment`.

No hay archivos temporales, no hay doble descarga. La barra de progreso nativa del navegador muestra el avance en tiempo real.

## Interfaz

- **Auto-portapapeles** — al cargar la página detecta si hay una URL de YouTube en el portapapeles, la rellena y lanza la búsqueda automáticamente.
- **Formatos agrupados** — secciones Audio / Video separadas con badges de calidad (SD · HD · FHD · 2K · 4K · 8K).
- **Metadatos del video** — título, canal, número de vistas y duración (formato h:mm:ss para videos largos).
- **ESC para limpiar** — limpia el input o resetea la vista.
- **Compatible con mobile** — descargas vía fetch+blob en iOS/Android con progreso en tiempo real; input sin zoom en iOS.

## Velocidad de descarga

- **Headers completos a ffmpeg** — se pasan todos los headers HTTP que yt-dlp extrae (cookies, `Origin`, `Referer`, etc.), evitando el throttling de YouTube al stream DASH.
- **Reconexión automática** — ffmpeg usa `-reconnect` para recuperarse si YouTube corta el stream a mitad de descarga.
- **Chunks de 256 KB** — buffer de lectura ampliado para reducir overhead en la transferencia al navegador.

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

El servidor queda escuchando en `127.0.0.1:7788` (solo localhost; el reverse proxy lo expone al exterior).

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `BASE_PATH` | `/yt-downloader` | Subpath en el que se monta la app |

## Reverse proxy

### Caddy

```caddy
@yt-downloader path /yt-downloader /yt-downloader/*
handle @yt-downloader {
    reverse_proxy localhost:7788 {
        flush_interval -1
    }
}
```

> `flush_interval -1` deshabilita el buffering de Caddy, necesario para que el streaming llegue inmediatamente al navegador.

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

## Formatos disponibles

| Formato | Códec | Contenedor |
|---|---|---|
| Audio MP3 | MP3 (máxima calidad) | `.mp3` |
| Audio WAV | WAV (sin pérdida) | `.wav` |
| Video 480p | H.264 + AAC | `.mp4` fragmentado |
| Video 720p | H.264 + AAC | `.mp4` fragmentado |
| Video 1080p | H.264 + AAC | `.mp4` fragmentado |
| Video 1440p / 2160p / … | H.264 + AAC | `.mp4` fragmentado |
| Video original | mejor codec disponible | `.mkv` |

Los formatos disponibles se detectan automáticamente según lo que ofrezca el video; solo aparecen las resoluciones realmente presentes.

## Actualizar

```bash
cd yt-downloader-docker
git pull
docker compose build && docker compose up -d
```

## Aviso legal

Ver sección de [propiedad intelectual en yt-pydownloader](https://github.com/vicvinue/yt-pydownloader/releases/tag/v1.0.0).
