"""YT-Downloader — Producción (piped streaming)
yt-dlp extrae las URLs del CDN → ffmpeg descarga y muxea → pipe directo al navegador.
Sin archivos temporales. Sin doble descarga.
"""
import os
import sys
import time
import threading
import subprocess
import urllib.parse

import yt_dlp
from flask import Flask, Blueprint, request, jsonify, Response, abort

# ── config ────────────────────────────────────────────────────────────────────

FFMPEG_BIN = "ffmpeg"
EXTRA_OPTS  = {"remote_components": "ejs:github"}
BASE_PATH   = os.environ.get("BASE_PATH", "/yt-downloader").rstrip("/")

_info_cache: dict = {}

def _cache_cleanup():
    while True:
        time.sleep(1800)
        cutoff = time.time() - 1800
        for k in [k for k, v in list(_info_cache.items()) if v["ts"] < cutoff]:
            _info_cache.pop(k, None)

threading.Thread(target=_cache_cleanup, daemon=True).start()

# ── yt-dlp helpers ────────────────────────────────────────────────────────────

class _SilentLogger:
    def debug(self, msg):   pass
    def info(self, msg):    pass
    def warning(self, msg): pass
    def error(self, msg):   pass

_SILENT = {
    "quiet": True, "no_warnings": True, "noprogress": True,
    "logger": _SilentLogger(),
}

def _best_audio(formats):
    fmts = [f for f in formats
            if f.get("acodec") != "none" and f.get("vcodec") == "none" and f.get("url")]
    if not fmts:
        fmts = [f for f in formats if f.get("acodec") != "none" and f.get("url")]
    return max(fmts, key=lambda f: f.get("abr", 0) or f.get("tbr", 0) or 0)

def _best_video(formats, max_height):
    fmts = [f for f in formats
            if f.get("vcodec") != "none" and f.get("acodec") == "none" and f.get("url")]
    if max_height:
        fmts = [f for f in fmts if (f.get("height") or 0) <= max_height]
    h264 = [f for f in fmts if (f.get("vcodec") or "").startswith("avc")]
    pool = h264 or fmts
    if not pool:
        raise ValueError(f"Sin stream de video para height≤{max_height}")
    return max(pool, key=lambda f: f.get("height", 0) or 0)

def _user_agent(formats):
    return (formats or [{}])[0].get("http_headers", {}).get(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    )

def _build_pipeline(choice, info):
    formats = info.get("formats", [])
    title   = info.get("title", "download")
    hdr     = f"User-Agent: {_user_agent(formats)}\r\n"

    if choice == "audio_mp3":
        af = _best_audio(formats)
        cmd = [FFMPEG_BIN,
               "-headers", hdr, "-i", af["url"],
               "-vn", "-f", "mp3", "-q:a", "0", "-loglevel", "quiet", "pipe:1"]
        return cmd, f"{title}.mp3", "audio/mpeg"

    elif choice == "audio_wav":
        af = _best_audio(formats)
        cmd = [FFMPEG_BIN,
               "-headers", hdr, "-i", af["url"],
               "-vn", "-f", "wav", "-loglevel", "quiet", "pipe:1"]
        return cmd, f"{title}.wav", "audio/wav"

    elif choice == "video_original":
        vf = _best_video(formats, None)
        af = _best_audio(formats)
        cmd = [FFMPEG_BIN,
               "-headers", hdr, "-i", vf["url"],
               "-headers", hdr, "-i", af["url"],
               "-c", "copy", "-f", "matroska", "-loglevel", "quiet", "pipe:1"]
        return cmd, f"{title}.mkv", "video/x-matroska"

    else:
        height = int(choice.split("_")[1])
        vf = _best_video(formats, height)
        af = _best_audio(formats)
        cmd = [FFMPEG_BIN,
               "-headers", hdr, "-i", vf["url"],
               "-headers", hdr, "-i", af["url"],
               "-c", "copy", "-f", "mp4",
               "-movflags", "frag_keyframe+empty_moov",
               "-loglevel", "quiet", "pipe:1"]
        return cmd, f"{title}.mp4", "video/mp4"

def _format_duration(seconds):
    if not seconds:
        return "0:00"
    h, rem = divmod(int(seconds), 3600)
    m, s   = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def _format_views(n):
    if not n:
        return None
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B vistas"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M vistas"
    if n >= 1_000:
        return f"{n/1_000:.1f}K vistas"
    return f"{n} vistas"

def _height_badge(h):
    if h >= 4320: return {"label": "8K",  "color": "purple"}
    if h >= 2160: return {"label": "4K",  "color": "purple"}
    if h >= 1440: return {"label": "2K",  "color": "indigo"}
    if h >= 1080: return {"label": "FHD", "color": "green"}
    if h >= 720:  return {"label": "HD",  "color": "blue"}
    return {"label": "SD", "color": "gray"}

# ── HTML ──────────────────────────────────────────────────────────────────────

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>YT-Downloader</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f3f4f6; color: #111827;
    min-height: 100vh;
    display: flex; flex-direction: column; align-items: center;
    padding: 2.5rem 1rem 5rem;
  }

  header { text-align: center; margin-bottom: 2rem; }
  .logo {
    width: 56px; height: 56px; background: #ef4444;
    border-radius: 14px; display: flex; align-items: center; justify-content: center;
    margin: 0 auto 0.75rem; box-shadow: 0 4px 12px rgba(239,68,68,.35);
  }
  .logo svg { fill: white; }
  header h1 { font-size: 1.5rem; font-weight: 700; letter-spacing: -0.02em; }
  header p  { color: #6b7280; font-size: 0.875rem; margin-top: 0.2rem; }

  .container { width: 100%; max-width: 540px; display: flex; flex-direction: column; gap: 0.875rem; }

  .card {
    background: white; border-radius: 14px; padding: 1.25rem 1.375rem;
    box-shadow: 0 1px 3px rgba(0,0,0,.07), 0 1px 2px rgba(0,0,0,.04);
    animation: fadeIn .2s ease;
  }
  @keyframes fadeIn { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:translateY(0); } }

  /* URL input */
  .url-row { display: flex; gap: 0.5rem; }
  .url-wrap { flex: 1; position: relative; display: flex; align-items: center; min-width: 0; }
  .url-wrap input {
    flex: 1;
    padding: 0.6rem 2.4rem 0.6rem 0.875rem;
    border: 1.5px solid #e5e7eb; border-radius: 9px;
    font-size: 0.875rem; outline: none; transition: border-color .15s; min-width: 0;
  }
  .url-wrap input:focus     { border-color: #3b82f6; }
  .url-wrap input::placeholder { color: #9ca3af; }
  .url-clear {
    position: absolute; right: 0.5rem;
    background: none; border: none; padding: 0; cursor: pointer;
    color: #9ca3af; display: flex; align-items: center; transition: color .15s;
  }
  .url-clear:hover { color: #6b7280; }
  .url-clear.hidden { display: none !important; }

  /* Clipboard hint */
  .clipboard-hint {
    margin-top: 0.5rem; display: flex; align-items: center; gap: 0.375rem;
    font-size: 0.78rem; color: #2563eb; animation: fadeIn .25s ease;
  }
  .clipboard-hint svg { flex-shrink: 0; }

  button {
    padding: 0.6rem 1.1rem; border: none; border-radius: 9px;
    font-size: 0.875rem; font-weight: 600; cursor: pointer;
    transition: background .15s, opacity .15s; white-space: nowrap;
    display: inline-flex; align-items: center; gap: 0.4rem;
  }
  button:disabled { opacity: .45; cursor: not-allowed; }
  .btn-primary { background: #2563eb; color: white; }
  .btn-primary:hover:not(:disabled) { background: #1d4ed8; }
  .btn-dl {
    background: #16a34a; color: white;
    width: 100%; padding: 0.75rem; margin-top: 1rem; font-size: 0.9375rem;
    justify-content: center;
  }
  .btn-dl:hover:not(:disabled) { background: #15803d; }
  .btn-ghost { background: #f3f4f6; color: #374151; justify-content: center; }
  .btn-ghost:hover { background: #e5e7eb; }

  /* Error */
  .error-box {
    margin-top: 0.75rem; padding: 0.625rem 0.875rem;
    background: #fef2f2; border: 1px solid #fecaca;
    border-radius: 8px; font-size: 0.8125rem; color: #dc2626; line-height: 1.4;
  }

  /* Loading */
  .loading-row { display: flex; align-items: center; gap: 0.625rem; color: #6b7280; font-size: 0.875rem; }

  /* Video info */
  .video-row { display: flex; gap: 0.875rem; align-items: flex-start; }
  .thumb-wrap {
    flex-shrink: 0; width: 112px; height: 63px; border-radius: 7px;
    overflow: hidden; background: #e5e7eb; position: relative;
  }
  .thumb-wrap img { width: 100%; height: 100%; object-fit: cover; display: block; }
  .thumb-shimmer {
    position: absolute; inset: 0;
    background: linear-gradient(90deg, #e5e7eb 25%, #f3f4f6 50%, #e5e7eb 75%);
    background-size: 200% 100%;
    animation: shimmer 1.2s infinite;
  }
  .thumb-shimmer.hidden { display: none !important; }
  @keyframes shimmer { to { background-position: -200% 0; } }

  .video-meta { flex: 1; min-width: 0; }
  .video-meta h2 {
    font-size: 0.9rem; font-weight: 600; line-height: 1.45;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
  }
  .video-meta .meta-row {
    margin-top: 0.3rem; display: flex; flex-wrap: wrap; gap: 0.35rem 0.6rem;
    font-size: 0.8rem; color: #6b7280;
  }
  .meta-dot { color: #d1d5db; }

  .divider { border: none; border-top: 1px solid #f3f4f6; margin: 1rem 0; }

  .section-label { font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; color: #9ca3af; margin-bottom: 0.625rem; }

  /* Format groups */
  .fmt-group + .fmt-group { margin-top: 0.875rem; }
  .fmt-group-label {
    font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: .06em; color: #9ca3af;
    display: flex; align-items: center; gap: 0.375rem; margin-bottom: 0.4rem;
  }
  .fmt-group-label svg { flex-shrink: 0; }

  .format-list { display: flex; flex-direction: column; gap: 0.3rem; }
  .format-opt {
    display: flex; align-items: center; gap: 0.625rem;
    padding: 0.575rem 0.75rem; border: 1.5px solid #e5e7eb;
    border-radius: 9px; cursor: pointer; transition: border-color .12s, background .12s; user-select: none;
  }
  .format-opt:hover { background: #f9fafb; border-color: #d1d5db; }
  .format-opt.sel   { border-color: #3b82f6; background: #eff6ff; }

  .rdot { width: 16px; height: 16px; border-radius: 50%; border: 2px solid #d1d5db; display: flex; align-items: center; justify-content: center; flex-shrink: 0; transition: border-color .12s; }
  .format-opt.sel .rdot { border-color: #3b82f6; }
  .rdot::after { content: ""; width: 7px; height: 7px; border-radius: 50%; background: #3b82f6; opacity: 0; transition: opacity .12s; }
  .format-opt.sel .rdot::after { opacity: 1; }

  .fmt-text { flex: 1; min-width: 0; }
  .fmt-text strong { font-size: 0.875rem; display: block; }
  .fmt-text small  { font-size: 0.775rem; color: #6b7280; }

  /* Quality badge */
  .qbadge {
    font-size: 0.7rem; font-weight: 700; padding: 0.15rem 0.425rem;
    border-radius: 5px; flex-shrink: 0; letter-spacing: .03em;
  }
  .qbadge-purple { background: #ede9fe; color: #7c3aed; }
  .qbadge-indigo { background: #e0e7ff; color: #4338ca; }
  .qbadge-green  { background: #dcfce7; color: #15803d; }
  .qbadge-blue   { background: #dbeafe; color: #1d4ed8; }
  .qbadge-gray   { background: #f3f4f6; color: #6b7280; }

  /* Download started */
  .started-wrap { display: flex; flex-direction: column; align-items: center; gap: 0.875rem; padding: 0.5rem 0; text-align: center; }
  .started-icon { width: 48px; height: 48px; background: #dbeafe; border-radius: 50%; display: flex; align-items: center; justify-content: center; }
  .started-icon svg { fill: #2563eb; }
  .started-title { font-size: 0.9375rem; font-weight: 600; }
  .started-sub   { font-size: 0.8125rem; color: #6b7280; line-height: 1.5; }
  .started-file  {
    font-size: 0.8rem; color: #374151; background: #f3f4f6;
    border-radius: 7px; padding: 0.4rem 0.75rem;
    max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    font-family: ui-monospace, monospace;
  }

  /* History */
  #history-card { padding: 1rem 1.375rem; }
  .history-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.625rem; }
  .history-label { font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; color: #9ca3af; }
  .history-clear { background: none; border: none; padding: 0; font-size: 0.78rem; color: #9ca3af; cursor: pointer; transition: color .15s; }
  .history-clear:hover { color: #ef4444; }
  .history-list { display: flex; flex-direction: column; gap: 0.3rem; }
  .history-item {
    display: flex; align-items: center; gap: 0.625rem;
    padding: 0.45rem 0.625rem; border-radius: 8px;
    cursor: pointer; transition: background .12s; user-select: none;
  }
  .history-item:hover { background: #f9fafb; }
  .history-thumb {
    flex-shrink: 0; width: 48px; height: 27px;
    border-radius: 4px; overflow: hidden; background: #e5e7eb;
  }
  .history-thumb img { width: 100%; height: 100%; object-fit: cover; }
  .history-title { font-size: 0.8125rem; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #374151; }
  .history-time  { font-size: 0.75rem; color: #9ca3af; flex-shrink: 0; }

  /* Footer */
  footer {
    position: fixed; bottom: 0; left: 0; right: 0;
    background: white; border-top: 1px solid #e5e7eb;
    display: flex; align-items: center; justify-content: center;
    gap: 1rem; padding: 0.625rem 1rem; z-index: 100;
  }
  .footer-link { display: flex; align-items: center; gap: 0.375rem; font-size: 0.8rem; color: #6b7280; text-decoration: none; transition: color .15s; }
  .footer-link:hover { color: #111827; }
  .footer-link svg { flex-shrink: 0; }
  .footer-divider { width: 1px; height: 14px; background: #d1d5db; }
  .footer-copy    { font-size: 0.8rem; color: #9ca3af; }

  /* Spinner */
  .spin { width: 15px; height: 15px; border: 2px solid rgba(255,255,255,.35); border-top-color: white; border-radius: 50%; flex-shrink: 0; animation: rot .7s linear infinite; }
  .spin-dark { border-color: #e5e7eb; border-top-color: #3b82f6; }
  @keyframes rot { to { transform: rotate(360deg); } }

  .hidden { display: none !important; }
</style>
</head>
<body>

<header>
  <div class="logo">
    <svg width="28" height="28" viewBox="0 0 24 24"><path d="M19.615 3.184c-3.604-.246-11.631-.245-15.23 0-3.897.266-4.356 2.62-4.385 8.816.029 6.185.484 8.549 4.385 8.816 3.6.245 11.626.246 15.23 0 3.897-.266 4.356-2.62 4.385-8.816-.029-6.185-.484-8.549-4.385-8.816zm-10.615 12.816v-8l8 3.993-8 4.007z"/></svg>
  </div>
  <h1>YT-Downloader</h1>
  <p>Descarga videos y audio de YouTube</p>
</header>

<div class="container">

  <!-- URL card -->
  <div class="card">
    <div class="url-row">
      <div class="url-wrap">
        <input type="text" id="url-input"
               placeholder="https://www.youtube.com/watch?v=..."
               autocomplete="off" spellcheck="false">
        <button class="url-clear hidden" id="url-clear" onclick="clearInput()" title="Limpiar">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>
        </button>
      </div>
      <button class="btn-primary" id="search-btn" onclick="fetchInfo()">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="white"><path d="M15.5 14h-.79l-.28-.27A6.471 6.471 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>
        Buscar
      </button>
    </div>
    <div id="clipboard-hint" class="clipboard-hint hidden">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg>
      <span>URL detectada del portapapeles — buscando…</span>
    </div>
    <div id="error-box" class="error-box hidden"></div>
  </div>

  <!-- History card -->
  <div class="card hidden" id="history-card">
    <div class="history-header">
      <span class="history-label">Recientes</span>
      <button class="history-clear" onclick="clearHistory()">Borrar historial</button>
    </div>
    <div class="history-list" id="history-list"></div>
  </div>

  <!-- Loading card -->
  <div class="card hidden" id="loading-card">
    <div class="loading-row">
      <div class="spin spin-dark"></div>
      <span>Obteniendo información del video…</span>
    </div>
  </div>

  <!-- Info + formats card -->
  <div class="card hidden" id="info-card">
    <div class="video-row">
      <div class="thumb-wrap">
        <div class="thumb-shimmer" id="thumb-shimmer"></div>
        <img id="thumb" src="" alt="" style="opacity:0;transition:opacity .2s"
             onload="this.style.opacity=1;document.getElementById('thumb-shimmer').classList.add('hidden')">
      </div>
      <div class="video-meta">
        <h2 id="vtitle"></h2>
        <div class="meta-row" id="vmeta"></div>
      </div>
    </div>
    <hr class="divider">
    <div id="fmt-groups"></div>
    <button class="btn-dl" id="dl-btn" onclick="startDownload()" disabled>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="white"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>
      Descargar
    </button>
  </div>

  <!-- Download started card -->
  <div class="card hidden" id="started-card">
    <div class="started-wrap">
      <div class="started-icon">
        <svg width="24" height="24" viewBox="0 0 24 24"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>
      </div>
      <div class="started-title">Descarga en progreso</div>
      <div class="started-file" id="started-file"></div>
      <div class="started-sub">El archivo llega directo a tu navegador.<br>Puedes ver el progreso en la barra de descargas.</div>
      <div style="display:flex;gap:.5rem;margin-top:.25rem">
        <button class="btn-ghost" style="flex:1" onclick="reset()">Descargar otro</button>
        <button class="btn-primary" onclick="showInfoCard()">Otro formato</button>
      </div>
    </div>
  </div>

</div>

<footer>
  <span class="footer-copy">© 2026 vicvinue</span>
  <div class="footer-divider"></div>
  <a class="footer-link" href="https://github.com/vicvinue" target="_blank" rel="noopener">
    <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0 0 24 12c0-6.63-5.37-12-12-12z"/></svg>
    GitHub
  </a>
  <div class="footer-divider"></div>
  <a class="footer-link" href="https://www.paypal.com/donate/?business=DKBNN7D7E2Q96&no_recurring=1&currency_code=USD" target="_blank" rel="noopener">
    <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M7.076 21.337H2.47a.641.641 0 0 1-.633-.74L4.944.901C5.026.382 5.474 0 5.998 0h7.46c2.57 0 4.578.543 5.69 1.81 1.01 1.15 1.304 2.42 1.012 4.287-.023.143-.047.288-.077.437-.983 5.05-4.349 6.797-8.647 6.797h-2.19c-.524 0-.968.382-1.05.9l-1.12 7.106zm14.146-14.42a3.35 3.35 0 0 0-.607-.541c-.013.076-.026.175-.041.254-.93 4.778-4.005 7.201-9.138 7.201h-2.19a.563.563 0 0 0-.556.479l-1.187 7.527h-.506l-.24 1.516a.56.56 0 0 0 .554.647h3.882c.46 0 .85-.334.922-.788.06-.26.76-4.852.816-5.09a.932.932 0 0 1 .923-.788h.58c3.76 0 6.705-1.528 7.565-5.946.36-1.847.174-3.388-.777-4.471z"/></svg>
    Donar con PayPal
  </a>
</footer>

<script>
const B = "__BASE_PATH__";
const HISTORY_KEY = "yt-dl-history";
const MAX_HISTORY = 5;

let currentUrl  = "";
let currentData = null;
let selectedFmt = "";
let selectedLabel = "";

const urlInput    = document.getElementById("url-input");
const searchBtn   = document.getElementById("search-btn");
const errorBox    = document.getElementById("error-box");
const loadingCard = document.getElementById("loading-card");
const infoCard    = document.getElementById("info-card");
const startedCard = document.getElementById("started-card");
const historyCard = document.getElementById("history-card");
const urlClear    = document.getElementById("url-clear");

// ── Input helpers ─────────────────────────────────────────────────────────────
urlInput.addEventListener("input", () => {
  urlClear.classList.toggle("hidden", !urlInput.value);
});
urlInput.addEventListener("keydown", e => {
  if (e.key === "Enter") fetchInfo();
  if (e.key === "Escape") { if (urlInput.value) clearInput(); else reset(); }
});
document.addEventListener("keydown", e => {
  if (e.key === "Escape" && document.activeElement !== urlInput) reset();
});

function clearInput() {
  urlInput.value = "";
  urlClear.classList.add("hidden");
  setError(null);
  urlInput.focus();
}

function showCards(...ids) {
  ["loading-card","info-card","started-card"]
    .forEach(id => document.getElementById(id).classList.add("hidden"));
  ids.forEach(id => document.getElementById(id).classList.remove("hidden"));
}

function setError(msg) {
  if (msg) { errorBox.textContent = msg; errorBox.classList.remove("hidden"); }
  else      { errorBox.classList.add("hidden"); }
}

// ── Clipboard auto-fill ───────────────────────────────────────────────────────
function isYouTubeUrl(s) {
  return /^https?:\/\/(www\.)?(youtube\.com\/(watch|shorts|live)|youtu\.be\/)/.test(s.trim());
}

async function tryClipboard() {
  try {
    const text = await navigator.clipboard.readText();
    if (isYouTubeUrl(text) && !urlInput.value.trim()) {
      urlInput.value = text.trim();
      urlClear.classList.remove("hidden");
      document.getElementById("clipboard-hint").classList.remove("hidden");
      await fetchInfo();
      document.getElementById("clipboard-hint").classList.add("hidden");
    }
  } catch (_) { /* clipboard permission denied — no-op */ }
}

window.addEventListener("DOMContentLoaded", () => {
  renderHistory();
  tryClipboard();
});

// ── History ───────────────────────────────────────────────────────────────────
function loadHistory() {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]"); }
  catch (_) { return []; }
}

function saveHistory(url, title, thumbnail) {
  let hist = loadHistory().filter(h => h.url !== url);
  hist.unshift({ url, title, thumbnail, ts: Date.now() });
  if (hist.length > MAX_HISTORY) hist = hist.slice(0, MAX_HISTORY);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(hist));
}

function renderHistory() {
  const hist = loadHistory();
  if (!hist.length) { historyCard.classList.add("hidden"); return; }
  historyCard.classList.remove("hidden");
  const list = document.getElementById("history-list");
  list.innerHTML = "";
  hist.forEach(h => {
    const el = document.createElement("div");
    el.className = "history-item";
    const ago = timeAgo(h.ts);
    el.innerHTML = `
      <div class="history-thumb">
        <img src="${h.thumbnail||''}" alt="" loading="lazy" style="width:100%;height:100%;object-fit:cover">
      </div>
      <span class="history-title">${escHtml(h.title)}</span>
      <span class="history-time">${ago}</span>`;
    el.addEventListener("click", () => {
      urlInput.value = h.url;
      urlClear.classList.remove("hidden");
      setError(null);
      fetchInfo();
    });
    list.appendChild(el);
  });
}

function clearHistory() {
  localStorage.removeItem(HISTORY_KEY);
  historyCard.classList.add("hidden");
}

function timeAgo(ts) {
  const s = Math.floor((Date.now() - ts) / 1000);
  if (s < 60)   return "ahora";
  if (s < 3600) return `${Math.floor(s/60)}m`;
  if (s < 86400) return `${Math.floor(s/3600)}h`;
  return `${Math.floor(s/86400)}d`;
}

function escHtml(s) {
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

// ── Fetch info ────────────────────────────────────────────────────────────────
async function fetchInfo() {
  const url = urlInput.value.trim();
  if (!url) { setError("Ingresa una URL de YouTube."); return; }

  setError(null);
  searchBtn.disabled = true;
  showCards("loading-card");

  try {
    const res  = await fetch(B + "/info", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Error desconocido");

    currentUrl  = url;
    currentData = data;
    selectedFmt = "";
    selectedLabel = "";

    // thumbnail
    const thumbEl = document.getElementById("thumb");
    thumbEl.style.opacity = "0";
    document.getElementById("thumb-shimmer").classList.remove("hidden");
    thumbEl.src = data.thumbnail || "";

    // title
    document.getElementById("vtitle").textContent = data.title;

    // meta row
    const metaParts = [];
    if (data.duration) metaParts.push(`⏱ ${data.duration}`);
    if (data.channel)  metaParts.push(escHtml(data.channel));
    if (data.views)    metaParts.push(data.views);
    document.getElementById("vmeta").innerHTML = metaParts.join('<span class="meta-dot"> · </span>');

    // format groups
    const fgEl = document.getElementById("fmt-groups");
    fgEl.innerHTML = "";

    const audioOpts = data.options.filter(o => o.group === "audio");
    const videoOpts = data.options.filter(o => o.group === "video");

    if (audioOpts.length) fgEl.appendChild(buildGroup("audio", audioOpts));
    if (videoOpts.length) fgEl.appendChild(buildGroup("video", videoOpts));

    document.getElementById("dl-btn").disabled = true;
    document.getElementById("dl-btn").innerHTML = `
      <svg width="16" height="16" viewBox="0 0 24 24" fill="white"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>
      Descargar`;
    showCards("info-card");

    saveHistory(url, data.title, data.thumbnail);
    renderHistory();
  } catch (e) {
    setError(e.message);
    showCards();
  } finally {
    searchBtn.disabled = false;
  }
}

function buildGroup(type, opts) {
  const wrap = document.createElement("div");
  wrap.className = "fmt-group";
  const audioIcon = `<svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><path d="M12 3v10.55c-.59-.34-1.27-.55-2-.55-2.21 0-4 1.79-4 4s1.79 4 4 4 4-1.79 4-4V7h4V3h-6z"/></svg>`;
  const videoIcon = `<svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><path d="M17 10.5V7c0-.55-.45-1-1-1H4c-.55 0-1 .45-1 1v10c0 .55.45 1 1 1h12c.55 0 1-.45 1-1v-3.5l4 4v-11l-4 4z"/></svg>`;
  wrap.innerHTML = `<div class="fmt-group-label">${type === "audio" ? audioIcon + " Audio" : videoIcon + " Video"}</div><div class="format-list" id="fmtlist-${type}"></div>`;
  const listEl = wrap.querySelector(`#fmtlist-${type}`);
  opts.forEach(opt => {
    const el = document.createElement("div");
    el.className = "format-opt";
    const badgeHtml = opt.badge
      ? `<span class="qbadge qbadge-${opt.badge.color}">${opt.badge.label}</span>`
      : "";
    el.innerHTML = `
      <div class="rdot"></div>
      <div class="fmt-text">
        <strong>${escHtml(opt.label)}</strong>
        ${opt.detail ? `<small>${escHtml(opt.detail)}</small>` : ""}
      </div>
      ${badgeHtml}`;
    el.addEventListener("click", () => selectFmt(el, opt.key, opt.label));
    listEl.appendChild(el);
  });
  return wrap;
}

function selectFmt(el, key, label) {
  document.querySelectorAll(".format-opt").forEach(o => o.classList.remove("sel"));
  el.classList.add("sel");
  selectedFmt   = key;
  selectedLabel = label;
  document.getElementById("dl-btn").disabled = false;
}

// ── Download ──────────────────────────────────────────────────────────────────
function startDownload() {
  if (!selectedFmt) return;

  const a = document.createElement("a");
  a.href = B + "/dl?url=" + encodeURIComponent(currentUrl)
              + "&choice=" + encodeURIComponent(selectedFmt);
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  // show the started card with filename info
  const ext = selectedFmt === "audio_mp3" ? ".mp3"
            : selectedFmt === "audio_wav" ? ".wav"
            : selectedFmt === "video_original" ? ".mkv" : ".mp4";
  const title = currentData ? currentData.title : "";
  document.getElementById("started-file").textContent = title + ext;
  showCards("started-card");
}

function showInfoCard() {
  showCards("info-card");
}

function reset() {
  urlInput.value = "";
  urlClear.classList.add("hidden");
  currentUrl  = "";
  currentData = null;
  selectedFmt = "";
  selectedLabel = "";
  setError(null);
  document.getElementById("clipboard-hint").classList.add("hidden");
  showCards();
  urlInput.focus();
}
</script>
</body>
</html>"""

HTML = _HTML_TEMPLATE.replace("__BASE_PATH__", BASE_PATH)

# ── Flask app + blueprint ─────────────────────────────────────────────────────

app = Flask(__name__)
bp  = Blueprint("yt", __name__)


@bp.route("/")
def index():
    return HTML, 200, {"Content-Type": "text/html; charset=utf-8"}


@bp.route("/info", methods=["POST"])
def route_info():
    url = (request.json or {}).get("url", "").strip()
    if not url:
        return jsonify({"error": "URL vacía"}), 400

    try:
        with yt_dlp.YoutubeDL({**_SILENT, **EXTRA_OPTS}) as ydl:
            info = ydl.extract_info(url, download=False)
        _info_cache[url] = {"info": info, "ts": time.time()}
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    available_heights = set()
    for f in info.get("formats", []):
        h = f.get("height")
        if h and f.get("vcodec") != "none":
            available_heights.add(h)
    max_height = max(available_heights) if available_heights else None

    options = [
        {
            "key": "audio_mp3", "group": "audio",
            "label": "MP3",
            "detail": "Máxima calidad · Más compatible",
            "badge": None,
        },
        {
            "key": "audio_wav", "group": "audio",
            "label": "WAV",
            "detail": "Sin compresión",
            "badge": None,
        },
    ]

    for res in [480, 720, 1080]:
        if res in available_heights:
            badge = _height_badge(res)
            options.append({
                "key": f"video_{res}", "group": "video",
                "label": f"{res}p con audio",
                "detail": "H.264 · MP4",
                "badge": badge,
            })

    for h in sorted(h for h in available_heights if h > 1080):
        badge = _height_badge(h)
        options.append({
            "key": f"video_{h}", "group": "video",
            "label": f"{h}p con audio",
            "detail": "H.264 · MP4",
            "badge": badge,
        })

    if max_height and max_height not in {480, 720, 1080}:
        badge = _height_badge(max_height)
        options.append({
            "key": "video_original", "group": "video",
            "label": f"Original ({max_height}p)",
            "detail": "Mejor codec disponible · MKV",
            "badge": badge,
        })
    elif max_height:
        options.append({
            "key": "video_original", "group": "video",
            "label": f"Original ({max_height}p)",
            "detail": "AV1/VP9 · MKV",
            "badge": _height_badge(max_height),
        })

    return jsonify({
        "title":     info.get("title", "Sin título"),
        "duration":  _format_duration(info.get("duration", 0)),
        "thumbnail": info.get("thumbnail"),
        "channel":   info.get("uploader") or info.get("channel"),
        "views":     _format_views(info.get("view_count")),
        "options":   options,
    })


@bp.route("/dl")
def route_dl():
    url    = request.args.get("url", "").strip()
    choice = request.args.get("choice", "").strip()
    if not url or not choice:
        abort(400)

    cached = _info_cache.get(url)
    if cached and time.time() - cached["ts"] < 1800:
        info = cached["info"]
    else:
        try:
            with yt_dlp.YoutubeDL({**_SILENT, **EXTRA_OPTS}) as ydl:
                info = ydl.extract_info(url, download=False)
            _info_cache[url] = {"info": info, "ts": time.time()}
        except Exception:
            abort(502)

    try:
        cmd, filename, mime = _build_pipeline(choice, info)
    except Exception:
        abort(500)

    safe = urllib.parse.quote(filename, safe="")

    def stream():
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            while True:
                chunk = proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                proc.kill()
                proc.wait()
            except Exception:
                pass

    return Response(
        stream(),
        headers={
            "Content-Type":        mime,
            "Content-Disposition": f"attachment; filename*=UTF-8''{safe}",
            "X-Accel-Buffering":   "no",
        },
    )


app.register_blueprint(bp, url_prefix=BASE_PATH)
