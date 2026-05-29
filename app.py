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

# Caché de info por URL (evita re-extraer al hacer clic en Descargar)
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
    """Devuelve (cmd_list, filename, mime_type)."""
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

# ── HTML ──────────────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """<!DOCTYPE html>
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

  .url-row { display: flex; gap: 0.5rem; }

  input[type="text"] {
    flex: 1; padding: 0.6rem 0.875rem;
    border: 1.5px solid #e5e7eb; border-radius: 9px;
    font-size: 0.875rem; outline: none; transition: border-color .15s; min-width: 0;
  }
  input[type="text"]:focus     { border-color: #3b82f6; }
  input[type="text"]::placeholder { color: #9ca3af; }

  button {
    padding: 0.6rem 1.1rem; border: none; border-radius: 9px;
    font-size: 0.875rem; font-weight: 600; cursor: pointer;
    transition: background .15s, opacity .15s; white-space: nowrap;
  }
  button:disabled { opacity: .45; cursor: not-allowed; }

  .btn-primary { background: #2563eb; color: white; }
  .btn-primary:hover:not(:disabled) { background: #1d4ed8; }

  .btn-dl {
    background: #16a34a; color: white;
    width: 100%; padding: 0.75rem; margin-top: 1rem; font-size: 0.9375rem;
  }
  .btn-dl:hover:not(:disabled) { background: #15803d; }

  .btn-ghost { background: #f3f4f6; color: #374151; }
  .btn-ghost:hover { background: #e5e7eb; }

  .error-box {
    margin-top: 0.75rem; padding: 0.625rem 0.875rem;
    background: #fef2f2; border: 1px solid #fecaca;
    border-radius: 8px; font-size: 0.8125rem; color: #dc2626; line-height: 1.4;
  }

  .loading-row { display: flex; align-items: center; gap: 0.625rem; color: #6b7280; font-size: 0.875rem; }

  .video-row { display: flex; gap: 0.875rem; align-items: flex-start; }
  .thumb-wrap { flex-shrink: 0; width: 112px; height: 63px; border-radius: 7px; overflow: hidden; background: #f3f4f6; }
  .thumb-wrap img { width: 100%; height: 100%; object-fit: cover; }
  .video-meta h2 { font-size: 0.9rem; font-weight: 600; line-height: 1.45; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .video-meta .dur { margin-top: 0.3rem; font-size: 0.8rem; color: #6b7280; }

  .divider { border: none; border-top: 1px solid #f3f4f6; margin: 1rem 0; }

  .section-label { font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; color: #9ca3af; margin-bottom: 0.625rem; }

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
  .format-opt span { font-size: 0.875rem; }

  /* card de descarga iniciada */
  .started-wrap { display: flex; flex-direction: column; align-items: center; gap: 0.875rem; padding: 0.5rem 0; text-align: center; }
  .started-icon { width: 48px; height: 48px; background: #dbeafe; border-radius: 50%; display: flex; align-items: center; justify-content: center; }
  .started-icon svg { fill: #2563eb; }
  .started-title { font-size: 0.9375rem; font-weight: 600; }
  .started-sub   { font-size: 0.8125rem; color: #6b7280; line-height: 1.5; }

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

  .spin { width: 15px; height: 15px; border: 2px solid #e5e7eb; border-top-color: #3b82f6; border-radius: 50%; flex-shrink: 0; animation: rot .7s linear infinite; }
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

  <div class="card">
    <div class="url-row">
      <input type="text" id="url-input"
             placeholder="https://www.youtube.com/watch?v=..."
             autocomplete="off" spellcheck="false">
      <button class="btn-primary" id="search-btn" onclick="fetchInfo()">Buscar</button>
    </div>
    <div id="error-box" class="error-box hidden"></div>
  </div>

  <div class="card hidden" id="loading-card">
    <div class="loading-row"><div class="spin"></div><span>Obteniendo información del video...</span></div>
  </div>

  <div class="card hidden" id="info-card">
    <div class="video-row">
      <div class="thumb-wrap"><img id="thumb" src="" alt=""></div>
      <div class="video-meta">
        <h2 id="vtitle"></h2>
        <div class="dur" id="vdur"></div>
      </div>
    </div>
    <hr class="divider">
    <div class="section-label">Formato de descarga</div>
    <div class="format-list" id="fmt-list"></div>
    <button class="btn-dl" id="dl-btn" onclick="startDownload()" disabled>Descargar</button>
  </div>

  <div class="card hidden" id="started-card">
    <div class="started-wrap">
      <div class="started-icon">
        <svg width="24" height="24" viewBox="0 0 24 24"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>
      </div>
      <div class="started-title">Descarga en progreso</div>
      <div class="started-sub">El archivo llega directo a tu navegador.<br>Puedes ver el progreso en la barra de descargas.</div>
      <button class="btn-ghost" style="margin-top:.25rem" onclick="reset()">Descargar otro</button>
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

  let currentUrl = "";
  let selectedFmt = "";

  const urlInput    = document.getElementById("url-input");
  const searchBtn   = document.getElementById("search-btn");
  const errorBox    = document.getElementById("error-box");
  const loadingCard = document.getElementById("loading-card");
  const infoCard    = document.getElementById("info-card");
  const startedCard = document.getElementById("started-card");

  urlInput.addEventListener("keydown", e => { if (e.key === "Enter") fetchInfo(); });

  function showCards(...ids) {
    ["loading-card","info-card","started-card"]
      .forEach(id => document.getElementById(id).classList.add("hidden"));
    ids.forEach(id => document.getElementById(id).classList.remove("hidden"));
  }

  function setError(msg) {
    if (msg) { errorBox.textContent = msg; errorBox.classList.remove("hidden"); }
    else      { errorBox.classList.add("hidden"); }
  }

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
      selectedFmt = "";

      document.getElementById("thumb").src          = data.thumbnail || "";
      document.getElementById("vtitle").textContent = data.title;
      document.getElementById("vdur").textContent   = "⏱ " + data.duration;

      const list = document.getElementById("fmt-list");
      list.innerHTML = "";
      data.options.forEach(opt => {
        const el = document.createElement("div");
        el.className = "format-opt";
        el.innerHTML = `<div class="rdot"></div><span>${opt.label}</span>`;
        el.addEventListener("click", () => selectFmt(el, opt.key));
        list.appendChild(el);
      });

      document.getElementById("dl-btn").disabled = true;
      document.getElementById("dl-btn").textContent = "Descargar";
      showCards("info-card");
    } catch (e) {
      setError(e.message);
      showCards();
    } finally {
      searchBtn.disabled = false;
    }
  }

  function selectFmt(el, key) {
    document.querySelectorAll(".format-opt").forEach(o => o.classList.remove("sel"));
    el.classList.add("sel");
    selectedFmt = key;
    document.getElementById("dl-btn").disabled = false;
  }

  function startDownload() {
    if (!selectedFmt) return;

    // Dispara la descarga directamente al navegador (sin pasar por /tmp)
    const a = document.createElement("a");
    a.href = B + "/dl?url=" + encodeURIComponent(currentUrl)
                + "&choice=" + encodeURIComponent(selectedFmt);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    showCards("started-card");
  }

  function reset() {
    urlInput.value = "";
    currentUrl = "";
    selectedFmt = "";
    setError(null);
    showCards();
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
        {"key": "audio_mp3", "label": "Audio MP3 (máxima calidad)"},
        {"key": "audio_wav", "label": "Audio WAV (sin pérdida)"},
    ]
    for res in [720, 1080]:
        if res in available_heights:
            options.append({"key": f"video_{res}", "label": f"Video {res}p con audio"})
    for h in sorted(h for h in available_heights if h > 1080):
        options.append({"key": f"video_{h}", "label": f"Video {h}p con audio"})
    if max_height and max_height not in {720, 1080}:
        options.append({"key": "video_original",
                        "label": f"Video calidad original ({max_height}p, mejor disponible) con audio"})
    elif max_height:
        options.append({"key": "video_original",
                        "label": f"Video calidad original (AV1/{max_height}p, menor tamaño) con audio"})

    mins, secs = divmod(info.get("duration", 0), 60)
    return jsonify({
        "title":     info.get("title", "Sin título"),
        "duration":  f"{mins}:{secs:02d}",
        "thumbnail": info.get("thumbnail"),
        "options":   options,
    })


@bp.route("/dl")
def route_dl():
    url    = request.args.get("url", "").strip()
    choice = request.args.get("choice", "").strip()
    if not url or not choice:
        abort(400)

    # Usar caché si está disponible, si no re-extraer
    cached = _info_cache.get(url)
    if cached and time.time() - cached["ts"] < 1800:
        info = cached["info"]
    else:
        try:
            with yt_dlp.YoutubeDL({**_SILENT, **EXTRA_OPTS}) as ydl:
                info = ydl.extract_info(url, download=False)
            _info_cache[url] = {"info": info, "ts": time.time()}
        except Exception as e:
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
