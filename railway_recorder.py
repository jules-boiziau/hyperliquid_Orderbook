#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
 RAILWAY RECORDER — carnet d'ordres Hyperliquid, collecte 24/7
═══════════════════════════════════════════════════════════════════════
 Service de collecte durci pour Railway :
   · reconnexion automatique au WebSocket (backoff progressif)
   · rotation quotidienne : un fichier book_YYYYMMDD.jsonl par jour
   · fenêtre d'enregistrement (par défaut session US élargie) pour
     économiser le volume — le carnet de nuit ne sert à rien
   · mini serveur HTTP pour télécharger les fichiers depuis l'URL
     publique Railway, protégé par token

 Variables d'environnement :
   COIN            marché à enregistrer      (défaut: xyz:XYZ100)
   DATA_DIR        dossier des fichiers      (défaut: /data)
   RECORD_WINDOW   fenêtre UTC hh:mm-hh:mm   (défaut: 13:00-20:30)
                   mettre "always" pour enregistrer 24/7
   DOWNLOAD_TOKEN  token requis pour télécharger (OBLIGATOIRE si le
                   service a un domaine public)
   PORT            port HTTP (fourni par Railway automatiquement)

 Téléchargement depuis ton PC :
   liste    : https://<app>.up.railway.app/?token=TON_TOKEN
   fichier  : https://<app>.up.railway.app/file?name=book_20260708.jsonl&token=TON_TOKEN

 Format identique au script local → replay/analyze directement dessus.
═══════════════════════════════════════════════════════════════════════
"""

import asyncio
import json
import os
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

WS_URL = "wss://api.hyperliquid.xyz/ws"

COIN = os.environ.get("COIN", "xyz:XYZ100")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
RECORD_WINDOW = os.environ.get("RECORD_WINDOW", "13:00-20:30")
DOWNLOAD_TOKEN = os.environ.get("DOWNLOAD_TOKEN", "")
PORT = int(os.environ.get("PORT", "8080"))


# ─────────────────────────────────────────────────────────────────────
# Fenêtre d'enregistrement (UTC)
# ─────────────────────────────────────────────────────────────────────
def _parse_window(spec):
    if spec.strip().lower() == "always":
        return None
    try:
        a, b = spec.split("-")
        h1, m1 = map(int, a.split(":"))
        h2, m2 = map(int, b.split(":"))
        return (h1 * 60 + m1, h2 * 60 + m2)
    except ValueError:
        print(f"RECORD_WINDOW invalide ({spec}), fallback 13:00-20:30")
        return (13 * 60, 20 * 60 + 30)


WINDOW = _parse_window(RECORD_WINDOW)


def in_window(ts):
    if WINDOW is None:
        return True
    tm = time.gmtime(ts)
    mins = tm.tm_hour * 60 + tm.tm_min
    return WINDOW[0] <= mins < WINDOW[1]


def day_path(ts):
    return os.path.join(DATA_DIR, "book_" + time.strftime("%Y%m%d", time.gmtime(ts)) + ".jsonl")


# ─────────────────────────────────────────────────────────────────────
# Boucle d'enregistrement avec reconnexion automatique
# ─────────────────────────────────────────────────────────────────────
async def recorder_loop():
    import websockets

    os.makedirs(DATA_DIR, exist_ok=True)
    backoff = 2
    n_total = 0
    last_log = 0.0
    cur_path = None
    fh = None

    while True:
        try:
            print(f"Connexion WS → {COIN} ...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                for sub in ("l2Book", "trades"):
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": sub, "coin": COIN},
                    }))
                print("Connecté, abonnements envoyés.")
                backoff = 2

                async for raw in ws:
                    ts = time.time()
                    if not in_window(ts):
                        # Hors fenêtre : on garde la connexion vivante mais on n'écrit pas
                        if fh is not None:
                            fh.close()
                            fh = None
                            cur_path = None
                            print(f"[{_fmt(ts)}] hors fenêtre {RECORD_WINDOW} UTC — écriture en pause")
                        continue

                    msg = json.loads(raw)
                    ch = msg.get("channel")
                    if ch not in ("l2Book", "trades"):
                        continue

                    # Rotation quotidienne
                    path = day_path(ts)
                    if path != cur_path:
                        if fh is not None:
                            fh.close()
                        fh = open(path, "a")
                        cur_path = path
                        print(f"[{_fmt(ts)}] écriture → {os.path.basename(path)}")

                    fh.write(json.dumps({"t": ts, "ch": ch, "data": msg["data"]}) + "\n")
                    n_total += 1

                    if ts - last_log >= 300:  # log toutes les 5 min
                        last_log = ts
                        fh.flush()
                        size_mb = os.path.getsize(cur_path) / 1e6 if cur_path else 0
                        print(f"[{_fmt(ts)}] {n_total} messages cumulés — "
                              f"{os.path.basename(cur_path)} : {size_mb:.1f} Mo")

        except Exception as e:
            if fh is not None:
                fh.close()
                fh = None
                cur_path = None
            print(f"Déconnexion ({type(e).__name__}: {e}) — reconnexion dans {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def _fmt(ts):
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + "Z"


# ─────────────────────────────────────────────────────────────────────
# Serveur HTTP de téléchargement (token requis)
# ─────────────────────────────────────────────────────────────────────
class DownloadHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # silence les logs d'accès

    def _deny(self, code, txt):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(txt.encode())

    def do_GET(self):
        url = urlparse(self.path)
        qs = parse_qs(url.query)
        token = qs.get("token", [""])[0]
        if not DOWNLOAD_TOKEN or token != DOWNLOAD_TOKEN:
            return self._deny(403, "Token invalide ou DOWNLOAD_TOKEN non configuré.")

        if url.path == "/":
            files = sorted(f for f in os.listdir(DATA_DIR) if f.endswith(".jsonl"))
            lines = []
            for f in files:
                size = os.path.getsize(os.path.join(DATA_DIR, f)) / 1e6
                lines.append(f"{f}  ({size:.1f} Mo)")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            body = "Fichiers disponibles :\n" + "\n".join(lines) if lines else "Aucun fichier."
            self.wfile.write(body.encode())
            return

        if url.path == "/file":
            name = os.path.basename(qs.get("name", [""])[0])  # anti path-traversal
            full = os.path.join(DATA_DIR, name)
            if not name.endswith(".jsonl") or not os.path.exists(full):
                return self._deny(404, "Fichier introuvable.")
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{name}"')
            self.send_header("Content-Length", str(os.path.getsize(full)))
            self.end_headers()
            with open(full, "rb") as fh:
                while chunk := fh.read(1 << 20):
                    self.wfile.write(chunk)
            return

        self._deny(404, "Routes : /  et  /file?name=...")


def start_http():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), DownloadHandler)
    print(f"Serveur de téléchargement sur :{PORT} "
          f"({'token configuré' if DOWNLOAD_TOKEN else '⚠ DOWNLOAD_TOKEN manquant — téléchargement bloqué'})")
    srv.serve_forever()


# ─────────────────────────────────────────────────────────────────────
def main():
    print(f"Railway recorder — coin={COIN}, fenêtre={RECORD_WINDOW} UTC, data={DATA_DIR}")
    threading.Thread(target=start_http, daemon=True).start()
    asyncio.run(recorder_loop())


if __name__ == "__main__":
    main()
