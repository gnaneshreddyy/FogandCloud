"""Local mock of the cloud layer (same API as the AWS Lambda), backed by SQLite.

Use it for offline development; final results should use the real AWS
endpoint (simulation.py --cloud aws). The simulator adds an emulated WAN
delay on top of this mock so its numbers are not unrealistically fast.

    python cloud.py --serve                     # listen on 127.0.0.1:8080
    python cloud.py --serve --port 8081 --db results/x/cloud.db
    python cloud.py --dump                      # row counts + latest alerts
    python cloud.py --reset                     # delete the default database
"""
import argparse
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common import config
from common.cloud_core import decode_body, handle

DEFAULT_DB = config.RESULTS_DIR / "cloud_local.db"


class SqliteStore:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS items (
            kind TEXT, patient TEXT, item_id TEXT, source TEXT, received REAL, body TEXT)""")
        self.lock = threading.Lock()

    def put(self, kind, items, source, received):
        rows = [(kind, it.get("patient"), it.get("alert_id") or it.get("reading_id")
                 or it.get("first_reading"), source, received, json.dumps(it)) for it in items]
        with self.lock:
            self.db.executemany("INSERT INTO items VALUES (?,?,?,?,?,?)", rows)
            self.db.commit()


def make_handler(store):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"   # keep-alive, like API Gateway / Function URLs

        def _reply(self, code, obj):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._reply(200, {"ok": True, "service": "icu-cloud-local"})

        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            try:
                body = decode_body(raw, self.headers.get("Content-Encoding", ""))
                self._reply(200, handle(body, store))
            except Exception as exc:  # malformed request must not kill the server
                self._reply(400, {"ok": False, "error": str(exc)})

        def log_message(self, *args):
            pass

    return Handler


def serve(port, db):
    store = SqliteStore(db)
    server = ThreadingHTTPServer((config.HOST, port), make_handler(store))
    server.daemon_threads = True
    print(f"[cloud] local mock listening on http://{config.HOST}:{port}  db={db}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def dump(db):
    if not Path(db).exists():
        print(f"no database at {db}")
        return
    con = sqlite3.connect(str(db))
    for kind, source, n in con.execute(
            "SELECT kind, source, COUNT(*) FROM items GROUP BY kind, source ORDER BY kind, source"):
        print(f"{kind:8s} {source:12s} {n}")
    print("\nlatest alerts:")
    for (body,) in con.execute("SELECT body FROM items WHERE kind='alert' ORDER BY rowid DESC LIMIT 10"):
        a = json.loads(body)
        print(f"  {a.get('patient')} {a.get('vital')}={a.get('value')} ({a.get('direction')}) "
              f"by {a.get('detector')}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--serve", action="store_true", help="run the local mock cloud")
    mode.add_argument("--dump", action="store_true", help="print what the cloud has stored")
    mode.add_argument("--reset", action="store_true", help="delete the database")
    ap.add_argument("--port", type=int, default=config.LOCAL_CLOUD_PORT)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    args = ap.parse_args()

    if args.serve:
        serve(args.port, args.db)
    elif args.dump:
        dump(args.db)
    else:
        for suffix in ("", "-wal", "-shm"):
            Path(args.db + suffix).unlink(missing_ok=True)
        print(f"deleted {args.db}")


if __name__ == "__main__":
    main()
