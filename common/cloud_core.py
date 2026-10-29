"""Cloud ingest logic shared by the local mock (cloud.py) and AWS Lambda.

Request body (JSON, optionally gzip):
    {"msg_id": str, "kind": "reading"|"alert"|"summary"|"batch",
     "source": str, "detect": bool,
     "readings": [...], "alerts": [...], "summaries": [...]}

If "detect" is true the cloud runs threshold detection on the readings
(architecture A, and B's fallback when an edge is down) and returns any
alerts in the response so the bedside can raise them.
Pure stdlib: shipped inside the Lambda zip.
"""
import gzip
import json
import time

from . import detection


def decode_body(raw: bytes, content_encoding: str = "") -> dict:
    if content_encoding.lower() == "gzip" or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def handle(body: dict, store) -> dict:
    """Detect (if asked), persist, and build the response. `store` has .put(kind, items)."""
    received = time.time()
    readings = body.get("readings", [])
    detected = []
    if body.get("detect"):
        for reading in readings:
            detected.extend(detection.check(reading, "cloud"))
    alerts = body.get("alerts", []) + detected
    for kind, items in (("reading", readings), ("alert", alerts), ("summary", body.get("summaries", []))):
        if items:
            store.put(kind, items, source=body.get("source", "?"), received=received)
    return {
        "ok": True,
        "msg_id": body.get("msg_id"),
        "alerts": detected,
        "stored": {"readings": len(readings), "alerts": len(alerts),
                   "summaries": len(body.get("summaries", []))},
        "server_time": received,
    }
