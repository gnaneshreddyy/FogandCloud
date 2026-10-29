"""Everything that talks to the cloud: HTTP client, byte accounting, buffering.

All cloud-bound traffic in every architecture goes through CloudClient, so
"cloud-bound bytes" and PDR are measured the same way everywhere.
"""
import asyncio
import gzip
import http.client
import itertools
import json
import os
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

from . import config
from .logs import CsvLog
from .netem import LinkError, NetEm

UPLINK_FIELDS = ["t_send", "t_done", "t_enqueue", "component", "msg_id", "kind", "attempt",
                 "bytes", "ok", "rtt_ms", "error", "n_readings", "n_alerts", "n_summaries"]

_local = threading.local()
_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="uplink")


def _conn(url):
    """One keep-alive connection per worker thread (like a real HTTP client pool)."""
    parts = urlsplit(url)
    key = (parts.scheme, parts.netloc)
    pool = getattr(_local, "pool", None)
    if pool is None:
        pool = _local.pool = {}
    conn = pool.get(key)
    if conn is None:
        cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
        conn = pool[key] = cls(parts.netloc, timeout=config.HTTP_TIMEOUT_S)
    return conn, parts.path or "/"


def _http_post(url, data: bytes, headers: dict) -> dict:
    for retry in (0, 1):  # one silent retry if a pooled keep-alive socket went stale
        conn, path = _conn(url)
        try:
            conn.request("POST", path, body=data, headers=headers)
            resp = conn.getresponse()
            body = resp.read()
            if resp.status >= 400:
                raise LinkError(f"http-{resp.status}")
            return json.loads(body or b"{}")
        except (http.client.HTTPException, ConnectionError, OSError) as exc:
            conn.close()
            _local.pool.pop((urlsplit(url).scheme, urlsplit(url).netloc), None)
            if retry or isinstance(exc, TimeoutError):
                raise LinkError(type(exc).__name__) from exc


class CloudClient:
    def __init__(self, url: str, netem: NetEm, run_dir, component: str, compress: bool = False):
        self.url = url
        self.netem = netem
        self.component = component
        self.compress = compress
        self.log = CsvLog(f"{run_dir}/uplink_{component}.csv", UPLINK_FIELDS)
        self._ids = itertools.count(1)

    def new_msg_id(self) -> str:
        # pid keeps ids unique when a crashed edge is restarted into the same run
        return f"{self.component}-{os.getpid()}-{next(self._ids)}"

    async def post(self, body: dict, attempt: int = 1, t_enqueue=None) -> dict:
        """One attempt to deliver `body`. Raises LinkError on any failure."""
        data = json.dumps(body, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json"}
        if self.compress:
            data = gzip.compress(data)
            # binary content type so Lambda Function URLs base64-encode instead of mangling it
            headers = {"Content-Type": "application/octet-stream", "Content-Encoding": "gzip"}
        nbytes = len(data) + config.HTTP_HEADER_OVERHEAD
        t_send = config.now()
        row = dict(t_send=t_send, t_enqueue=t_enqueue if t_enqueue is not None else t_send,
                   component=self.component, msg_id=body["msg_id"], kind=body["kind"],
                   attempt=attempt, bytes=nbytes,
                   n_readings=len(body.get("readings", [])),
                   n_alerts=len(body.get("alerts", [])),
                   n_summaries=len(body.get("summaries", [])))
        try:
            await self.netem.before_send(nbytes, t_send)
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(_executor, _http_post, self.url, data, headers)
            await self.netem.after_receive()
        except LinkError as exc:
            t_done = config.now()
            self.log.write(**row, t_done=t_done, ok=0, rtt_ms=round((t_done - t_send) * 1000, 2),
                           error=str(exc))
            raise
        t_done = config.now()
        self.log.write(**row, t_done=t_done, ok=1, rtt_ms=round((t_done - t_send) * 1000, 2), error="")
        return resp


class BufferedUploader:
    """Store-and-forward queue in front of a CloudClient.

    Alerts always jump ahead of summaries. `max_items=None` means unbounded
    (fog); a bounded queue (edge) drops its oldest entry when full.
    """

    def __init__(self, client: CloudClient, run_dir, component: str, max_items=None):
        self.client = client
        self.max_items = max_items
        self.alerts = deque()
        self.other = deque()
        self.dropped = 0
        self.buffer_log = CsvLog(f"{run_dir}/buffer_{component}.csv", ["t", "component", "size", "dropped"])
        self.component = component
        self._wake = asyncio.Event()
        self._closing = False

    def __len__(self):
        return len(self.alerts) + len(self.other)

    def enqueue(self, body: dict):
        body.setdefault("msg_id", self.client.new_msg_id())
        item = (config.now(), body)
        (self.alerts if body["kind"] == "alert" else self.other).append(item)
        if self.max_items is not None:
            while len(self) > self.max_items:
                (self.other or self.alerts).popleft()
                self.dropped += 1
        self._wake.set()

    async def run(self):
        sampler = asyncio.create_task(self._sample())
        try:
            while not (self._closing and not len(self)):
                if not len(self):
                    self._wake.clear()
                    try:
                        await asyncio.wait_for(self._wake.wait(), 0.5)
                    except asyncio.TimeoutError:
                        pass
                    continue
                queue = self.alerts or self.other
                t_enq, body = queue[0]
                attempt = body.setdefault("_attempt", 0) + 1
                body["_attempt"] = attempt
                send_body = {k: v for k, v in body.items() if k != "_attempt"}
                try:
                    await self.client.post(send_body, attempt=attempt, t_enqueue=t_enq)
                except LinkError:
                    await asyncio.sleep(config.RETRY_INTERVAL_S)
                    continue
                if queue and queue[0][1] is body:
                    queue.popleft()
        finally:
            sampler.cancel()

    async def _sample(self):
        last = None
        while True:
            size = len(self)
            if size != last:
                self.buffer_log.write(t=config.now(), component=self.component, size=size,
                                      dropped=self.dropped)
                last = size
            await asyncio.sleep(0.25)

    def close(self):
        self._closing = True
        self._wake.set()
