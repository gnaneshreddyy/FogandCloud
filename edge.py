"""Edge layer: one bedside process per patient.

Receives that patient's raw readings over local TCP, runs the threshold
check immediately and raises the alert locally (no network involved).
Raw readings never leave the bedside; only alerts and 5 s summaries do:
    B: alerts + summaries -> cloud, through a small bounded RAM buffer
    C: alerts + summaries -> fog over the ward LAN

    python edge.py --patient P01 --run-config results/runs/<run>/run_config.json
Started (and, in the edge-failure scenario, killed) by simulation.py.
"""
import argparse
import asyncio
from collections import deque

from common import config, transport
from common.detection import WindowSummarizer, check
from common.logs import alert_log, event_log
from common.netem import NetEm
from common.uplink import BufferedUploader, CloudClient


class Edge:
    def __init__(self, rc, patient):
        self.rc = rc
        self.patient = patient
        self.name = f"edge-{patient}"
        self.arch = rc["arch"]
        self.run_dir = rc["run_dir"]
        self.local_delay = rc.get("local_delay_ms", 0)
        self.alerts = alert_log(self.run_dir, self.name)
        self.events = event_log(self.run_dir, self.name)
        self.summarizer = WindowSummarizer(patient, config.SUMMARY_WINDOW_S)
        self.stop = asyncio.Event()
        self.uploader = None
        if self.arch == "B":
            uplink = dict(rc["uplink"])
            if uplink.get("bandwidth_kbps"):  # each bedside gets a fair share of the site uplink
                uplink["bandwidth_kbps"] /= rc["patients"]
            netem = NetEm(uplink, rc["t0"], seed=rc["seed"] + int(patient[1:]),
                          base_delay_ms=rc.get("wan_ms", 0))
            client = CloudClient(rc["cloud_url"], netem, self.run_dir, self.name)
            self.uploader = BufferedUploader(client, self.run_dir, self.name, max_items=config.EDGE_BUFFER_MAX)
        self.fog_writer = None
        self.fog_pending = deque(maxlen=config.EDGE_BUFFER_MAX)

    # -- upstream ---------------------------------------------------------
    async def forward(self, msg):
        if self.arch == "B":
            self.uploader.enqueue({"kind": msg["type"], "source": self.name,
                                   "alerts": msg.get("alerts", []),
                                   "summaries": msg.get("summaries", [])})
            return
        self.fog_pending.append(msg)
        while self.fog_pending:
            try:
                if self.fog_writer is None or self.fog_writer.is_closing():
                    _, self.fog_writer = await transport.connect(config.HOST, config.FOG_PORT)
                await transport.send(self.fog_writer, self.fog_pending[0], self.local_delay)
                self.fog_pending.popleft()
            except (OSError, asyncio.TimeoutError):
                self.fog_writer = None
                return  # fog unreachable: keep it pending, retry on the next message

    # -- downstream (sensor) ----------------------------------------------
    async def on_reading(self, reading):
        found = check(reading, "edge")
        t = config.now()
        for a in found:   # the point-of-care alarm: raised before anything is sent anywhere
            self.alerts.write(t_alert=t, component=self.name, path="edge", **{
                k: a[k] for k in ("alert_id", "patient", "reading_id", "vital", "value",
                                  "direction", "detector")})
        if found:
            await self.forward({"type": "alert", "source": self.name, "alerts": found})
        self.summarizer.add(reading, t)
        if self.summarizer.due(t):
            await self.flush_summary()

    async def flush_summary(self):
        summary = self.summarizer.flush()
        if summary:
            await self.forward({"type": "summary", "source": self.name, "summaries": [summary]})

    async def handle(self, reader, writer):
        async for msg in transport.messages(reader):
            if msg.get("type") == "reading":
                await self.on_reading(msg["reading"])
            elif msg.get("type") == "shutdown":
                self.stop.set()
                break
        writer.close()

    async def run(self, port):
        server = await asyncio.start_server(self.handle, config.HOST, port)
        upload_task = asyncio.create_task(self.uploader.run()) if self.uploader is not None else None
        self.events.write(t=config.now(), component=self.name, event="start", detail=str(port))
        print(f"[{self.name}] listening on {port} (arch {self.arch})", flush=True)
        async with server:
            await self.stop.wait()
        await self.flush_summary()
        if upload_task:
            self.uploader.close()
            try:
                await asyncio.wait_for(upload_task, config.DRAIN_GRACE_S)
            except asyncio.TimeoutError:
                pass
            self.events.write(t=config.now(), component=self.name, event="exit",
                              detail=f"unsent={len(self.uploader)} dropped={self.uploader.dropped}")
        else:
            self.events.write(t=config.now(), component=self.name, event="exit",
                              detail=f"unsent={len(self.fog_pending)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--patient", required=True, help="e.g. P01")
    ap.add_argument("--run-config", required=True)
    ap.add_argument("--port", type=int, help="default: %d + patient number" % config.EDGE_BASE_PORT)
    args = ap.parse_args()
    rc = config.load_run_config(args.run_config)
    asyncio.run(Edge(rc, args.patient).run(args.port or config.edge_port(args.patient)))


if __name__ == "__main__":
    main()
