"""Fog layer (architecture C): one ward-level server for all bedside edges.

    * dedupes alerts: one cloud alert per patient+vital per cooldown window
    * batches all patients' summaries into one gzip request every 2 s
    * store-and-forward: unbounded buffer while the cloud is unreachable
    * ward coordination: raises a ward alert when several patients alarm together
    * backup detection: if a bedside edge dies, sensors fall back to the fog,
      which runs the same threshold check and raises the alert locally

    python fog.py --run-config results/runs/<run>/run_config.json
Started by simulation.py.
"""
import argparse
import asyncio

from common import config, transport
from common.detection import WindowSummarizer, check
from common.logs import alert_log, event_log
from common.netem import NetEm
from common.uplink import BufferedUploader, CloudClient

NAME = "fog"


class Fog:
    def __init__(self, rc):
        self.rc = rc
        self.run_dir = rc["run_dir"]
        self.alerts = alert_log(self.run_dir, NAME)
        self.events = event_log(self.run_dir, NAME)
        netem = NetEm(rc["uplink"], rc["t0"], seed=rc["seed"] + 1000, base_delay_ms=rc.get("wan_ms", 0))
        client = CloudClient(rc["cloud_url"], netem, self.run_dir, NAME, compress=True)
        self.uploader = BufferedUploader(client, self.run_dir, NAME, max_items=None)
        self.pending_alerts, self.pending_summaries = [], []
        self.last_sent = {}             # (patient, vital) -> time of last forwarded alert
        self.recent_patients = {}       # patient -> last alert time (ward coordination)
        self.last_ward_alert = -1e9
        self.fallback_summaries = {}    # patient -> WindowSummarizer for edge-less patients
        self.stats = {"alerts_in": 0, "alerts_forwarded": 0, "summaries_in": 0, "fallback_readings": 0}
        self.stop = asyncio.Event()

    def _log_alert(self, a, t, path):
        self.alerts.write(t_alert=t, component=NAME, path=path, **{
            k: a.get(k) for k in ("alert_id", "patient", "reading_id", "vital", "value",
                                  "direction", "detector")})

    def on_alerts(self, alerts):
        t = config.now()
        for a in alerts:
            self.stats["alerts_in"] += 1
            self.recent_patients[a["patient"]] = t
            key = (a["patient"], a["vital"])
            if t - self.last_sent.get(key, -1e9) >= config.FOG_DEDUP_COOLDOWN_S:
                self.last_sent[key] = t
                self.pending_alerts.append(a)
                self.stats["alerts_forwarded"] += 1
            else:
                self.last_sent[key] = t   # still alarming: extend the suppression window
        self.check_ward(t)

    def check_ward(self, t):
        active = [p for p, ts in self.recent_patients.items() if t - ts <= config.WARD_ALERT_WINDOW_S]
        if len(active) >= config.WARD_ALERT_MIN_PATIENTS and t - self.last_ward_alert >= config.WARD_ALERT_WINDOW_S:
            self.last_ward_alert = t
            ward = {"alert_id": f"WARD:{int(t * 1000)}", "patient": "WARD", "reading_id": "",
                    "vital": "multi", "value": len(active), "direction": "high", "detector": "fog",
                    "limit": config.WARD_ALERT_MIN_PATIENTS, "patients": sorted(active)}
            self._log_alert(ward, t, "fog-ward")
            self.pending_alerts.append(ward)

    def on_fallback_reading(self, reading):
        """An edge is down: the fog runs the bedside check itself."""
        self.stats["fallback_readings"] += 1
        found = check(reading, "fog")
        t = config.now()
        for a in found:
            self._log_alert(a, t, "fog-fallback")
        if found:
            self.on_alerts(found)
        summ = self.fallback_summaries.setdefault(
            reading["patient"], WindowSummarizer(reading["patient"], config.SUMMARY_WINDOW_S))
        summ.add(reading, t)
        if summ.due(t):
            self.pending_summaries.append(summ.flush())

    async def handle(self, reader, writer):
        async for msg in transport.messages(reader):
            kind = msg.get("type")
            if kind == "alert":
                self.on_alerts(msg["alerts"])
            elif kind == "summary":
                self.stats["summaries_in"] += len(msg["summaries"])
                self.pending_summaries.extend(msg["summaries"])
            elif kind == "reading":
                self.on_fallback_reading(msg["reading"])
            elif kind == "shutdown":
                self.stop.set()
                break
        writer.close()

    def flush(self, include_summaries):
        if self.pending_alerts:
            self.uploader.enqueue({"kind": "alert", "source": NAME, "alerts": self.pending_alerts})
            self.pending_alerts = []
        if include_summaries and self.pending_summaries:
            self.uploader.enqueue({"kind": "batch", "source": NAME, "summaries": self.pending_summaries})
            self.pending_summaries = []

    async def flusher(self):
        ticks_per_batch = max(1, round(config.FOG_FLUSH_S / config.FOG_ALERT_FLUSH_S))
        tick = 0
        while not self.stop.is_set():
            await asyncio.sleep(config.FOG_ALERT_FLUSH_S)
            tick += 1
            self.flush(include_summaries=tick % ticks_per_batch == 0)

    async def run(self, port):
        server = await asyncio.start_server(self.handle, config.HOST, port)
        upload_task = asyncio.create_task(self.uploader.run())
        flush_task = asyncio.create_task(self.flusher())
        self.events.write(t=config.now(), component=NAME, event="start", detail=str(port))
        print(f"[fog] listening on {port}", flush=True)
        async with server:
            await self.stop.wait()
        await asyncio.sleep(0.3)        # let edges' final summaries land
        flush_task.cancel()
        self.flush(include_summaries=True)
        self.uploader.close()
        try:
            await asyncio.wait_for(upload_task, config.DRAIN_GRACE_S)
        except asyncio.TimeoutError:
            pass
        detail = " ".join(f"{k}={v}" for k, v in self.stats.items())
        self.events.write(t=config.now(), component=NAME, event="exit",
                          detail=f"{detail} unsent={len(self.uploader)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-config", required=True)
    ap.add_argument("--port", type=int, default=config.FOG_PORT)
    args = ap.parse_args()
    asyncio.run(Fog(config.load_run_config(args.run_config)).run(args.port))


if __name__ == "__main__":
    main()
