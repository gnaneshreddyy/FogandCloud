"""Sensor layer: synthetic ICU vitals with labelled anomaly episodes.

    python generator.py --generate                     # data/dataset.csv + data/episodes.csv
    python generator.py --generate --patients 10 --duration 240 --seed 7
    python generator.py --stats                        # summarise the saved dataset
    python generator.py --replay --run-config results/runs/<run>/run_config.json

--replay is started by simulation.py. It streams the saved dataset in real
time (1 Hz per patient) along the path of the architecture under test:
    A: every reading -> cloud (cloud detects, alert returned in the response)
    B: reading -> bedside edge; if the edge is down -> straight to cloud
    C: reading -> bedside edge; if the edge is down -> fog (fog detects)
The "episode" column is ground truth for metrics only; it is never sent.
"""
import argparse
import asyncio
import csv
import random
import sys

from common import config, transport
from common.detection import VITALS
from common.logs import CsvLog, alert_log, event_log
from common.netem import LinkError, NetEm
from common.uplink import CloudClient

# baseline ranges sit well inside the alarm thresholds
BASELINE = {"hr": (65, 95), "spo2": (95, 99), "temp": (36.4, 37.2), "sbp": (105, 135), "rr": (12, 18)}
SAFE_BAND = {"hr": (50, 120), "spo2": (92, 100), "temp": (35.6, 38.1), "sbp": (95, 170), "rr": (10, 26)}
NOISE = {"hr": 1.5, "spo2": 0.4, "temp": 0.03, "sbp": 2.0, "rr": 0.6}
DECIMALS = {"hr": 0, "spo2": 0, "temp": 1, "sbp": 0, "rr": 0}

EPISODE_TYPES = {  # name: (vital, abnormal range)
    "desaturation": ("spo2", (82, 88)),
    "tachycardia": ("hr", (138, 160)),
    "bradycardia": ("hr", (32, 38)),
    "fever": ("temp", (38.8, 39.6)),
    "hypotension": ("sbp", (70, 85)),
    "hypertension": ("sbp", (186, 205)),
    "tachypnea": ("rr", (32, 40)),
}
EPISODE_LEN = (6, 15)    # seconds
EPISODE_GAP = (18, 40)   # seconds between episodes of one patient
FIELDS = ["reading_id", "patient", "seq", "t", *VITALS, "episode"]


# ---------------------------------------------------------------- generate
def generate(n_patients, duration, seed):
    rng = random.Random(seed)
    readings, episodes = [], []
    for pid in config.patient_ids(n_patients):
        base = {v: rng.uniform(*BASELINE[v]) for v in VITALS}
        state = dict(base)

        # schedule non-overlapping episodes for this patient
        schedule = {}
        t = rng.uniform(5, 25)
        while t + EPISODE_LEN[1] < duration - 3:
            kind = rng.choice(list(EPISODE_TYPES))
            vital, (lo, hi) = EPISODE_TYPES[kind]
            length = rng.randint(*EPISODE_LEN)
            ep_id = f"{pid}-E{len(episodes) + 1:03d}"
            level = rng.uniform(lo, hi)
            start = int(t)
            for s in range(start, start + length):
                schedule[s] = (ep_id, vital, level, (lo, hi))
            episodes.append({"episode_id": ep_id, "patient": pid, "kind": kind, "vital": vital,
                             "start_seq": start, "end_seq": start + length - 1, "length_s": length})
            t = start + length + rng.uniform(*EPISODE_GAP)

        for seq in range(int(duration * config.SAMPLE_HZ)):
            row = {"reading_id": f"{pid}-{seq:05d}", "patient": pid, "seq": seq,
                   "t": round(seq / config.SAMPLE_HZ, 3), "episode": ""}
            for v in VITALS:  # mean-reverting random walk, clipped to the safe band
                state[v] += 0.1 * (base[v] - state[v]) + rng.gauss(0, NOISE[v])
                state[v] = min(max(state[v], SAFE_BAND[v][0]), SAFE_BAND[v][1])
                row[v] = round(state[v], DECIMALS[v]) if DECIMALS[v] else int(round(state[v]))
            if seq in schedule:
                ep_id, vital, level, (lo, hi) = schedule[seq]
                value = min(max(level + rng.gauss(0, NOISE[vital]), lo), hi)
                row[vital] = round(value, DECIMALS[vital]) if DECIMALS[vital] else int(round(value))
                row["episode"] = ep_id
            readings.append(row)

    readings.sort(key=lambda r: (r["t"], r["patient"]))
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.DATASET_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(readings)
    with open(config.EPISODES_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(episodes[0]))
        w.writeheader()
        w.writerows(episodes)
    print(f"wrote {len(readings)} readings for {n_patients} patients ({duration}s, seed={seed}) "
          f"-> {config.DATASET_CSV}")
    print(f"wrote {len(episodes)} anomaly episodes -> {config.EPISODES_CSV}")


def load_dataset(path=config.DATASET_CSV):
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            r["seq"] = int(r["seq"])
            r["t"] = float(r["t"])
            for v in VITALS:
                r[v] = float(r[v]) if DECIMALS[v] else int(float(r[v]))
            rows.append(r)
    return rows


def stats():
    if not config.DATASET_CSV.exists():
        sys.exit("no dataset yet - run: python generator.py --generate")
    rows = load_dataset()
    with open(config.EPISODES_CSV, newline="") as fh:
        eps = list(csv.DictReader(fh))
    patients = sorted({r["patient"] for r in rows})
    print(f"{len(rows)} readings, {len(patients)} patients, {max(r['t'] for r in rows) + 1:.0f}s")
    print(f"{len(eps)} episodes, {sum(1 for r in rows if r['episode'])} anomalous readings")
    by_kind = {}
    for e in eps:
        by_kind[e["kind"]] = by_kind.get(e["kind"], 0) + 1
    for kind, n in sorted(by_kind.items()):
        print(f"  {kind:14s} {n}")
    print("\nvital ranges (min / max):")
    for v in VITALS:
        vals = [r[v] for r in rows]
        print(f"  {v:5s} {min(vals):7.1f} / {max(vals):7.1f}")


# ------------------------------------------------------------------ replay
class Replayer:
    def __init__(self, rc):
        self.rc = rc
        self.arch = rc["arch"]
        self.t0 = rc["t0"]
        self.run_dir = rc["run_dir"]
        self.local_delay = rc.get("local_delay_ms", 0)
        self.sent = CsvLog(f"{self.run_dir}/sent_generator.csv",
                           ["reading_id", "patient", "t_emit", "route", "episode"])
        self.alerts = alert_log(self.run_dir, "sensors")
        self.events = event_log(self.run_dir, "sensors")
        self.cloud = None
        if self.arch in ("A", "B"):
            share = 1 if self.arch == "A" else rc["patients"]  # B: fair share of the site uplink
            uplink = dict(rc["uplink"])
            if uplink.get("bandwidth_kbps"):
                uplink["bandwidth_kbps"] /= share
            netem = NetEm(uplink, self.t0, seed=rc["seed"], base_delay_ms=rc.get("wan_ms", 0))
            self.cloud = CloudClient(rc["cloud_url"], netem, self.run_dir, "sensors")
        self.fog_writer = None
        self.tasks = set()

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def to_cloud(self, reading, path):
        """A, and B's fallback: the cloud detects; an alert exists only once the reply is back."""
        body = {"msg_id": self.cloud.new_msg_id(), "kind": "reading", "source": "sensors",
                "detect": True, "readings": [reading]}
        try:
            resp = await self.cloud.post(body)
        except LinkError:
            return
        t = config.now()
        for a in resp.get("alerts", []):
            self.alerts.write(t_alert=t, component="sensors", path=path, **{
                k: a.get(k) for k in ("alert_id", "patient", "reading_id", "vital", "value",
                                      "direction", "detector")})

    async def to_fog(self, reading):
        """C fallback: bedside edge is down, the ward fog server takes over detection."""
        try:
            if self.fog_writer is None or self.fog_writer.is_closing():
                _, self.fog_writer = await transport.connect(config.HOST, config.FOG_PORT)
            await transport.send(self.fog_writer, {"type": "reading", "fallback": True,
                                                   "reading": reading}, self.local_delay)
            return True
        except (OSError, asyncio.TimeoutError):
            self.fog_writer = None
            return False

    async def patient_stream(self, pid, rows):
        writer, alive, last_try = None, False, -1e9

        async def watch(reader):  # EOF from the edge means its process died
            nonlocal alive
            async for _ in transport.messages(reader):
                pass
            alive = False
            self.events.write(t=config.now(), component="sensors", event="edge-down", detail=pid)

        for row in rows:
            delay = self.t0 + row["t"] - config.now()
            if delay > 0:
                await asyncio.sleep(delay)
            reading = {k: row[k] for k in ("reading_id", "patient", "seq", *VITALS)}
            t_emit = config.now()
            reading["t_emit"] = t_emit

            if self.arch == "A":
                self._spawn(self.to_cloud(reading, "cloud"))
                route = "cloud"
            else:
                if not alive and t_emit - last_try >= 1.0:   # (re)connect to the bedside edge
                    last_try = t_emit
                    try:
                        reader, writer = await transport.connect(config.HOST, config.edge_port(pid), 0.2)
                        alive = True
                        asyncio.create_task(watch(reader))
                        self.events.write(t=config.now(), component="sensors", event="edge-up", detail=pid)
                    except (OSError, asyncio.TimeoutError):
                        alive = False
                route = "edge"
                if alive:
                    try:
                        await transport.send(writer, {"type": "reading", "reading": reading}, self.local_delay)
                    except (OSError, ConnectionError):
                        alive = False
                if not alive:
                    if self.arch == "B":
                        self._spawn(self.to_cloud(reading, "cloud-fallback"))
                        route = "cloud-fallback"
                    else:
                        route = "fog-fallback" if await self.to_fog(reading) else "dropped"
            self.sent.write(reading_id=row["reading_id"], patient=pid, t_emit=t_emit,
                            route=route, episode=row["episode"])
        if writer is not None:
            writer.close()

    async def run(self):
        rows = [r for r in load_dataset() if r["t"] < self.rc["duration"]
                and r["patient"] in self.rc["patient_ids"]]
        by_patient = {}
        for r in rows:
            by_patient.setdefault(r["patient"], []).append(r)
        self.events.write(t=config.now(), component="sensors", event="replay-start",
                          detail=f"{len(rows)} readings")
        await asyncio.gather(*(self.patient_stream(pid, rs) for pid, rs in by_patient.items()))
        if self.tasks:  # let in-flight cloud requests finish (bounded by the HTTP timeout)
            await asyncio.wait(list(self.tasks), timeout=config.HTTP_TIMEOUT_S + 2)
        self.events.write(t=config.now(), component="sensors", event="replay-end", detail="")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate", action="store_true", help="create and save the synthetic dataset")
    mode.add_argument("--stats", action="store_true", help="summarise the saved dataset")
    mode.add_argument("--replay", action="store_true", help="stream the dataset (used by simulation.py)")
    ap.add_argument("--patients", type=int, default=config.DEFAULT_PATIENTS)
    ap.add_argument("--duration", type=int, default=config.DEFAULT_DATASET_SECONDS, help="seconds of data")
    ap.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    ap.add_argument("--run-config", help="run_config.json written by simulation.py (--replay)")
    args = ap.parse_args()

    if args.generate:
        generate(args.patients, args.duration, args.seed)
    elif args.stats:
        stats()
    else:
        if not args.run_config:
            ap.error("--replay needs --run-config")
        asyncio.run(Replayer(config.load_run_config(args.run_config)).run())


if __name__ == "__main__":
    main()
