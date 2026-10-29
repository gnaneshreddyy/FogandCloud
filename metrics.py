"""Compute the evaluation metrics for one finished run directory.

    python metrics.py results/runs/<run>             # print summary
    python metrics.py results/runs/<run> --save      # also (re)write summary.json

Metrics (all from the per-process CSV logs written during the run):
    alert latency      t_alert - t_emit of the reading that triggered it, using the
                       earliest point-of-care alert per (reading, vital)
    detection rate     share of ground-truth episodes that raised >= 1 alert
    time to detect     first alert of an episode - emission of its first reading
    false alerts       alerts on readings outside any episode
    cloud bytes        every byte sent toward the cloud, retries and HTTP headers included
    PDR                application-level: cloud messages acknowledged / messages created
    recovery time      outage end -> last message buffered before outage end acknowledged
"""
import argparse
import csv
import glob
import json
import statistics
from pathlib import Path

from common import config


def _read(pattern):
    rows = []
    for path in sorted(glob.glob(str(pattern))):
        with open(path, newline="", encoding="utf-8") as fh:
            rows.extend(csv.DictReader(fh))
    return rows


def _pct(values, q):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def _ms(x):
    return None if x is None else round(x * 1000, 2)


def compute(run_dir) -> dict:
    run_dir = Path(run_dir)
    rc = json.loads((run_dir / "run_config.json").read_text())
    t0 = rc["t0"]

    sent = {r["reading_id"]: r for r in _read(run_dir / "sent_generator.csv")}
    emit = {rid: float(r["t_emit"]) for rid, r in sent.items()}
    with open(config.EPISODES_CSV, newline="") as fh:
        episodes = [e for e in csv.DictReader(fh)
                    if e["patient"] in rc["patient_ids"] and int(e["start_seq"]) < rc["duration"]]
    ep_of = {rid: r["episode"] for rid, r in sent.items() if r["episode"]}
    ep_vital = {e["episode_id"]: e["vital"] for e in episodes}

    # earliest point-of-care alert per (reading, vital)
    first = {}
    for a in _read(run_dir / "alerts_*.csv"):
        if a["patient"] == "WARD" or a["reading_id"] not in emit:
            continue
        key = (a["reading_id"], a["vital"])
        t = float(a["t_alert"])
        if key not in first or t < first[key][0]:
            first[key] = (t, a["path"])
    latencies = [t - emit[rid] for (rid, _), (t, _) in first.items()]
    by_path = {}
    for (rid, _), (t, path) in first.items():
        by_path.setdefault(path, []).append(t - emit[rid])

    true_alerts = {k: v for k, v in first.items()
                   if ep_of.get(k[0]) and ep_vital.get(ep_of[k[0]]) == k[1]}
    false_alerts = len(first) - len(true_alerts)
    detected, ttd = set(), []
    for e in episodes:
        eid = e["episode_id"]
        ep_reads = [rid for rid, ep in ep_of.items() if ep == eid]
        hits = [first[(rid, e["vital"])][0] for rid in ep_reads if (rid, e["vital"]) in first]
        if hits:
            detected.add(eid)
            ttd.append(min(hits) - min(emit[rid] for rid in ep_reads))
    reading_recall = sum(1 for rid, ep in ep_of.items() if (rid, ep_vital.get(ep)) in first) / max(1, len(ep_of))

    # cloud uplink
    up = _read(run_dir / "uplink_*.csv")
    msgs = {}
    for r in up:
        m = msgs.setdefault(r["msg_id"], {"ok": False, "t_enqueue": float(r["t_enqueue"]),
                                          "t_ack": None, "kind": r["kind"],
                                          "n_alerts": int(r["n_alerts"])})
        if r["ok"] == "1" and not m["ok"]:
            m["ok"], m["t_ack"] = True, float(r["t_done"])
    dropped = 0
    for path in glob.glob(str(run_dir / "buffer_*.csv")):
        rows = _read(path)
        if rows:
            dropped += int(rows[-1]["dropped"])
    unsent = 0
    for e in _read(run_dir / "events_*.csv"):
        if e["event"] == "exit":
            for part in e["detail"].split():
                if part.startswith("unsent="):
                    unsent += int(part.split("=")[1])
    created = len(msgs) + dropped + unsent
    delivered = sum(1 for m in msgs.values() if m["ok"])
    cloud_bytes = sum(int(r["bytes"]) for r in up)
    ok_rtts = [float(r["rtt_ms"]) for r in up if r["ok"] == "1"]

    recovery = None
    outages = rc["uplink"].get("outages", [])
    if outages:
        end = t0 + outages[-1][1]
        start = t0 + outages[-1][0]
        backlog = [m for m in msgs.values() if start <= m["t_enqueue"] < end]
        acked = [m["t_ack"] for m in backlog if m["ok"]]
        if backlog:
            recovery = round(max(acked) - end, 3) if acked else None
        else:
            recovery = 0.0

    routes = {}
    for r in sent.values():
        routes[r["route"]] = routes.get(r["route"], 0) + 1

    return {
        "run": run_dir.name,
        "arch": rc["arch"],
        "scenario": rc["scenario"],
        "rep": rc.get("rep", 1),
        "cloud": rc["cloud"],
        "duration_s": rc["duration"],
        "patients": rc["patients"],
        "readings_emitted": len(sent),
        "routes": routes,
        "episodes": len(episodes),
        "episodes_detected": len(detected),
        "detection_rate": round(len(detected) / max(1, len(episodes)), 4),
        "anomalous_reading_recall": round(reading_recall, 4),
        "false_alerts": false_alerts,
        "alerts_point_of_care": len(first),
        "latency_median_ms": _ms(_pct(latencies, 0.5)),
        "latency_p95_ms": _ms(_pct(latencies, 0.95)),
        "latency_mean_ms": _ms(statistics.fmean(latencies)) if latencies else None,
        "latency_max_ms": _ms(max(latencies)) if latencies else None,
        "latency_by_path_median_ms": {p: _ms(_pct(v, 0.5)) for p, v in by_path.items()},
        "ttd_median_ms": _ms(_pct(ttd, 0.5)),
        "cloud_requests": len(up),
        "cloud_bytes": cloud_bytes,
        "cloud_bytes_per_s": round(cloud_bytes / rc["duration"], 1),
        "cloud_rtt_median_ms": round(_pct(ok_rtts, 0.5), 2) if ok_rtts else None,
        "messages_created": created,
        "messages_delivered": delivered,
        "pdr": round(delivered / created, 4) if created else None,
        "alerts_delivered_to_cloud": sum(m["n_alerts"] for m in msgs.values() if m["ok"]),
        "buffer_dropped": dropped,
        "unsent_at_exit": unsent,
        "recovery_s": recovery,
    }


FLAT_KEYS = ["run", "arch", "scenario", "rep", "cloud", "duration_s", "patients", "readings_emitted",
             "episodes", "episodes_detected", "detection_rate", "anomalous_reading_recall",
             "false_alerts", "alerts_point_of_care", "latency_median_ms", "latency_p95_ms",
             "latency_mean_ms", "latency_max_ms", "ttd_median_ms", "cloud_requests", "cloud_bytes",
             "cloud_bytes_per_s", "cloud_rtt_median_ms", "messages_created", "messages_delivered",
             "pdr", "alerts_delivered_to_cloud", "buffer_dropped", "unsent_at_exit", "recovery_s"]


def save(summary, run_dir):
    Path(run_dir, "summary.json").write_text(json.dumps(summary, indent=2))
    out = config.RESULTS_DIR / "summary.csv"
    new = not out.exists()
    with open(out, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FLAT_KEYS, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow(summary)


def print_summary(s):
    print(f"\n=== {s['run']}  (arch {s['arch']}, {s['scenario']}, cloud={s['cloud']}) ===")
    print(f"readings emitted     {s['readings_emitted']}  routes={s['routes']}")
    print(f"episodes detected    {s['episodes_detected']}/{s['episodes']}  "
          f"(rate {s['detection_rate']:.0%}, reading recall {s['anomalous_reading_recall']:.0%}, "
          f"false alerts {s['false_alerts']})")
    print(f"alert latency        median {s['latency_median_ms']} ms  p95 {s['latency_p95_ms']} ms  "
          f"max {s['latency_max_ms']} ms")
    print(f"  by path            {s['latency_by_path_median_ms']}")
    print(f"time to detect       median {s['ttd_median_ms']} ms")
    print(f"cloud traffic        {s['cloud_bytes']} B in {s['cloud_requests']} requests "
          f"({s['cloud_bytes_per_s']} B/s), RTT median {s['cloud_rtt_median_ms']} ms")
    print(f"PDR                  {s['messages_delivered']}/{s['messages_created']} = {s['pdr']}  "
          f"(dropped {s['buffer_dropped']}, unsent {s['unsent_at_exit']})")
    if s["recovery_s"] is not None:
        print(f"recovery time        {s['recovery_s']} s after the outage ended")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir")
    ap.add_argument("--save", action="store_true", help="write summary.json and append results/summary.csv")
    args = ap.parse_args()
    s = compute(args.run_dir)
    print_summary(s)
    if args.save:
        save(s, args.run_dir)


if __name__ == "__main__":
    main()
