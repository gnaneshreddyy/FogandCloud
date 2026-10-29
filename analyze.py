"""Aggregate results across runs and draw the comparison plots.

  python analyze.py --table                  # mean (std) per architecture x scenario
  python analyze.py --plots                  # results/plots/*.png
  python analyze.py --report                 # both, plus results/comparison.csv
  python analyze.py --run results/runs/<run> # timeline plot of a single run
  python analyze.py --rebuild                # recompute summary.csv from every run dir
  python analyze.py --report --cloud aws     # only runs that used the real AWS endpoint
"""
import argparse
import glob
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from common import config  # noqa: E402

PLOTS = config.RESULTS_DIR / "plots"
SUMMARY = config.RESULTS_DIR / "summary.csv"
ARCH_COLORS = {"A": "#d1495b", "B": "#edae49", "C": "#00798c"}
SCENARIO_ORDER = list(config.SCENARIOS)
METRICS = [  # column, label, log-scale
    ("latency_median_ms", "Median alert latency (ms)", True),
    ("latency_p95_ms", "p95 alert latency (ms)", True),
    ("detection_rate", "Episodes detected (share)", False),
    ("cloud_bytes_per_s", "Cloud-bound traffic (bytes/s)", False),
    ("pdr", "Packet delivery ratio (app level)", False),
    ("recovery_s", "Backlog recovery after outage (s)", False),
]


def load(cloud=None):
    if not SUMMARY.exists():
        sys.exit("no results yet - run simulation.py first")
    df = pd.read_csv(SUMMARY)
    if cloud:
        df = df[df["cloud"] == cloud]
    if df.empty:
        sys.exit("no matching runs")
    df["scenario"] = pd.Categorical(df["scenario"], SCENARIO_ORDER, ordered=True)
    return df


def table(df):
    cols = [m for m, _, _ in METRICS] + ["false_alerts", "cloud_requests"]
    g = df.groupby(["scenario", "arch"], observed=True)[cols]
    mean, std, n = g.mean(), g.std(), g.size()
    out = mean.round(2).astype(str)
    for c in cols:
        out[c] = [f"{m:.2f}" + (f" ({s:.2f})" if pd.notna(s) else "") if pd.notna(m) else "-"
                  for m, s in zip(mean[c], std[c])]
    out["runs"] = n
    return out


def plots(df):
    PLOTS.mkdir(parents=True, exist_ok=True)
    scenarios = [s for s in SCENARIO_ORDER if s in set(df["scenario"].astype(str))]
    archs = sorted(df["arch"].unique())
    width = 0.8 / len(archs)
    for col, label, log in METRICS:
        sub = df.dropna(subset=[col])
        if sub.empty:
            continue
        fig, ax = plt.subplots(figsize=(9, 4.5))
        for i, arch in enumerate(archs):
            g = sub[sub["arch"] == arch].groupby("scenario", observed=True)[col]
            means, stds = g.mean(), g.std().fillna(0)
            xs = [scenarios.index(s) + (i - (len(archs) - 1) / 2) * width
                  for s in means.index.astype(str) if s in scenarios]
            ax.bar(xs, means.values, width, yerr=stds.values, capsize=3,
                   color=ARCH_COLORS.get(arch), label=f"{arch}: {config.ARCHITECTURES[arch].split(':')[0]}")
        ax.set_xticks(range(len(scenarios)), scenarios)
        ax.set_ylabel(label)
        if log:
            ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(frameon=False, fontsize=8)
        ax.set_title(label)
        fig.tight_layout()
        path = PLOTS / f"{col}.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"wrote {path}")


def run_timeline(run_dir):
    run_dir = Path(run_dir)
    rc = json.loads((run_dir / "run_config.json").read_text())
    t0 = rc["t0"]
    sent = pd.read_csv(run_dir / "sent_generator.csv").set_index("reading_id")
    alerts = pd.concat([pd.read_csv(p) for p in glob.glob(str(run_dir / "alerts_*.csv"))], ignore_index=True)
    alerts = alerts[alerts["patient"] != "WARD"]
    alerts = alerts.sort_values("t_alert").drop_duplicates(["reading_id", "vital"])
    alerts["t_emit"] = alerts["reading_id"].map(sent["t_emit"])
    alerts["lat_ms"] = (alerts["t_alert"] - alerts["t_emit"]) * 1000

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    for path, g in alerts.groupby("path"):
        ax1.scatter(g["t_emit"] - t0, g["lat_ms"].clip(lower=0.01), s=12, label=path)
    ax1.set_yscale("log")
    ax1.set_ylabel("alert latency (ms)")
    ax1.legend(frameon=False, fontsize=8)
    for p in glob.glob(str(run_dir / "buffer_*.csv")):
        b = pd.read_csv(p)
        ax2.step(b["t"] - t0, b["size"], where="post", label=Path(p).stem.replace("buffer_", ""))
    ax2.set_ylabel("uplink buffer (msgs)")
    ax2.set_xlabel("time since replay start (s)")
    if ax2.get_legend_handles_labels()[0]:
        ax2.legend(frameon=False, fontsize=7, ncol=4)
    for ax in (ax1, ax2):
        for s, e in rc["uplink"].get("outages", []):
            ax.axvspan(s, e, color="grey", alpha=0.15)
        if rc.get("edge_failure"):
            ef = rc["edge_failure"]
            ax.axvspan(ef["kill_at"], ef["restart_at"], color="orange", alpha=0.12)
        ax.grid(alpha=0.3)
    fig.suptitle(f"{run_dir.name}: arch {rc['arch']}, {rc['scenario']}")
    fig.tight_layout()
    out = run_dir / "timeline.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def rebuild():
    import metrics
    SUMMARY.unlink(missing_ok=True)
    runs = sorted(p for p in (config.RESULTS_DIR / "runs").glob("*") if (p / "run_config.json").exists())
    for run in runs:
        try:
            metrics.save(metrics.compute(run), run)
        except Exception as exc:
            print(f"skipped {run.name}: {exc}")
    print(f"rebuilt {SUMMARY} from {len(runs)} runs")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--table", action="store_true")
    ap.add_argument("--plots", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--run", help="single run directory -> timeline.png")
    ap.add_argument("--rebuild", action="store_true")
    ap.add_argument("--cloud", choices=["local", "aws"], help="filter runs by cloud backend")
    args = ap.parse_args()
    if not any([args.table, args.plots, args.report, args.run, args.rebuild]):
        ap.print_help()
        return
    if args.rebuild:
        rebuild()
    if args.run:
        run_timeline(args.run)
    if args.table or args.plots or args.report:
        df = load(args.cloud)
        if args.table or args.report:
            t = table(df)
            with pd.option_context("display.width", 200, "display.max_columns", 20):
                print(t)
            if args.report:
                t.to_csv(config.RESULTS_DIR / "comparison.csv")
                print(f"wrote {config.RESULTS_DIR / 'comparison.csv'}")
        if args.plots or args.report:
            plots(df)


if __name__ == "__main__":
    main()
