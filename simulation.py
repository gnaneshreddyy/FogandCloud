"""Run the experiments: architecture x scenario x repetitions.

Each layer runs as its own OS process on this PC (sensors, one edge per
patient, fog); the cloud is real AWS (--cloud aws) or the local mock.

  python simulation.py --list                                   # architectures + scenarios
  python simulation.py --arch A --scenario baseline             # one run
  python simulation.py --arch C --scenario cloud-failure
  python simulation.py --arch B --scenario edge-failure --cloud aws
  python simulation.py --arch all --scenario latency --reps 3   # A, B and C
  python simulation.py --all --reps 3                           # full matrix
  python simulation.py --quick                                  # 20 s smoke test of A/B/C baseline

Scenarios: baseline | latency | congestion | cloud-failure | edge-failure
Results:   results/runs/<run>/  (raw CSV logs + summary.json)
           results/summary.csv  (one row per run, read by analyze.py)
"""
import argparse
import asyncio
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime

from common import config, transport

PY = sys.executable
STARTUP_BUDGET_S = 6.0


class Run:
    def __init__(self, arch, scenario, rep, args):
        self.arch, self.scenario, self.rep, self.args = arch, scenario, rep, args
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        tag = f"_{args.tag}" if args.tag else ""
        self.dir = config.RESULTS_DIR / "runs" / f"{stamp}_{arch}_{scenario}_r{rep}{tag}"
        (self.dir / "logs").mkdir(parents=True, exist_ok=True)
        self.procs = {}
        self.patients = config.patient_ids(args.patients)

    def spawn(self, name, *argv):
        log = open(self.dir / "logs" / f"{name}.log", "a")
        self.procs[name] = subprocess.Popen([PY, *argv], cwd=config.ROOT, stdout=log,
                                            stderr=subprocess.STDOUT)
        return self.procs[name]

    def cloud_url(self):
        if self.args.cloud == "aws":
            url = config.load_aws_endpoint()
            if not url:
                sys.exit("no AWS endpoint - run: python aws_setup.py --deploy  (or use --cloud local)")
            return url
        return f"http://{config.HOST}:{self.args.cloud_port}/ingest"

    def write_config(self):
        sc = config.SCENARIOS[self.scenario]
        self.rc = {
            "arch": self.arch, "scenario": self.scenario, "rep": self.rep,
            "run_dir": str(self.dir), "t0": config.now() + STARTUP_BUDGET_S,
            "duration": self.args.duration, "patients": self.args.patients,
            "patient_ids": self.patients, "seed": config.DEFAULT_SEED + self.rep,
            "cloud": self.args.cloud, "cloud_url": self.cloud_url(),
            "wan_ms": self.args.wan_ms if self.args.cloud == "local" else 0.0,
            "local_delay_ms": self.args.local_delay_ms,
            "uplink": sc["uplink"], "edge_failure": sc.get("edge_failure"),
        }
        self.rc_path = self.dir / "run_config.json"
        self.rc_path.write_text(json.dumps(self.rc, indent=2))

    def start_cloud(self):
        if self.args.cloud != "local":
            return
        self.spawn("cloud", "cloud.py", "--serve", "--port", str(self.args.cloud_port),
                   "--db", str(self.dir / "cloud.db"))
        for _ in range(50):
            try:
                urllib.request.urlopen(f"http://{config.HOST}:{self.args.cloud_port}/", timeout=0.5)
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("local cloud did not start (port busy?)")

    def start_edge(self, pid):
        return self.spawn(f"edge-{pid}", "edge.py", "--patient", pid, "--run-config", str(self.rc_path))

    async def wait_port(self, port, deadline):
        while config.now() < deadline:
            try:
                _, w = await transport.connect(config.HOST, port, 0.3)
                w.close()
                return True
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.1)
        return False

    async def edge_failure(self, spec):
        t0 = self.rc["t0"]
        await asyncio.sleep(max(0, t0 + spec["kill_at"] - config.now()))
        for pid in spec["patients"]:
            self.procs[f"edge-{pid}"].kill()            # hard crash, no graceful flush
            print(f"    t={spec['kill_at']}s killed edge-{pid}")
        await asyncio.sleep(max(0, t0 + spec["restart_at"] - config.now()))
        for pid in spec["patients"]:
            self.start_edge(pid)
            print(f"    t={spec['restart_at']}s restarted edge-{pid}")

    async def execute(self):
        self.write_config()
        self.start_cloud()
        deadline = self.rc["t0"] - 0.5
        ports = []
        if self.arch == "C":
            self.spawn("fog", "fog.py", "--run-config", str(self.rc_path))
            ports.append(config.FOG_PORT)
        if self.arch in ("B", "C"):
            for pid in self.patients:
                self.start_edge(pid)
                ports.append(config.edge_port(pid))
        ready = await asyncio.gather(*(self.wait_port(p, deadline) for p in ports))
        if not all(ready):
            raise RuntimeError(f"processes not ready in {STARTUP_BUDGET_S}s; see {self.dir / 'logs'}")

        gen = self.spawn("sensors", "generator.py", "--replay", "--run-config", str(self.rc_path))
        failure = None
        if self.rc["edge_failure"] and self.arch in ("B", "C"):
            failure = asyncio.create_task(self.edge_failure(self.rc["edge_failure"]))
        while gen.poll() is None:
            await asyncio.sleep(0.2)
        if failure:
            await failure

        # graceful stop: edges first (their last summaries go to the fog), then the fog
        for pid in self.patients if self.arch in ("B", "C") else []:
            await transport.request_shutdown(config.HOST, config.edge_port(pid))
        if self.arch == "C":
            await asyncio.sleep(0.5)
            await transport.request_shutdown(config.HOST, config.FOG_PORT)
        end = time.time() + config.DRAIN_GRACE_S + 3
        for name, p in self.procs.items():
            if name == "cloud":
                continue
            try:
                p.wait(timeout=max(0.1, end - time.time()))
            except subprocess.TimeoutExpired:
                p.kill()

    def cleanup(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait()


def run_one(arch, scenario, rep, args):
    import metrics
    run = Run(arch, scenario, rep, args)
    print(f"\n>>> arch {arch} | {scenario} | rep {rep} | {args.duration}s | cloud={args.cloud}")
    print(f"    {config.ARCHITECTURES[arch]}")
    print(f"    {config.SCENARIOS[scenario]['description']}")
    try:
        asyncio.run(run.execute())
    finally:
        run.cleanup()
    summary = metrics.compute(run.dir)
    metrics.save(summary, run.dir)
    metrics.print_summary(summary)
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arch", default="all", help="A, B, C, or all (comma list ok: A,C)")
    ap.add_argument("--scenario", default="baseline",
                    help=f"{' | '.join(config.SCENARIOS)} | all (comma list ok)")
    ap.add_argument("--all", action="store_true", help="every architecture x every scenario")
    ap.add_argument("--reps", type=int, default=1, help="repetitions per combination")
    ap.add_argument("--duration", type=int, default=120, help="seconds of data replayed per run")
    ap.add_argument("--patients", type=int, default=config.DEFAULT_PATIENTS)
    ap.add_argument("--cloud", choices=["local", "aws"], default="local",
                    help="local mock (dev) or the deployed AWS endpoint (final results)")
    ap.add_argument("--cloud-port", type=int, default=config.LOCAL_CLOUD_PORT)
    ap.add_argument("--wan-ms", type=float, default=config.LOCAL_CLOUD_WAN_MS,
                    help="one-way WAN delay emulated in front of the local mock cloud")
    ap.add_argument("--local-delay-ms", type=float, default=0.0,
                    help="emulated LAN delay on sensor->edge->fog hops (default 0)")
    ap.add_argument("--tag", default="", help="suffix for run directory names")
    ap.add_argument("--quick", action="store_true", help="20 s baseline of A, B and C")
    ap.add_argument("--list", action="store_true", help="show architectures and scenarios")
    args = ap.parse_args()

    if args.list:
        print("architectures:")
        for k, v in config.ARCHITECTURES.items():
            print(f"  {k}  {v}")
        print("\nscenarios:")
        for k, v in config.SCENARIOS.items():
            print(f"  {k:14s} {v['description']}")
        return

    if args.quick:
        args.arch, args.scenario, args.duration = "all", "baseline", 20
    if args.all:
        args.arch, args.scenario = "all", "all"
    archs = list(config.ARCHITECTURES) if args.arch == "all" else args.arch.upper().split(",")
    scenarios = list(config.SCENARIOS) if args.scenario == "all" else args.scenario.split(",")
    for a in archs:
        if a not in config.ARCHITECTURES:
            ap.error(f"unknown architecture {a!r}")
    for s in scenarios:
        if s not in config.SCENARIOS:
            ap.error(f"unknown scenario {s!r}; choose from {', '.join(config.SCENARIOS)}")

    import generator
    if not config.DATASET_CSV.exists():
        generator.generate(max(args.patients, config.DEFAULT_PATIENTS),
                           max(args.duration, config.DEFAULT_DATASET_SECONDS), config.DEFAULT_SEED)
    rows = generator.load_dataset()
    have_p = len({r["patient"] for r in rows})
    have_s = max(r["t"] for r in rows) + 1
    if args.patients > have_p or args.duration > have_s:
        sys.exit(f"dataset has {have_p} patients / {have_s:.0f}s; regenerate with: "
                 f"python generator.py --generate --patients {args.patients} --duration {args.duration}")
    if args.cloud == "local":
        print(f"note: using the LOCAL mock cloud with {args.wan_ms} ms emulated WAN; "
              f"use --cloud aws for reportable results")

    total = len(archs) * len(scenarios) * args.reps
    done = 0
    for rep in range(1, args.reps + 1):
        for scenario in scenarios:
            for arch in archs:
                done += 1
                print(f"\n[{done}/{total}]", end="")
                run_one(arch, scenario, rep, args)
    print(f"\nall runs done -> {config.RESULTS_DIR / 'summary.csv'}  (next: python analyze.py --report)")


if __name__ == "__main__":
    main()
