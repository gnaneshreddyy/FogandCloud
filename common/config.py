"""Central configuration: paths, ports, timing, and the scenario catalogue.

Everything that must be identical across architectures A/B/C (thresholds,
dataset, scenario timings) lives here so the comparison stays fair.
"""
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RESULTS_DIR = ROOT / "results"
DATASET_CSV = DATA_DIR / "dataset.csv"
EPISODES_CSV = DATA_DIR / "episodes.csv"
AWS_CONFIG = ROOT / "aws_config.json"   # written by aws_setup.py --deploy

# ---- local ports (all processes run on this PC) -------------------------
HOST = "127.0.0.1"
FOG_PORT = 9000
EDGE_BASE_PORT = 9100          # edge for patient i listens on EDGE_BASE_PORT + i
LOCAL_CLOUD_PORT = 8080

# ---- dataset defaults ---------------------------------------------------
DEFAULT_SEED = 42
DEFAULT_PATIENTS = 8
DEFAULT_DATASET_SECONDS = 300
SAMPLE_HZ = 1.0                # one reading per patient per second

# ---- behaviour knobs ----------------------------------------------------
SUMMARY_WINDOW_S = 5.0         # edge summarises raw readings every N seconds
FOG_FLUSH_S = 2.0              # fog uploads one batch every N seconds
FOG_ALERT_FLUSH_S = 0.25       # ...but alerts leave the fog within this time
FOG_DEDUP_COOLDOWN_S = 10.0    # same patient+vital alert suppressed for N s
WARD_ALERT_MIN_PATIENTS = 3    # fog raises a ward alert if >= N patients alarm together
WARD_ALERT_WINDOW_S = 10.0
EDGE_BUFFER_MAX = 200          # edge (arch B) keeps at most N unsent messages in RAM
RETRY_INTERVAL_S = 1.0         # buffered uploaders retry this often
HTTP_TIMEOUT_S = 5.0
HTTP_HEADER_OVERHEAD = 200     # bytes of HTTP request headers counted per request
LOCAL_CLOUD_WAN_MS = 40.0      # one-way WAN delay added when using the local mock cloud
DRAIN_GRACE_S = 8.0            # time allowed after replay ends for buffers to drain

ARCHITECTURES = {
    "A": "Cloud-only: sensors -> AWS (cloud detects)",
    "B": "Edge-Cloud: sensors -> edge (detects) -> AWS",
    "C": "Edge-Fog-Cloud: sensors -> edge (detects) -> fog (dedupe/batch/buffer) -> AWS",
}

# ---- scenarios ----------------------------------------------------------
# Times are seconds from the start of the replay. "uplink" is the emulated
# network between this PC and the cloud; "local" is edge<->fog<->sensor.
SCENARIOS = {
    "baseline": {
        "description": "No injected impairment",
        "uplink": {},
    },
    "latency": {
        "description": "High WAN latency: +300 ms one-way, 100 ms jitter",
        "uplink": {"delay_ms": 300, "jitter_ms": 100},
    },
    "congestion": {
        "description": "Congested uplink: 32 kbit/s cap, 3% loss, +80 ms",
        "uplink": {"bandwidth_kbps": 32, "loss": 0.03, "delay_ms": 80, "jitter_ms": 30},
    },
    "cloud-failure": {
        "description": "Cloud unreachable from t=30 s to t=60 s",
        "uplink": {"outages": [[30, 60]]},
    },
    "edge-failure": {
        "description": "Edges of P01,P02 killed at t=30 s, restarted at t=60 s",
        "uplink": {},
        "edge_failure": {"patients": ["P01", "P02"], "kill_at": 30, "restart_at": 60},
    },
}


def now() -> float:
    """Shared clock for every process on this PC.

    perf_counter is system-wide (QPC on Windows, CLOCK_MONOTONIC on Linux),
    so timestamps from different processes are directly comparable, with
    far better resolution than time.time() on Windows.
    """
    return time.perf_counter()


def patient_ids(n: int) -> list:
    return [f"P{i:02d}" for i in range(1, n + 1)]


def edge_port(patient_id: str) -> int:
    return EDGE_BASE_PORT + int(patient_id[1:])


def load_aws_endpoint():
    if AWS_CONFIG.exists():
        return json.loads(AWS_CONFIG.read_text()).get("function_url")
    return os.environ.get("ICU_CLOUD_URL")


def load_run_config(path) -> dict:
    return json.loads(Path(path).read_text())
