# ICU Monitoring Testbed: Cloud vs Edge-Cloud vs Edge-Fog-Cloud

This project compares three ways of monitoring ICU patients. It measures alert
latency, cloud traffic, delivery ratio and recovery, under normal conditions and
under failures.

Everything except the cloud runs on one PC. Each layer is its own OS process.
The cloud is real AWS (Lambda + DynamoDB), or a local mock of it while you develop.

> Network conditions are **emulated** in Python (`common/netem.py`). This is not a
> real hospital network, so state that in the report.

---

## 1. How data flows

```
                      ┌──────────────────────── this PC ────────────────────────┐
                      │                                                          │
 data/dataset.csv ──► │ generator.py  (sensors, 1 reading/s per patient)         │
                      │      │                                                   │
   A  Cloud-only      │      └──────────── every reading ───────────────────────►│──► AWS: detects, stores,
                      │                                                          │    returns alerts in reply
                      │                                                          │
   B  Edge-Cloud      │      └─► edge.py  (one process per patient, TCP 91xx)    │
                      │            detects ► LOCAL ALERT                         │
                      │            └── alerts + 5 s summaries ──────────────────►│──► AWS: stores
                      │                                                          │
   C  Edge-Fog-Cloud  │      └─► edge.py ─► fog.py  (one ward server, TCP 9000)  │
                      │       detects ►      dedupe, batch, gzip, buffer         │
                      │       LOCAL ALERT    └── one batched request / 2 s ─────►│──► AWS: stores
                      └──────────────────────────────────────────────────────────┘
                                         ▲
                     common/netem.py sits on every PC → cloud request
                     (delay, jitter, loss, bandwidth cap, outages)
```

All three architectures use the **same dataset, thresholds and scenarios**. The
only thing that changes is the path the data takes.

| Hop | Transport | Port / address |
|---|---|---|
| generator → edge | newline-JSON over TCP | `127.0.0.1:9100 + patient no.` (P01 → 9101) |
| edge → fog | newline-JSON over TCP | `127.0.0.1:9000` |
| generator → fog (fallback in C) | newline-JSON over TCP | `127.0.0.1:9000` |
| anything → cloud | HTTPS POST, JSON (fog: gzip) | see section 2 |

**What happens when something fails**
- **Edge killed in B**: the sensors send that patient's readings straight to the cloud, and the alert arrives only when the cloud replies.
- **Edge killed in C**: the sensors fall back to the fog. The fog runs the same threshold check and raises the alert locally.
- **Cloud unreachable**:
  - A has no buffer, so readings are lost and no alerts are raised.
  - In B, each edge keeps a bounded buffer (200 messages) and retries every 1 s.
  - In C, the fog keeps an unbounded buffer and retries every 1 s.
  - Local alerts in B and C keep firing throughout.

---

## 2. Where the cloud listens

**On AWS (for the final results)**
- `aws_setup.py --deploy` creates:
  - a DynamoDB table `icu-monitoring` (`pk` = patient, `sk` = `<kind>#<id>`)
  - an IAM role `icu-monitoring-lambda-role`
  - a Lambda function `icu-monitoring` (Python 3.12, handler `lambda_function.lambda_handler`)
  - a **Lambda Function URL**: `https://<id>.lambda-url.<region>.on.aws/`
- The Lambda listens on **`POST /`** at that URL. A `GET` returns a health check.
- The URL is saved to `aws_config.json`. `simulation.py --cloud aws` reads it from there, or from the `ICU_CLOUD_URL` environment variable.
- The default region is `ap-south-1` (Mumbai). Use `--region` to change it.
- The URL uses auth `NONE`, so run `--teardown` when you have finished.

**Local mock (for development)**
- `cloud.py --serve` listens on **`http://127.0.0.1:8080`**. The simulator posts to `/ingest`, but any path works.
- It accepts the same API as the Lambda and stores data in SQLite.
- The simulator adds 40 ms of one-way emulated WAN delay in front of it (`--wan-ms`).

**Request body** (the same for both):
```json
{"msg_id": "...", "kind": "reading|alert|summary|batch", "source": "sensors|edge-P01|fog",
 "detect": true, "readings": [...], "alerts": [...], "summaries": [...]}
```
`detect: true` (used by A, and by B's fallback) tells the cloud to run the
threshold check and return any alerts in the reply:
`{"ok": true, "alerts": [...], "stored": {...}}`.

---

## 3. Files and run modes

### `generator.py`: sensor layer
Builds the synthetic vitals dataset (heart rate, SpO₂, temperature, systolic BP, respiratory rate) with labelled anomaly episodes. During a run it replays that dataset in real time.

| Command | What it does |
|---|---|
| `python generator.py --generate` | Writes `data/dataset.csv` and `data/episodes.csv` (8 patients, 300 s, seed 42) |
| `python generator.py --generate --patients 10 --duration 240 --seed 7` | Same, with different settings |
| `python generator.py --stats` | Prints episode counts and vital ranges for the saved dataset |
| `python generator.py --replay --run-config <run>/run_config.json` | Streams readings along the A/B/C path *(started by simulation.py)* |

### `edge.py`: edge layer (one per patient)
- Receives one patient's readings and checks them against the thresholds.
- Raises the alert locally, in under 1 ms.
- Only alerts and 5 s summaries leave the bedside. In B they go to the cloud through a bounded buffer. In C they go to the fog.

| Command | What it does |
|---|---|
| `python edge.py --patient P01 --run-config <run>/run_config.json [--port 9101]` | Runs one bedside edge *(started, and killed in the edge-failure scenario, by simulation.py)* |

### `fog.py`: fog layer (architecture C only)
Runs as one ward server. It:
- dedupes alerts (one cloud alert per patient+vital every 10 s)
- batches and gzips summaries every 2 s, while alerts leave within 0.25 s
- buffers without limit while the cloud is down
- raises a ward alert when 3 or more patients alarm within 10 s
- runs backup detection when an edge dies

| Command | What it does |
|---|---|
| `python fog.py --run-config <run>/run_config.json [--port 9000]` | Runs the fog server *(started by simulation.py)* |

### `cloud.py`: local mock cloud
| Command | What it does |
|---|---|
| `python cloud.py --serve [--port 8080] [--db path.db]` | Runs the mock cloud on `127.0.0.1:8080` |
| `python cloud.py --dump [--db path.db]` | Shows stored row counts and the latest alerts |
| `python cloud.py --reset` | Deletes the default database `results/cloud_local.db` |

During a run, the simulator starts its own mock instance with `--db results/runs/<run>/cloud.db`.

### `lambda_function.py`: AWS cloud layer
This is the Lambda handler. It decodes the request (base64 and gzip as Function URLs deliver it), runs detection when asked, and writes to DynamoDB. You don't run it yourself; `aws_setup.py` zips and deploys it.

### `aws_setup.py`: create and manage the AWS resources
You need `pip install boto3` and configured credentials (`aws configure`).

| Command | What it does |
|---|---|
| `python aws_setup.py --deploy [--region ap-south-1]` | Creates the table, role, Lambda and Function URL, and writes `aws_config.json` |
| `python aws_setup.py --update` | Re-uploads the code after you edit `lambda_function.py` or `common/` |
| `python aws_setup.py --test` | Sends one abnormal reading and prints the reply and round-trip time |
| `python aws_setup.py --ping 20` | Measures 20 round trips to AWS (min / median / max) |
| `python aws_setup.py --status` | Shows what exists and the approximate item count |
| `python aws_setup.py --teardown` | Deletes everything that `--deploy` created |

### `simulation.py`: experiment runner (the main entry point)
For each run it:
1. writes `run_config.json`
2. starts the cloud mock, fog and edges as needed
3. starts the generator
4. injects the scenario
5. shuts everything down gracefully
6. computes the metrics

| Command | What it does |
|---|---|
| `python simulation.py --list` | Lists architectures and scenarios |
| `python simulation.py --quick` | 20 s baseline smoke test of A, B and C |
| `python simulation.py --arch A --scenario baseline` | One run |
| `python simulation.py --arch C --scenario cloud-failure` | Cloud outage from t = 30 s to 60 s |
| `python simulation.py --arch B --scenario edge-failure` | Edges P01 and P02 killed at 30 s, restarted at 60 s |
| `python simulation.py --arch all --scenario latency --reps 3` | A, B and C, 3 repetitions |
| `python simulation.py --arch A,C --scenario congestion,latency` | Comma lists work |
| `python simulation.py --all --reps 3` | Full matrix: 3 architectures × 5 scenarios × 3 reps |
| `... --cloud aws` | Use the deployed AWS endpoint (for reportable results) |

Other options:
- `--duration 120` (seconds replayed)
- `--patients 8`
- `--wan-ms 40` (emulated WAN delay in front of the local mock)
- `--local-delay-ms 0` (emulated LAN delay between sensor, edge and fog)
- `--cloud-port 8080`
- `--tag name` (suffix for the run directory)

**Scenarios** (defined in `common/config.py`):

| Name | Injected condition |
|---|---|
| `baseline` | None |
| `latency` | +300 ms one-way, ±100 ms jitter on the uplink |
| `congestion` | 32 kbit/s uplink cap, 3% loss, +80 ms |
| `cloud-failure` | Cloud unreachable from t = 30 s to 60 s |
| `edge-failure` | Edges of P01 and P02 killed at 30 s, restarted at 60 s |

### `metrics.py`: metrics for one run
| Command | What it does |
|---|---|
| `python metrics.py results/runs/<run>` | Prints the run's metrics |
| `python metrics.py results/runs/<run> --save` | Also writes `summary.json` and appends a row to `results/summary.csv` |

It computes:
- **alert latency**: reading emitted → first point-of-care alert, with median, p95 and max
- **episode detection rate** and **time to detect**
- **false alerts**
- **cloud-bound bytes**: retries and HTTP headers included
- **application-level PDR**
- **backlog recovery time**: from the end of the outage until the backlog is fully acknowledged

### `analyze.py`: compare runs and draw plots
| Command | What it does |
|---|---|
| `python analyze.py --table` | Mean (std) per architecture × scenario |
| `python analyze.py --plots` | Writes `results/plots/*.png` (latency, detection, bytes, PDR, recovery) |
| `python analyze.py --report` | Table + plots + `results/comparison.csv` |
| `python analyze.py --report --cloud aws` | Only runs that used real AWS |
| `python analyze.py --run results/runs/<run>` | Writes `timeline.png` (latency over time + buffer sizes, with outage shading) |
| `python analyze.py --rebuild` | Recomputes `summary.csv` from every run directory |

### `common/`: shared modules
| File | Role |
|---|---|
| `config.py` | Paths, ports, timing knobs, scenario catalogue, shared clock (`perf_counter`, which is system-wide, so timestamps from different processes can be compared) |
| `detection.py` | Threshold rules (HR 40–130, SpO₂ ≥ 90, temp 35–38.5, SBP 90–180, RR 8–30) and 5 s window summaries. Edge, fog and cloud all use this file |
| `cloud_core.py` | Cloud ingest logic shared by `cloud.py` and the Lambda |
| `netem.py` | Uplink emulator: delay, jitter, loss, bandwidth queue, outage windows |
| `uplink.py` | HTTP client that records bytes and RTT for every cloud request, plus the store-and-forward buffer (alerts go first) |
| `transport.py` | Newline-JSON TCP helpers for the local hops, plus graceful shutdown |
| `logs.py` | CSV loggers that flush every row, so logs from killed processes stay complete |

---

## 4. Typical workflow

```bash
pip install -r requirements.txt
python generator.py --generate          # fixed-seed dataset, reused by every run
python simulation.py --quick            # check everything works locally
python aws_setup.py --deploy            # create the AWS cloud layer
python aws_setup.py --test
python simulation.py --all --reps 3 --cloud aws
python analyze.py --report --cloud aws
python aws_setup.py --teardown          # when finished
```

## 5. Output layout

```
data/dataset.csv, data/episodes.csv     the dataset and its ground truth
results/runs/<time>_<arch>_<scenario>_r<rep>/
    run_config.json        exact settings for this run
    sent_generator.csv     every reading and the route it took
    alerts_*.csv           every point-of-care alert (latency source)
    uplink_*.csv           every cloud request: bytes, ok, rtt
    buffer_*.csv           uplink buffer size over time
    events_*.csv           start/exit, edge up/down
    logs/*.log             process stdout
    summary.json           metrics for this run
results/summary.csv        one row per run (input to analyze.py)
results/plots/*.png        comparison charts
```

## 6. Caveats to state in the report
- Edge, fog and sensors share one PC, so local hops are close to 0 ms. Use `--local-delay-ms` to emulate a ward LAN.
- With bandwidth capped, B's edges each get a fair 1/N share of the site uplink, because they are separate processes and can't share one emulated queue.
- The fog may not beat B on latency, because it adds a hop. Its gains show up as fewer cloud bytes and requests, and in coordination and backup detection.
- PDR is measured at the application level (messages created vs acknowledged), not at the packet level.
- On Windows, `asyncio.sleep` has about 15 ms granularity, which inflates emulated delays slightly.
