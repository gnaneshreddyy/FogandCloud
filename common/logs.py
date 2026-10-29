"""Append-only CSV logs, one file per process.

Each row is flushed immediately so a hard-killed process (edge-failure
scenario) still leaves a complete log behind.
"""
import csv
from pathlib import Path


class CsvLog:
    def __init__(self, path, fields):
        self.path = Path(path)
        self.fields = list(fields)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new = not self.path.exists() or self.path.stat().st_size == 0
        self._fh = open(self.path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.fields, extrasaction="ignore")
        if new:
            self._writer.writeheader()
            self._fh.flush()

    def write(self, **row):
        self._writer.writerow(row)
        self._fh.flush()

    def close(self):
        self._fh.close()


ALERT_FIELDS = ["t_alert", "component", "path", "alert_id", "patient", "reading_id",
                "vital", "value", "direction", "detector"]


def alert_log(run_dir, component):
    """Every alert raised *at the point of care* is logged here (latency metric)."""
    return CsvLog(Path(run_dir) / f"alerts_{component}.csv", ALERT_FIELDS)


def event_log(run_dir, component):
    return CsvLog(Path(run_dir) / f"events_{component}.csv", ["t", "component", "event", "detail"])
