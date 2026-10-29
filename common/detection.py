"""Threshold anomaly detection and window summaries.

The exact same rules run at the edge (B, C), the fog (C fallback) and the
cloud (A), so only the *location* of detection differs between architectures.
Pure stdlib: this module is shipped inside the Lambda zip.
"""

VITALS = ("hr", "spo2", "temp", "sbp", "rr")

# vital: (low, high); None means no bound on that side
THRESHOLDS = {
    "hr": (40, 130),       # beats/min
    "spo2": (90, None),    # %
    "temp": (35.0, 38.5),  # deg C
    "sbp": (90, 180),      # systolic mmHg
    "rr": (8, 30),         # breaths/min
}


def check(reading: dict, detector: str) -> list:
    """Return one alert dict per vital outside its threshold."""
    alerts = []
    for vital, (low, high) in THRESHOLDS.items():
        value = reading.get(vital)
        if value is None:
            continue
        if low is not None and value < low:
            direction, limit = "low", low
        elif high is not None and value > high:
            direction, limit = "high", high
        else:
            continue
        alerts.append({
            "alert_id": f"{reading['reading_id']}:{vital}",
            "patient": reading["patient"],
            "reading_id": reading["reading_id"],
            "vital": vital,
            "value": value,
            "limit": limit,
            "direction": direction,
            "detector": detector,
        })
    return alerts


class WindowSummarizer:
    """Accumulates raw readings and emits min/mean/max per vital per window."""

    def __init__(self, patient: str, window_s: float):
        self.patient = patient
        self.window_s = window_s
        self._rows = []
        self._window_start = None

    def add(self, reading: dict, t: float):
        if self._window_start is None:
            self._window_start = t
        self._rows.append(reading)

    def due(self, t: float) -> bool:
        return self._window_start is not None and t - self._window_start >= self.window_s

    def flush(self):
        if not self._rows:
            return None
        summary = {
            "patient": self.patient,
            "n": len(self._rows),
            "first_reading": self._rows[0]["reading_id"],
            "last_reading": self._rows[-1]["reading_id"],
        }
        for vital in VITALS:
            values = [r[vital] for r in self._rows if r.get(vital) is not None]
            if values:
                summary[vital] = {
                    "min": round(min(values), 2),
                    "mean": round(sum(values) / len(values), 2),
                    "max": round(max(values), 2),
                }
        self._rows = []
        self._window_start = None
        return summary
