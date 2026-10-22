"""Central configuration."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

HISTORY_CSV = str(DATA / "history" / "history.csv.gz")   # 1 day of "normal" data for the baseline
STREAM_IN = str(DATA / "stream_in")                       # landing folder watched by Spark
GROUND_TRUTH = str(DATA / "ground_truth.json")

OUT = DATA / "processed"
BASELINE = str(OUT / "baseline")
WINDOW_METRICS = str(OUT / "window_metrics")
ALERTS = str(OUT / "alerts")
QUARANTINE = str(OUT / "quarantine")
CHECKPOINTS = str(DATA / "checkpoints")

METRICS = ["temperature", "vibration", "pressure"]

WINDOW = "1 minute"          # tumbling window length
WATERMARK = "2 minutes"      # how late an event may arrive and still be counted
Z_THRESHOLD = 4.0            # |z| above this => anomaly
CRITICAL_Z = 8.0             # |z| above this => critical
MIN_EVENTS = 3               # windows with fewer events are "low confidence"
