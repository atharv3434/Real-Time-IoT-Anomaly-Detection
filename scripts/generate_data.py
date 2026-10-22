"""Simulate 20 sensors x 3 metrics.

Creates
  data/history/history.csv.gz   1 day of normal readings  (-> baseline)
  data/stream_in/events_NNN.json  60 minutes of live readings in 30 files of 2 minutes
  data/ground_truth.json        which (sensor, window, metric) are truly anomalous

Injected stream problems: 3 real anomalies, duplicates, corrupt (truncated) JSON lines,
sensor dropouts (null metric) and late-arriving events.

Usage: python scripts/generate_data.py [--drip SECONDS]
  --drip N  writes one stream file every N seconds (to demo live mode alongside
            `python src/stream_job.py --mode live`).
"""
import argparse
import json
import os
import random
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config as C  # noqa: E402

SEED = 7
SENSORS = [f"S{i:02d}" for i in range(1, 21)]
NOISE = {"temperature": 1.5, "vibration": 0.05, "pressure": 2.0}
START = datetime(2026, 1, 1, 0, 0, 0)
STEP_S, EVENTS_PER_MIN = 10, 6
N_FILES, MIN_PER_FILE = 30, 2

# name: (sensor, metric, first_min, last_min, offset(minute) -> value shift)
ANOMALIES = [
    ("spike", "S07", "temperature", 20, 24, lambda m: 8.0),                          # sudden spike
    ("drift", "S13", "vibration", 35, 50, lambda m: 0.3 * (m - 35) / 15),            # slow drift
    ("drop", "S03", "pressure", 45, 47, lambda m: -15.0),                            # pressure loss
]


def make_profiles(rng):
    return {s: {"temperature": rng.normal(70, 5), "vibration": rng.normal(0.5, 0.08),
                "pressure": rng.normal(100, 8)} for s in SENSORS}


def anomaly_shift(sensor, metric, minute):
    for _, s, m, lo, hi, fn in ANOMALIES:
        if s == sensor and m == metric and lo <= minute < hi + 1:
            return fn(minute)
    return 0.0


def write_history(rng, profiles):
    frames = []
    n = 24 * 3600 // STEP_S
    ts = [START - timedelta(days=1) + timedelta(seconds=STEP_S * i) for i in range(n)]
    for s in SENSORS:
        df = pd.DataFrame({"sensor_id": s, "event_ts": [t.strftime("%Y-%m-%d %H:%M:%S") for t in ts]})
        for m in C.METRICS:
            df[m] = np.round(rng.normal(profiles[s][m], NOISE[m], n), 3)
        frames.append(df)
    out = Path(C.HISTORY_CSV)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(frames).to_csv(out, index=False, compression="gzip")
    return sum(len(f) for f in frames)


def build_stream_files(rng, profiles):
    """Returns list of (filename, lines) and a stats dict."""
    stats = {"events": 0, "duplicates": 0, "corrupt": 0, "dropouts": 0, "late": 0}
    files = []
    for f in range(N_FILES):
        lines = []
        for sensor in SENSORS:
            for k in range(MIN_PER_FILE * EVENTS_PER_MIN):
                ts = START + timedelta(minutes=f * MIN_PER_FILE, seconds=k * STEP_S)
                minute = (ts - START).total_seconds() / 60 + 0.5 / EVENTS_PER_MIN
                rec = {"event_id": f"{sensor}-{int(ts.timestamp())}", "sensor_id": sensor,
                       "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S")}
                for m in C.METRICS:
                    rec[m] = round(float(rng.normal(profiles[sensor][m], NOISE[m])
                                         + anomaly_shift(sensor, m, int(minute))), 3)
                if random.random() < 0.01:            # sensor dropout
                    rec["temperature"] = None
                    stats["dropouts"] += 1
                lines.append(json.dumps(rec))
                stats["events"] += 1
                if random.random() < 0.01:            # duplicate delivery
                    lines.append(json.dumps(rec)); stats["duplicates"] += 1
        # late events: timestamps from minutes 3-8, delivered in later files (beyond watermark)
        if f >= 15:
            for j in range(5):
                ts = START + timedelta(minutes=3 + j, seconds=5)
                rec = {"event_id": f"LATE-{f}-{j}", "sensor_id": random.choice(SENSORS),
                       "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                       "temperature": 70.0, "vibration": 0.5, "pressure": 100.0}
                lines.append(json.dumps(rec)); stats["late"] += 1
        # corrupt lines (truncated JSON)
        for _ in range(random.choice([0, 1, 1, 2])):
            lines.append('{"event_id": "S01-broken", "sensor_id": "S01", "event_ts": "2026-01-01 0')
            stats["corrupt"] += 1
        random.shuffle(lines)
        files.append((f"events_{f:03d}.json", lines))
    return files, stats


def ground_truth():
    """Windows whose injected mean shift is >= 5 window-sigmas, i.e. unmistakable."""
    truth = []
    for name, sensor, metric, lo, hi, fn in ANOMALIES:
        window_sigma = NOISE[metric] / np.sqrt(EVENTS_PER_MIN)
        for m in range(lo, hi + 1):
            if abs(fn(m + 0.5)) >= 5 * window_sigma:
                truth.append({"anomaly": name, "sensor_id": sensor, "metric": metric,
                              "window_start": (START + timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S")})
    return truth


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--drip", type=float, default=0, help="seconds between stream files (0 = write all at once)")
    args = ap.parse_args()

    random.seed(SEED)
    rng = np.random.default_rng(SEED)
    profiles = make_profiles(rng)

    shutil.rmtree(C.STREAM_IN, ignore_errors=True)
    Path(C.STREAM_IN).mkdir(parents=True, exist_ok=True)

    n_hist = write_history(rng, profiles)
    files, stats = build_stream_files(rng, profiles)
    truth = ground_truth()
    with open(C.GROUND_TRUTH, "w") as fh:
        json.dump({"truth": truth, "stats": stats}, fh, indent=2)

    base = time.time() - len(files)
    for i, (name, lines) in enumerate(files):
        tmp = Path(C.STREAM_IN) / f"_tmp_{name}"          # Spark ignores files starting with "_"
        tmp.write_text("\n".join(lines) + "\n")
        if not args.drip:
            # Spark's file source orders files by modification time with *millisecond* resolution.
            # Files written in the same ms are processed in arbitrary order, which makes older
            # data look "late" and the watermark would drop it. Give each file a distinct mtime.
            os.utime(tmp, (base + i, base + i))
        os.replace(tmp, Path(C.STREAM_IN) / name)         # atomic: Spark never sees half-written files
        if args.drip:
            print(f"wrote {name}"); time.sleep(args.drip)

    print(f"history rows={n_hist:,} | stream files={len(files)} | {stats} | truth windows={len(truth)}")


if __name__ == "__main__":
    main()
