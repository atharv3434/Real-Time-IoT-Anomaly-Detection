"""Evaluate the streaming output against the generator's ground truth and draw charts."""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config as C  # noqa: E402
from src.spark_session import get_spark  # noqa: E402

spark = get_spark("report")
rows = spark.read.parquet(C.WINDOW_METRICS).orderBy("window_start", "sensor_id").collect()
w = pd.DataFrame([{**r.asDict(), "anomalous_metrics": list(r.anomalous_metrics)} for r in rows])
n_quarantined = spark.read.parquet(C.QUARANTINE).count()
spark.stop()

gt = json.load(open(C.GROUND_TRUTH))
truth = {(t["sensor_id"], t["window_start"], t["metric"]) for t in gt["truth"]}
w["window_start"] = pd.to_datetime(w["window_start"])
alerts = w[w.is_anomaly]
detected = {(r.sensor_id, r.window_start.strftime("%Y-%m-%d %H:%M:%S"), m)
            for r in alerts.itertuples() for m in r.anomalous_metrics}

tp, fp, fn = len(truth & detected), len(detected - truth), len(truth - detected)
precision = tp / (tp + fp) if tp + fp else 0
recall = tp / (tp + fn) if tp + fn else 0

print("=" * 56)
print(f"windows scored            : {len(w):,}  ({w.window_start.nunique()} minutes x {w.sensor_id.nunique()} sensors)")
print(f"alerts raised             : {len(alerts)}")
print(f"true positives            : {tp}/{len(truth)}   recall    = {recall:.1%}")
print(f"false positives           : {fp}             precision = {precision:.1%}")
print(f"corrupt lines injected    : {gt['stats']['corrupt']}  | quarantined: {n_quarantined}  "
      f"-> {'MATCH' if n_quarantined == gt['stats']['corrupt'] else 'MISMATCH'}")
print(f"last window finalised     : {w.window_start.max():%H:%M}  (final windows stay open until the watermark passes)")
print("=" * 56)

# ---------------- charts ----------------
fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True)
for ax, (sensor, metric, title) in zip(axes, [("S07", "temperature", "S07 temperature - sudden spike"),
                                              ("S13", "vibration", "S13 vibration - gradual drift"),
                                              ("S03", "pressure", "S03 pressure - sudden drop")]):
    d = w[w.sensor_id == sensor]
    ax.plot(d.window_start, d[f"{metric}_z"], lw=1.5, label="z-score")
    ax.axhline(C.Z_THRESHOLD, color="red", ls="--", lw=1); ax.axhline(-C.Z_THRESHOLD, color="red", ls="--", lw=1)
    hit = d[d.anomalous_metrics.apply(lambda a: metric in a)]
    ax.scatter(hit.window_start, hit[f"{metric}_z"], color="red", zorder=3, label="alert")
    ax.set(title=title, ylabel="z-score"); ax.legend(loc="upper left")
plt.tight_layout()
Path(C.ROOT / "reports").mkdir(exist_ok=True)
plt.savefig(C.ROOT / "reports" / "anomalies.png", dpi=130)
print("chart -> reports/anomalies.png")
