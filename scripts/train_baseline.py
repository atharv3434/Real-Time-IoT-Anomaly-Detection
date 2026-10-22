"""BATCH step: learn each sensor's normal behaviour from 1 day of history."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config as C  # noqa: E402
from src.spark_session import get_spark  # noqa: E402
from src.transforms import build_baseline  # noqa: E402

spark = get_spark("baseline")
hist = spark.read.csv(C.HISTORY_CSV, header=True, inferSchema=True)
base = build_baseline(hist).cache()
base.write.mode("overwrite").parquet(C.BASELINE)
print(f"history rows={hist.count():,} -> baseline for {base.count()} sensors")
base.orderBy("sensor_id").select("sensor_id", "temperature_mu", "temperature_sigma",
                                 "vibration_mu", "vibration_sigma").show(5)
spark.stop()
