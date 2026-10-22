"""Pure DataFrame transformations, shared by batch (baseline / tests) and streaming code.

Because these only use operations supported on both batch and streaming DataFrames,
the exact same logic is unit-tested on static data and run on the stream.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src import config as C

# _corrupt_record is where Spark's PERMISSIVE JSON reader puts lines it cannot parse.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("sensor_id", StringType()),
    StructField("event_ts", StringType()),
    *[StructField(m, DoubleType()) for m in C.METRICS],
    StructField("_corrupt_record", StringType()),
])


def clean_events(df: DataFrame) -> DataFrame:
    """Drop unparseable rows, parse the timestamp safely (Spark 4 ANSI: use try_*)."""
    return (
        df.filter(F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("event_ts", F.expr("try_to_timestamp(event_ts, 'yyyy-MM-dd HH:mm:ss')"))
        .filter(F.col("sensor_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("event_id").isNotNull())
    )


def window_aggregates(events: DataFrame, window: str = C.WINDOW, watermark: str = C.WATERMARK) -> DataFrame:
    """Dedupe + tumbling-window aggregation per sensor.

    - withWatermark bounds how long Spark keeps state and when a window is final.
    - dropDuplicates includes event_ts so the watermark can evict old dedupe state.
    """
    return (
        events.withWatermark("event_ts", watermark)
        .dropDuplicates(["event_id", "event_ts"])
        .groupBy(F.window("event_ts", window).alias("w"), "sensor_id")
        .agg(F.count("*").alias("n_events"),
             *[F.avg(m).alias(m) for m in C.METRICS])
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "sensor_id", "n_events", *C.METRICS)
    )


def build_baseline(history: DataFrame, window: str = C.WINDOW) -> DataFrame:
    """Per-sensor mean/std of *per-window averages* (same granularity we score on)."""
    hist = history.withColumn("event_ts", F.to_timestamp("event_ts"))
    per_window = (hist.groupBy(F.window("event_ts", window).alias("w"), "sensor_id")
                  .agg(*[F.avg(m).alias(m) for m in C.METRICS]))
    return per_window.groupBy("sensor_id").agg(
        *[F.avg(m).alias(f"{m}_mu") for m in C.METRICS],
        *[F.stddev(m).alias(f"{m}_sigma") for m in C.METRICS],
    )


def score_windows(windows: DataFrame, baseline: DataFrame, z_threshold: float = C.Z_THRESHOLD,
                  critical_z: float = C.CRITICAL_Z, min_events: int = C.MIN_EVENTS) -> DataFrame:
    """Stream-static join with the baseline, then z-score each metric."""
    s = windows.join(F.broadcast(baseline), "sensor_id", "left")
    for m in C.METRICS:
        s = s.withColumn(f"{m}_z", F.round(
            (F.col(m) - F.col(f"{m}_mu")) / F.greatest(F.col(f"{m}_sigma"), F.lit(1e-9)), 2))
    s = s.withColumn("low_confidence", F.col("n_events") < min_events)
    s = s.withColumn("anomalous_metrics", F.array_compact(F.array(
        *[F.when(F.abs(F.col(f"{m}_z")) > z_threshold, F.lit(m)) for m in C.METRICS])))
    s = s.withColumn("max_abs_z", F.greatest(*[F.abs(F.col(f"{m}_z")) for m in C.METRICS]))
    s = s.withColumn("is_anomaly", (F.size("anomalous_metrics") > 0) & ~F.col("low_confidence"))
    s = s.withColumn("severity", F.when(F.col("is_anomaly") & (F.col("max_abs_z") >= critical_z), "critical")
                                  .when(F.col("is_anomaly"), "warning").otherwise("ok"))
    keep = ["window_start", "window_end", "sensor_id", "n_events", *C.METRICS,
            *[f"{m}_z" for m in C.METRICS], "max_abs_z", "anomalous_metrics",
            "low_confidence", "is_anomaly", "severity"]
    return s.select(*keep)
