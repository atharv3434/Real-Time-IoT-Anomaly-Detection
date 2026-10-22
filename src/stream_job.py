"""
Structured Streaming job.

  files -> parse (PERMISSIVE) -> clean -> watermark+dedupe -> 1-min windows per sensor
        -> stream-static join with baseline -> z-scores -> foreachBatch sinks (metrics + alerts)

A second query routes corrupt lines to a quarantine folder.

Modes
  replay (default): Trigger.AvailableNow - process everything already in the folder, then stop.
  live            : micro-batch every 5s until Ctrl-C (pair with `generate_data.py --drip 3`).
  
"""
import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F  # noqa: E402

from src import config as C  # noqa: E402
from src.spark_session import get_spark  # noqa: E402
from src.transforms import EVENT_SCHEMA, clean_events, score_windows, window_aggregates  # noqa: E402


def read_raw_stream(spark, max_files: int):
    return (spark.readStream.schema(EVENT_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("maxFilesPerTrigger", max_files)       # several micro-batches, shows watermark progress
            .json(C.STREAM_IN))


def process_batch(batch_df, batch_id: int) -> None:
    """foreachBatch sink: lets us write one result to several destinations.
    Delivery is at-least-once; for exactly-once use an idempotent/transactional sink."""
    batch_df.persist()
    n = batch_df.count()
    if n:
        batch_df.write.mode("append").parquet(C.WINDOW_METRICS)
        alerts = batch_df.filter("is_anomaly").cache()
        k = alerts.count()
        if k:
            alerts.write.mode("append").parquet(C.ALERTS)
            for r in alerts.orderBy("window_start").collect():
                print(f"  [batch {batch_id}] {r.severity.upper():<8} {r.sensor_id} "
                      f"{r.window_start:%H:%M} {','.join(r.anomalous_metrics)} (|z|={r.max_abs_z})")
        alerts.unpersist()
        print(f"batch {batch_id}: windows finalised={n}, alerts={k}")
    batch_df.unpersist()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["replay", "live"], default="replay")
    ap.add_argument("--keep-state", action="store_true", help="do not wipe outputs/checkpoints first")
    ap.add_argument("--max-files", type=int, default=5)
    args = ap.parse_args()

    if not args.keep_state:
        for p in (C.WINDOW_METRICS, C.ALERTS, C.QUARANTINE, C.CHECKPOINTS):
            shutil.rmtree(p, ignore_errors=True)

    spark = get_spark("iot-stream")
    baseline = spark.read.parquet(C.BASELINE)           # static side of the stream-static join
    raw = read_raw_stream(spark, args.max_files)
    trigger = {"availableNow": True} if args.mode == "replay" else {"processingTime": "5 seconds"}

    scored = score_windows(window_aggregates(clean_events(raw)), baseline)
    q_main = (scored.writeStream.outputMode("append")
              .foreachBatch(process_batch)
              .option("checkpointLocation", f"{C.CHECKPOINTS}/main")
              .trigger(**trigger).start())

    q_bad = (raw.filter(F.col("_corrupt_record").isNotNull())
             # Spark forbids queries that touch ONLY _corrupt_record, so also select other columns;
             # _metadata.file_name gives lineage back to the offending file.
             .select("_corrupt_record", "event_id", "sensor_id",
                     F.col("_metadata.file_name").alias("source_file"),
                     F.current_timestamp().alias("quarantined_at"))
             .writeStream.format("parquet").option("path", C.QUARANTINE)
             .option("checkpointLocation", f"{C.CHECKPOINTS}/quarantine")
             .trigger(**trigger).start())

    try:
        if args.mode == "replay":
            q_main.awaitTermination(); q_bad.awaitTermination()
        else:
            print("live mode - Ctrl-C to stop")
            while q_main.isActive:
                time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        for q in (q_main, q_bad):
            if q.isActive:
                q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
