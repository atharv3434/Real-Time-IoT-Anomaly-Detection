# Real-Time IoT Anomaly Detection (PySpark Structured Streaming)

Detects abnormal readings from 20 simulated industrial sensors (temperature, vibration, pressure)
as data arrives, using **Spark Structured Streaming** with event-time windows, watermarks and a
statistical baseline learned from historical data.

```
 history.csv.gz ──► [BATCH] per-sensor baseline (mean/std of 1-min averages) ─┐ static side
                                                                              ▼
 stream_in/*.json ─► parse (PERMISSIVE) ─► clean ─► watermark + dedupe ─► 1-min windows ─► z-score ─► foreachBatch
      │                                                                                              ├─► window_metrics (parquet)
      └─► corrupt lines ───────────────────────────────────────────────────────────────────────────►  └─► alerts (parquet + console)
                                                                  quarantine (parquet, with source file)
```

## Quick start
```bash
pip install -r requirements.txt            # needs Java 17+
python scripts/generate_data.py            # history + 30 stream files + ground truth
python scripts/train_baseline.py           # batch: learn normal behaviour
python src/stream_job.py                   # replay mode (Trigger.AvailableNow), prints alerts per micro-batch
python scripts/report.py                   # precision/recall vs ground truth + reports/anomalies.png
pytest -q                                  # 5 unit tests

# Live demo (two terminals)
python scripts/generate_data.py --drip 3   # one new file every 3 s
python src/stream_job.py --mode live       # micro-batch every 5 s, Ctrl-C to stop
```

## Streaming concepts demonstrated
| Concept | Where |
|---|---|
| File source with explicit schema, `maxFilesPerTrigger` | `stream_job.read_raw_stream` |
| Malformed-record handling (`PERMISSIVE` + `_corrupt_record`) and a quarantine sink with lineage (`_metadata.file_name`) | `stream_job.main`, `transforms.clean_events` |
| Event-time tumbling windows + **watermark** | `transforms.window_aggregates` |
| Stateful de-duplication bounded by the watermark | `dropDuplicates(["event_id","event_ts"])` |
| **Stream-static join** (broadcast baseline) | `transforms.score_windows` |
| `foreachBatch` multi-sink writes, checkpointing | `stream_job.process_batch` |
| `Trigger.AvailableNow` (replay/backfill) vs `processingTime` (live) | `--mode` |
| Same transforms unit-tested on static DataFrames | `tests/` |

## Results (synthetic data, replay mode)
- 7,200 events -> 1,140 sensor-minute windows (57 minutes x 20 sensors).
- 3 injected faults: a sudden temperature spike (S07), a slow vibration drift (S13), a pressure drop (S03).
- **19/19 truly anomalous windows detected, 0 false positives.** The drift is flagged as soon as
  it exceeds 4 sigma (00:40), well before it peaks.
- All 29 corrupt JSON lines were quarantined; 77 duplicates and 75 deliberately late events did not
  distort the aggregates.

Perfect scores partly reflect the data: noise is Gaussian and the faults are large. Real sensors
have seasonality, drift and non-Gaussian noise, so expect to tune `Z_THRESHOLD` and use richer models.

## Things worth knowing (lessons from building it)
1. **File ordering and mtime.** Spark's file source orders by modification time at *millisecond*
   resolution. Files written in the same millisecond are processed in arbitrary order, so older
   data arrives after the watermark has advanced and is silently dropped as late. The generator
   gives each file a distinct mtime for this reason. In production, use a message queue (Kafka)
   or guarantee ordered, distinct landing times.
2. **Watermark latency.** A window is only emitted once the watermark passes its end, so the last
   `WATERMARK` minutes of data stay "open" (here, minutes 57-59 are never emitted in replay).
   Lower watermark = faster alerts but more late data discarded.
3. **Late data is timing dependent.** In live mode an injected late event can land in a micro-batch
   *before* the watermark has passed its window and be counted (we observed one marginal false
   positive, S09 00:05, |z|=4.1, because of this). Replay mode is deterministic for the same files.
4. **Spark 4 is ANSI by default.** Bad casts raise errors, so parsing uses `try_to_timestamp`.
5. **`_corrupt_record` queries** may not reference *only* that column, so the quarantine query
   selects extra columns.
6. `foreachBatch` is at-least-once; make sinks idempotent for exactly-once behaviour.

## Layout
```
src/config.py         paths + thresholds          scripts/generate_data.py   simulator + ground truth
src/transforms.py     pure, testable logic        scripts/train_baseline.py  batch baseline
src/stream_job.py     streaming queries           scripts/report.py          evaluation + charts
tests/                pytest suite
```

## Ideas to extend
Swap the file source for Kafka; use `applyInPandasWithState` for per-sensor rolling baselines or
CUSUM drift detection; add an alert-suppression state (cool-down); push alerts to Slack/webhook.
