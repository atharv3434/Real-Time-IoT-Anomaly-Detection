"""Unit tests on static DataFrames (same transforms the stream runs)."""

import pytest
from pyspark.sql import SparkSession

from src import config as C
from src.transforms import EVENT_SCHEMA, build_baseline, clean_events, score_windows


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def test_clean_events_drops_corrupt_and_bad_timestamps(spark):
    rows = [
        ("e1", "S1", "2026-01-01 00:00:10", 70.0, 0.5, 100.0, None),
        ("e2", "S1", "not-a-date", 70.0, 0.5, 100.0, None),     # bad timestamp
        (None, None, None, None, None, None, '{"truncated'),      # corrupt record
    ]
    out = clean_events(spark.createDataFrame(rows, EVENT_SCHEMA)).collect()
    assert [r.event_id for r in out] == ["e1"]
    assert out[0].event_ts is not None


def _baseline(spark):
    return spark.createDataFrame(
        [("S1", 70.0, 0.5, 100.0, 1.0, 0.05, 2.0)],
        ["sensor_id", "temperature_mu", "vibration_mu", "pressure_mu",
         "temperature_sigma", "vibration_sigma", "pressure_sigma"])


def _windows(spark, rows):
    return spark.createDataFrame(rows, ["window_start", "window_end", "sensor_id", "n_events", *C.METRICS])


def test_score_flags_only_real_deviations(spark):
    w = _windows(spark, [
        ("2026-01-01 00:00:00", "2026-01-01 00:01:00", "S1", 6, 70.5, 0.5, 100.0),    # normal
        ("2026-01-01 00:01:00", "2026-01-01 00:02:00", "S1", 6, 79.0, 0.5, 100.0),    # z=9 -> critical
        ("2026-01-01 00:02:00", "2026-01-01 00:03:00", "S1", 6, 70.0, 0.5, 90.0),     # z=-5 -> warning
    ])
    r = {x.window_start: x for x in score_windows(w, _baseline(spark)).collect()}
    assert r["2026-01-01 00:00:00"].severity == "ok"
    assert r["2026-01-01 00:01:00"].severity == "critical" and list(r["2026-01-01 00:01:00"].anomalous_metrics) == ["temperature"]
    assert r["2026-01-01 00:02:00"].severity == "warning" and list(r["2026-01-01 00:02:00"].anomalous_metrics) == ["pressure"]


def test_sparse_windows_are_low_confidence_not_alerts(spark):
    w = _windows(spark, [("2026-01-01 00:00:00", "2026-01-01 00:01:00", "S1", 1, 99.0, 0.5, 100.0)])
    r = score_windows(w, _baseline(spark)).first()
    assert r.low_confidence is True and r.is_anomaly is False


def test_unknown_sensor_is_not_flagged(spark):
    w = _windows(spark, [("2026-01-01 00:00:00", "2026-01-01 00:01:00", "NEW", 6, 99.0, 0.5, 100.0)])
    assert score_windows(w, _baseline(spark)).first().is_anomaly is False


def test_baseline_matches_window_average_statistics(spark):
    # two minutes for one sensor, constant values -> mu = value, sigma over 2 windows
    hist = spark.createDataFrame(
        [("S1", "2026-01-01 00:00:10", 10.0, 1.0, 5.0), ("S1", "2026-01-01 00:00:40", 10.0, 1.0, 5.0),
         ("S1", "2026-01-01 00:01:10", 20.0, 1.0, 5.0), ("S1", "2026-01-01 00:01:40", 20.0, 1.0, 5.0)],
        ["sensor_id", "event_ts", *C.METRICS])
    r = build_baseline(hist).first()
    assert r.temperature_mu == pytest.approx(15.0)
    assert r.temperature_sigma == pytest.approx(7.0710678, rel=1e-4)   # sample stddev of [10, 20]
    assert r.vibration_sigma == pytest.approx(0.0)
