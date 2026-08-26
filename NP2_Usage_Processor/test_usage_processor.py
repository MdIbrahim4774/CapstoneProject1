import pandas as pd

from usage_processor import UsageProcessor


def sample_data():
    return pd.DataFrame({
        "timestamp": [
            "2013-11-03 10:00:00",
            "2013-11-03 10:20:00",
            "2013-11-03 11:00:00",
        ],
        "grid_id": [
            1,
            1,
            2,
        ],
        "sms_in": [
            10,
            5,
            2,
        ],
        "sms_out": [
            5,
            2,
            3,
        ],
        "call_in": [
            2,
            1,
            4,
        ],
        "call_out": [
            3,
            1,
            2,
        ],
        "internet": [
            20,
            10,
            30,
        ],
    })


def test_load_data():
    processor = UsageProcessor(sample_data())

    df = processor.load_data()

    assert len(df) == 3


def test_clean_data():
    processor = UsageProcessor(sample_data())

    processor.load_data()
    df = processor.clean_data()

    assert df["timestamp"].notna().all()
    assert df["grid_id"].notna().all()


def test_derive_time_features():
    processor = UsageProcessor(sample_data())

    processor.load_data()
    processor.clean_data()

    df = processor.derive_time_features()

    assert "date" in df.columns
    assert "hour" in df.columns
    assert "day_of_week" in df.columns


def test_aggregate_to_grid_time():
    processor = UsageProcessor(sample_data())

    processor.load_data()
    processor.clean_data()

    result = processor.aggregate_to_grid_time()

    assert "grid_id" in result.columns
    assert "hour_timestamp" in result.columns

    # Two records for grid 1 during the same hour
    # should become one record.
    assert len(result) == 2


def test_derive_activity_features():
    processor = UsageProcessor(sample_data())

    processor.load_data()
    processor.clean_data()
    processor.aggregate_to_grid_time()

    result = processor.derive_activity_features()

    assert "total_sms" in result.columns
    assert "total_calls" in result.columns
    assert "total_activity" in result.columns


def test_compute_kpis():
    processor = UsageProcessor(sample_data())

    processor.load_data()
    processor.clean_data()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()

    daily, grid = processor.compute_kpis()

    assert not daily.empty
    assert not grid.empty


def test_export_summary(tmp_path):
    processor = UsageProcessor(sample_data())

    processor.run(tmp_path)

    assert (tmp_path / "daily_summary.csv").exists()
    assert (tmp_path / "grid_summary.csv").exists()