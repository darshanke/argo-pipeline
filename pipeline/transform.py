import pandas as pd
import numpy as np
from scipy.stats import zscore
from datetime import timedelta

# Calibration params (hardcoded for now)
CALIBRATION_PARAMS = {
    "temperature": {"multiplier": 1.02, "offset": -0.5},
    "humidity": {"multiplier": 0.98, "offset": 1.0},
    "soil_moisture": {"multiplier": 1.1, "offset": 0.0},
    "light": {"multiplier": 1.0, "offset": 0.0},
    "battery": {"multiplier": 1.0, "offset": 0.0},
}

# Value ranges (used for anomaly detection)
VALID_RANGES = {
    "temperature": (10, 45),
    "humidity": (20, 90),
    "soil_moisture": (0, 100),
    "light": (0, 1000),
    "battery": (3.0, 4.5),
}

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("🧼 Cleaning and transforming data...")

    # 1. Drop exact duplicates
    df = df.drop_duplicates()

    # 2. Handle missing values
    df = df.dropna(subset=["sensor_id", "timestamp", "reading_type", "value"])

    # 3. Convert timestamp & localize to UTC+5:30
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)  # remove tz for Parquet saving

    # 4. Apply calibration
    def calibrate(row):
        params = CALIBRATION_PARAMS.get(row["reading_type"], {"multiplier": 1, "offset": 0})
        return row["value"] * params["multiplier"] + params["offset"]

    df["value_calibrated"] = df.apply(calibrate, axis=1)

    # 5. Detect outliers using z-score (grouped by sensor + reading type)
    df["z_score"] = df.groupby(["sensor_id", "reading_type"])["value_calibrated"].transform(
        lambda x: zscore(x, nan_policy='omit')
    )
    df["is_outlier"] = df["z_score"].abs() > 3

    # 6. Flag anomaly based on range
    def is_anomalous(row):
        min_val, max_val = VALID_RANGES.get(row["reading_type"], (-np.inf, np.inf))
        return not (min_val <= row["value_calibrated"] <= max_val)

    df["anomalous_reading"] = df.apply(is_anomalous, axis=1)

    # 7. Daily average and 7-day rolling avg (for demo, keep it simple)
    df["date"] = df["timestamp"].dt.date
    daily_avg = (
        df.groupby(["sensor_id", "reading_type", "date"])["value_calibrated"]
        .mean()
        .reset_index()
        .rename(columns={"value_calibrated": "daily_avg"})
    )

    df = df.merge(daily_avg, on=["sensor_id", "reading_type", "date"], how="left")

    df.sort_values(by="timestamp", inplace=True)
    df["rolling_avg_7d"] = (
        df.groupby(["sensor_id", "reading_type"])["value_calibrated"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    print("✅ Transformation complete!")
    return df
