import pandas as pd
import os

def load_data(df: pd.DataFrame, path: str) -> None:
    print("💾 Saving data to partitioned Parquet...")

    os.makedirs(path, exist_ok=True)

    # Ensure timestamp is pandas datetime64
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Save partitioned Parquet (by date and sensor_id)
    df.to_parquet(
        path,
        engine="pyarrow",
        index=False,
        partition_cols=["date", "sensor_id"],
        compression="snappy"
    )

    print(f"✅ Data saved to {path}")
