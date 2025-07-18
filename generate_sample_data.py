import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

SENSOR_TYPES = ["temperature", "humidity", "soil_moisture", "light", "battery"]
SENSOR_IDS = ["sensor_001", "sensor_002", "sensor_003"]

def generate_data_for_date(date):
    rows = []
    for sensor_id in SENSOR_IDS:
        for hour in range(24):
            timestamp = datetime.combine(date, datetime.min.time()) + timedelta(hours=hour)
            for reading_type in SENSOR_TYPES:
                value = np.random.normal(loc=25, scale=5) if reading_type != "battery" else np.random.uniform(3.5, 4.2)
                battery_level = np.random.uniform(3.5, 4.2)
                rows.append({
                    "sensor_id": sensor_id,
                    "timestamp": timestamp.isoformat(),
                    "reading_type": reading_type,
                    "value": round(value, 2),
                    "battery_level": round(battery_level, 2)
                })
    return pd.DataFrame(rows)

def main():
    raw_dir = "data/raw"
    os.makedirs(raw_dir, exist_ok=True)

    for i in range(3):  # Generate 3 days of data
        date = datetime(2023, 6, 1) + timedelta(days=i)
        df = generate_data_for_date(date)
        file_path = os.path.join(raw_dir, f"{date.date()}.parquet")
        df.to_parquet(file_path, index=False)
        print(f"✅ Generated {file_path}")

if __name__ == "__main__":
    main()
