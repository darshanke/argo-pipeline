import pandas as pd
import duckdb
import os

# Instead of one big file, save each report as its own CSV
def validate_data(df: pd.DataFrame) -> None:
    print("🔎 Running data validation...")
    con = duckdb.connect()
    con.register("sensor_data", df)

    reports = {
        "missing_values": con.execute("""
            SELECT 
                COUNT(*) AS total_rows,
                SUM(CASE WHEN sensor_id IS NULL THEN 1 ELSE 0 END) AS missing_sensor_id,
                SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS missing_timestamp,
                SUM(CASE WHEN reading_type IS NULL THEN 1 ELSE 0 END) AS missing_reading_type,
                SUM(CASE WHEN value_calibrated IS NULL THEN 1 ELSE 0 END) AS missing_value
            FROM sensor_data
        """).df(),

        "anomalies": con.execute("""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) AS anomalies,
                ROUND(100.0 * SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percent
            FROM sensor_data
        """).df(),

        "outliers": con.execute("""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outliers,
                ROUND(100.0 * SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) / COUNT(*), 2) AS outlier_percent
            FROM sensor_data
        """).df(),

        "time_coverage_gaps": con.execute("""
            SELECT 
                sensor_id, reading_type, date,
                COUNT(DISTINCT strftime('%H', timestamp)) AS hours_covered,
                24 - COUNT(DISTINCT strftime('%H', timestamp)) AS missing_hours
            FROM (
                SELECT *, CAST(timestamp AS DATE) AS date FROM sensor_data
            )
            GROUP BY sensor_id, reading_type, date
            HAVING missing_hours > 0
        """).df()
    }

    # Save individual files
    os.makedirs("data/reports", exist_ok=True)
    for name, report in reports.items():
        report.to_csv(f"data/reports/{name}.csv", index=False)

    print("✅ Validation reports saved to `data/reports/`")
