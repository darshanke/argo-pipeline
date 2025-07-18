Agro Sensor Data Pipeline

A production-ready pipeline to ingest, transform, validate, and store agricultural sensor data from multiple sources — optimized for downstream analytics.



##  Features

-  Ingest raw Parquet files (split by day)
-  Clean & transform sensor readings
-  Apply calibration & detect anomalies
-  Validate data quality using DuckDB
-  Store partitioned Parquet data (by date & sensor)
-  Dockerized for portability

---

## 📁 Project Structure

agro_pipeline/
├── main.py # Entry point
├── generate_sample_data.py
├── requirements.txt
├── Dockerfile
├── pipeline/
│ ├── ingest.py
│ ├── transform.py
│ ├── validate.py
│ └── load.py
└── data/
├── raw/ # Input Parquet files
├── processed/ # Output partitioned data
└── reports/ # CSV data quality reports



## ⚙️ Setup & Run

### 🔧 1. Install Locally

```bash
pip install -r requirements.txt
python generate_sample_data.py     # Generate demo input
python main.py  

## ⚙️ Setup & Run
docker build -t agro-pipeline .
docker run --rm -v ${PWD}/data:/app/data agro-pipeline

Pipeline Overview
📥 Ingestion
Reads all .parquet files from data/raw/

Handles corrupt/missing data

Logs stats using DuckDB

🧼 Transformation
Drop duplicates

Fill/drop missing values

Normalize using hardcoded calibration:

ini
Copy
Edit
value = raw_value * multiplier + offset
Detect outliers using Z-score (> 3)

Tag anomalies if value is out of expected range

Calculate:

Daily average per sensor/reading type

7-day rolling average

🔍 Validation
Powered by DuckDB

Checks:

Missing values

Type consistency

Anomalies / outliers

Hourly data coverage

Output: data/reports/*.csv

📤 Load & Storage
Saved as partitioned Parquet by date and sensor_id

Compression: Snappy
Columnar format for analytics



Example query 
SELECT * FROM 'data/processed/*.parquet'
WHERE reading_type = 'temperature'
  AND date = DATE '2023-06-01'
ORDER BY timestamp;



 Example Calibration Table
Reading Type	Multiplier	Offset
temperature	    1.02	    -0.5
humidity	    0.98	    1.0
soil_moisture	1.1	        0.0
light	        1.0	        0.0
battery	        1.0	        0.0


gitHub repo: 
Video: 




