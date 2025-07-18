import pandas as pd
import duckdb
import os
import glob


def ingest_data(path):
    parquet_files = glob.glob(os.path.join(path, "*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No Parquet files found in raw data path.")

    df_list = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            df["source_file"] = os.path.basename(file)
            df_list.append(df)
        except Exception as e:
            print(f"❌ Error reading {file}: {e}")

    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df
