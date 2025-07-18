from pipeline.ingest import ingest_data
from pipeline.transform import transform_data
from pipeline.validate import validate_data
from pipeline.load import load_data


def main():
    raw_data_path = "data/raw/"
    processed_data_path = "data/processed/"

    print("🚀 Starting ingestion...")
    df = ingest_data(raw_data_path)

    print("🛠️ Transforming data...")
    df_transformed = transform_data(df)

    print("🔍 Validating data...")
    validate_data(df_transformed)

    print("📦 Saving data...")
    load_data(df_transformed, processed_data_path)

    print("✅ Pipeline completed successfully!")


if __name__ == "__main__":
    main()
