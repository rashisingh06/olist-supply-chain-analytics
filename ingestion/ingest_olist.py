import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "OLIST_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

FILES_TO_LOAD = {
    "olist_orders_dataset.csv": "OLIST_ORDERS",
    "olist_order_items_dataset.csv": "OLIST_ORDER_ITEMS",
    "olist_order_payments_dataset.csv": "OLIST_ORDER_PAYMENTS",
    "olist_products_dataset.csv": "OLIST_PRODUCTS",
    "olist_customers_dataset.csv": "OLIST_CUSTOMERS",
    "olist_sellers_dataset.csv": "OLIST_SELLERS",
    "olist_geolocation_dataset.csv": "OLIST_GEOLOCATION",
    "product_category_name_translation.csv": "OLIST_PRODUCT_CATEGORY_TRANSLATION",
}


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def check_raw_files_exist():
    missing_files = [
        filename for filename in FILES_TO_LOAD
        if not (RAW_DATA_DIR / filename).exists()
    ]
    if missing_files:
        raise FileNotFoundError(
            f"Missing raw files in {RAW_DATA_DIR}:\n" + "\n".join(missing_files)
        )


def preview_file_shapes():
    print("Previewing source files...\n")
    for filename in FILES_TO_LOAD:
        filepath = RAW_DATA_DIR / filename
        df = pd.read_csv(filepath)
        print(f"{filename}: {df.shape[0]} rows, {df.shape[1]} columns")


def load_csv_to_snowflake(conn, filepath: Path, table_name: str):
    print(f"Loading {filepath.name} -> {table_name}")

    df = pd.read_csv(filepath)

    # normalize column names
    df.columns = [col.upper() for col in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=False,
        overwrite=True,
    )

    if success:
        print(f"Loaded {nrows} rows into {table_name}")
    else:
        raise RuntimeError(f"Failed to load {filepath.name} into {table_name}")


def main():
    print("Starting OLIST ingestion pipeline...")

    check_raw_files_exist()
    preview_file_shapes()

    conn = None

    try:
        conn = get_snowflake_connection()

        for filename, table_name in FILES_TO_LOAD.items():
            filepath = RAW_DATA_DIR / filename
            load_csv_to_snowflake(conn, filepath, table_name)

        print("\nIngestion pipeline completed successfully.")

    except Exception as e:
        print(f"\nError during ingestion: {e}")
        raise

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()