import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "OLIST_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")


# -----------------------------
# File paths
# -----------------------------
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


# -----------------------------
# Snowflake connection
# -----------------------------
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


# -----------------------------
# Utility functions
# -----------------------------
def check_raw_files_exist():
    missing_files = []
    for filename in FILES_TO_LOAD:
        filepath = RAW_DATA_DIR / filename
        if not filepath.exists():
            missing_files.append(filename)

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


# -----------------------------
# Placeholder load logic
# -----------------------------
def load_csv_to_snowflake(cursor, filepath: Path, table_name: str):
    """
    Placeholder:
    Later this function will upload the file to Snowflake stage
    and execute COPY INTO for the target RAW table.
    """
    print(f"[TODO] Load {filepath.name} into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}")


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    print("Starting OLIST ingestion pipeline...")

    check_raw_files_exist()
    preview_file_shapes()

    conn = None
    cursor = None

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        for filename, table_name in FILES_TO_LOAD.items():
            filepath = RAW_DATA_DIR / filename
            load_csv_to_snowflake(cursor, filepath, table_name)

        print("\nIngestion pipeline completed (skeleton mode).")

    except Exception as e:
        print(f"\nError during ingestion: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()