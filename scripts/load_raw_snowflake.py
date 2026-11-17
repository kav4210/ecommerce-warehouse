"""Load the Olist CSVs into Snowflake RAW.* as untouched text columns (PUT to a table stage, then COPY INTO).
Same contract as load_raw.py for Postgres: raw stays a faithful copy, dbt does the typing and cleaning.
Every table's row count is checked against a csv-module count of the file, because the review comments
contain quoted newlines and a naive line count would be wrong."""
import csv, os, sys
import snowflake.connector

SRC = sys.argv[1] if len(sys.argv) > 1 else "data"
TABLES = {
    "olist_customers_dataset": "customers", "olist_orders_dataset": "orders",
    "olist_order_items_dataset": "order_items", "olist_order_payments_dataset": "order_payments",
    "olist_order_reviews_dataset": "order_reviews", "olist_products_dataset": "products",
    "olist_sellers_dataset": "sellers", "product_category_name_translation": "category_translation",
}
con = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.getenv("SNOWFLAKE_USER", "DBT_USER"),
    authenticator="SNOWFLAKE_JWT",
    private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_key.p8"),
    role="DBT_ROLE", warehouse="ECOM_WH", database="ECOM")
cur = con.cursor()
cur.execute("create schema if not exists raw")
cur.execute("use schema raw")
for fname, tname in TABLES.items():
    path = os.path.abspath(os.path.join(SRC, fname + ".csv"))
    with open(path, newline="", encoding="utf-8-sig") as f:
        rd = csv.reader(f)
        header = next(rd)
        expected = sum(1 for _ in rd)
    cols = ", ".join(f"{c.strip()} varchar" for c in header)
    cur.execute(f"create or replace table raw.{tname} ({cols})")
    cur.execute(f"put 'file://{path.replace(os.sep, '/')}' @raw.%{tname} overwrite=true auto_compress=true")
    cur.execute(
        f"copy into raw.{tname} from @raw.%{tname} "
        "file_format=(type=csv skip_header=1 field_optionally_enclosed_by='\"' encoding='UTF8') "
        "on_error=abort_statement purge=true")
    cur.execute(f"select count(*) from raw.{tname}")
    got = cur.fetchone()[0]
    if got != expected:
        sys.exit(f"raw.{tname}: loaded {got:,} rows but the file has {expected:,}")
    print(f"raw.{tname:22s} {got:>8,} rows  (matches file)")
con.close()
