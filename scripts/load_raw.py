"""Load Olist CSVs into Postgres schema raw.* as untouched text columns (COPY).
Typing and cleaning happen in dbt staging, so raw stays a faithful copy of the source."""
import csv, os, sys, glob
import psycopg2

DSN = dict(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5432"),
           user=os.getenv("PGUSER", "dbt"), password=os.getenv("PGPASSWORD", "dbt"),
           dbname=os.getenv("PGDATABASE", "ecom"))
SRC = sys.argv[1] if len(sys.argv) > 1 else "seeds_raw"
TABLES = {
    "olist_customers_dataset": "customers", "olist_orders_dataset": "orders",
    "olist_order_items_dataset": "order_items", "olist_order_payments_dataset": "order_payments",
    "olist_order_reviews_dataset": "order_reviews", "olist_products_dataset": "products",
    "olist_sellers_dataset": "sellers", "product_category_name_translation": "category_translation",
}
con = psycopg2.connect(**DSN); con.autocommit = False
cur = con.cursor()
cur.execute("create schema if not exists raw")
for fname, tname in TABLES.items():
    path = os.path.join(SRC, fname + ".csv")
    with open(path, newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f))
    cols = ", ".join(f'"{c.strip()}" text' for c in header)
    cur.execute(f"drop table if exists raw.{tname} cascade")
    cur.execute(f"create table raw.{tname} ({cols})")
    with open(path, encoding="utf-8") as f:
        cur.copy_expert(f"copy raw.{tname} from stdin with (format csv, header true)", f)
    cur.execute(f"select count(*) from raw.{tname}")
    print(f"raw.{tname:22s} {cur.fetchone()[0]:>8,} rows")
con.commit(); con.close()
