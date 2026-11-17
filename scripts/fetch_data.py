"""Download the 8 Olist CSVs this project uses and verify row counts against the published dataset."""
import os, sys, urllib.request

BASE = "https://raw.githubusercontent.com/iiaauu456-boop/Brazilian-Olist-E-Commerce-Dataset/main"
EXPECTED = {  # file -> data rows (excluding header)
    "olist_customers_dataset": 99441, "olist_orders_dataset": 99441,
    "olist_order_items_dataset": 112650, "olist_order_payments_dataset": 103886,
    "olist_order_reviews_dataset": 99224, "olist_products_dataset": 32951,
    "olist_sellers_dataset": 3095, "product_category_name_translation": 71,
}
dest = sys.argv[1] if len(sys.argv) > 1 else "data"
os.makedirs(dest, exist_ok=True)
for name, want in EXPECTED.items():
    path = os.path.join(dest, name + ".csv")
    if not os.path.exists(path):
        print("downloading", name)
        urllib.request.urlretrieve(f"{BASE}/{name}.csv", path)
    import csv
    with open(path, newline="", encoding="utf-8-sig") as f:
        got = sum(1 for _ in csv.reader(f)) - 1  # csv-aware: review comments contain newlines
    if got != want:
        sys.exit(f"{name}: expected {want} rows, got {got}. If the mirror changed, download the Olist dataset from Kaggle into {dest}/")
    print(f"ok  {name:36s} {got:>8,} rows")
