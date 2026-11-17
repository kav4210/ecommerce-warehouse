"""Cross-warehouse parity check. The control totals in expected_marts.json were generated from the PostgreSQL
build (`--generate`). Running with --warehouse snowflake recomputes the same queries on the Snowflake marts and
fails if any total differs, so "the same models give the same answers on both warehouses" is tested, not assumed."""
import json, math, os, sys

CHECKS = [
    ("dim_customer rows", "select count(*) from mart.dim_customer"),
    ("dim_customer current versions", "select sum(case when is_current then 1 else 0 end) from mart.dim_customer"),
    ("dim_date rows", "select count(*) from mart.dim_date"),
    ("dim_date weekend days", "select sum(case when is_weekend then 1 else 0 end) from mart.dim_date"),
    ("dim_product rows", "select count(*) from mart.dim_product"),
    ("dim_seller rows", "select count(*) from mart.dim_seller"),
    ("fct_orders rows", "select count(*) from mart.fct_orders"),
    ("fct_orders sum payment_value", "select sum(payment_value) from mart.fct_orders"),
    ("fct_orders sum items_value", "select sum(items_value) from mart.fct_orders"),
    ("fct_orders sum freight_value", "select sum(freight_value) from mart.fct_orders"),
    ("fct_orders late orders", "select sum(case when is_late then 1 else 0 end) from mart.fct_orders"),
    ("fct_orders delivered orders", "select sum(case when is_delivered then 1 else 0 end) from mart.fct_orders"),
    ("fct_orders sum floor(delivery_days)", "select sum(floor(delivery_days)) from mart.fct_orders"),
    ("fct_orders orders with a payment_types value", "select count(payment_types) from mart.fct_orders"),
    ("fct_order_items rows", "select count(*) from mart.fct_order_items"),
    ("fct_order_items sum price", "select sum(price) from mart.fct_order_items"),
    ("fct_order_items sum line_total", "select sum(line_total) from mart.fct_order_items"),
    ("mart_monthly_sales rows", "select count(*) from mart.mart_monthly_sales"),
    ("mart_monthly_sales sum orders", "select sum(orders) from mart.mart_monthly_sales"),
    ("mart_monthly_sales sum items_revenue", "select sum(items_revenue) from mart.mart_monthly_sales"),
    ("mart_cohort_retention rows", "select count(*) from mart.mart_cohort_retention"),
    ("mart_cohort_retention sum active", "select sum(active) from mart.mart_cohort_retention"),
    ("mart_cohort_retention max month_offset", "select max(month_offset) from mart.mart_cohort_retention"),
    ("mart_seller_scorecard rows", "select count(*) from mart.mart_seller_scorecard"),
    ("mart_seller_scorecard sum revenue", "select sum(revenue) from mart.mart_seller_scorecard"),
    ("mart_category_sales rows", "select count(*) from mart.mart_category_sales"),
    ("mart_category_sales sum revenue", "select sum(revenue) from mart.mart_category_sales"),
]
EXPECTED = os.path.join(os.path.dirname(os.path.abspath(__file__)), "expected_marts.json")


def connect(warehouse):
    if warehouse == "postgres":
        import psycopg2
        return psycopg2.connect(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5432"),
                                user=os.getenv("PGUSER", "dbt"), password=os.getenv("PGPASSWORD", "dbt"),
                                dbname=os.getenv("PGDATABASE", "ecom"))
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.getenv("SNOWFLAKE_USER", "DBT_USER"),
        authenticator="SNOWFLAKE_JWT", private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_key.p8"),
        role="DBT_ROLE", warehouse="ECOM_WH", database="ECOM")


def compute(warehouse):
    con = connect(warehouse)
    cur = con.cursor()
    out = {}
    for name, sql in CHECKS:
        cur.execute(sql)
        out[name] = float(cur.fetchone()[0])
    con.close()
    return out


if __name__ == "__main__":
    wh = sys.argv[sys.argv.index("--warehouse") + 1] if "--warehouse" in sys.argv else "postgres"
    got = compute(wh)
    if "--generate" in sys.argv:
        json.dump(got, open(EXPECTED, "w"), indent=2)
        print(f"wrote {len(got)} control totals from {wh} to {EXPECTED}")
        sys.exit(0)
    want = json.load(open(EXPECTED))
    bad = 0
    for name, _ in CHECKS:
        ok = math.isclose(got[name], want[name], rel_tol=1e-9, abs_tol=0.01)
        bad += not ok
        print(f"{'MATCH   ' if ok else 'MISMATCH'} {name:48s} {wh}={got[name]:,.2f}  postgres={want[name]:,.2f}")
    print(f"\n{len(CHECKS) - bad}/{len(CHECKS)} control totals match the PostgreSQL build")
    sys.exit(1 if bad else 0)
