"""Independent PySpark re-implementation of the warehouse, built from the RAW Olist CSVs.
It recomputes the same 27 control totals that scripts/verify_marts.py checks on PostgreSQL and Snowflake
(row counts, revenue, payment and freight sums, late orders, SCD2 versions, cohort retention, ...) and compares
them to scripts/expected_marts.json. It shares no SQL and no code with the dbt models, so agreement is evidence
that the star schema, the SCD Type 2 logic and the point-in-time join are right, not just self-consistent."""
import json, math, os, sys
from pyspark.sql import SparkSession, Window, functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
EXPECTED = os.path.join(HERE, "..", "scripts", "expected_marts.json")
TS = "yyyy-MM-dd HH:mm:ss"
UNKNOWN_FROM, UNKNOWN_TO, OPEN_END = "1900-01-01 00:00:00", "9999-12-31 00:00:00", "9999-12-31 00:00:00"


def spark_session():
    return (SparkSession.builder.master("local[4]").appName("olist-spark-parity")
            .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


def read(spark, d, name):
    # multiLine + quote/escape: review comments contain quoted newlines. Everything stays text, like raw.*
    return (spark.read.option("header", True).option("multiLine", True).option("quote", '"')
            .option("escape", '"').csv(os.path.join(d, name + ".csv")))


def ts(c):
    return F.to_timestamp(F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)), TS)


def build(spark, d):
    o = read(spark, d, "olist_orders_dataset").select(
        "order_id", "customer_id", F.col("order_status").alias("status"),
        ts("order_purchase_timestamp").alias("purchase_ts"), ts("order_delivered_customer_date").alias("delivered_ts"),
        ts("order_estimated_delivery_date").alias("estimated_ts"))
    c = read(spark, d, "olist_customers_dataset").select(
        "customer_id", "customer_unique_id", F.lower(F.trim("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"))
    items = read(spark, d, "olist_order_items_dataset").select(
        "order_id", F.col("order_item_id").cast("int").alias("item_seq"), "product_id", "seller_id",
        F.col("price").cast("decimal(12,2)").alias("price"), F.col("freight_value").cast("decimal(12,2)").alias("freight_value"))
    pay = read(spark, d, "olist_order_payments_dataset").select(
        "order_id", "payment_type", F.col("payment_value").cast("decimal(12,2)").alias("payment_value"))
    rv = read(spark, d, "olist_order_reviews_dataset").select(
        "order_id", "review_id", F.col("review_score").cast("int").alias("review_score"),
        ts("review_answer_timestamp").alias("answered_ts"))
    rv = (rv.withColumn("rn", F.row_number().over(Window.partitionBy("order_id").orderBy(
        F.col("answered_ts").desc_nulls_last(), F.col("review_id").desc()))).filter("rn = 1").drop("rn"))
    prod = read(spark, d, "olist_products_dataset")
    trans = read(spark, d, "product_category_name_translation")
    products = (prod.join(trans, prod.product_category_name == trans.product_category_name, "left")
                .select(prod.product_id, F.coalesce(trans.product_category_name_english,
                        F.when(F.trim(prod.product_category_name) == "", None).otherwise(prod.product_category_name),
                        F.lit("unknown")).alias("category")))
    sellers = read(spark, d, "olist_sellers_dataset").select("seller_id")

    # dim_customer: SCD Type 2. A new version starts whenever a person's city+state differs from their previous order.
    oc = o.join(c, "customer_id")
    w = Window.partitionBy("customer_unique_id").orderBy("purchase_ts", "order_id")
    m = (oc.withColumn("pc", F.lag("city").over(w)).withColumn("ps", F.lag("state").over(w))
         .withColumn("new_run", F.when(F.col("pc").isNull() | (F.col("city") != F.col("pc")) | (F.col("state") != F.col("ps")), 1).otherwise(0))
         .withColumn("run_id", F.sum("new_run").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow))))
    wr = Window.partitionBy("customer_unique_id", "run_id").orderBy("purchase_ts", "order_id")
    m = m.withColumn("rn", F.row_number().over(wr))
    v = (m.groupBy("customer_unique_id", "run_id")
         .agg(F.min("purchase_ts").alias("valid_from"), F.max(F.when(F.col("rn") == 1, F.col("city"))).alias("city")))
    wv = Window.partitionBy("customer_unique_id").orderBy("valid_from", "run_id")
    nxt = F.lead("valid_from").over(wv)
    dim_c = v.select(F.md5(F.concat_ws("|", "customer_unique_id", F.col("run_id").cast("string"))).alias("customer_key"),
                     "customer_unique_id", "valid_from",
                     F.coalesce(nxt, F.to_timestamp(F.lit(OPEN_END))).alias("valid_to"), nxt.isNull().alias("is_current"))
    unknown = spark.createDataFrame([("unknown", "unknown", UNKNOWN_FROM, UNKNOWN_TO, True)],
                                    "customer_key string, customer_unique_id string, vf string, vt string, is_current boolean"
                                    ).select("customer_key", "customer_unique_id", F.to_timestamp("vf").alias("valid_from"),
                                             F.to_timestamp("vt").alias("valid_to"), "is_current")
    dim_customer = dim_c.unionByName(unknown)

    # fct_orders: point-in-time join to the customer version valid at purchase time (half-open [valid_from, valid_to))
    it = items.groupBy("order_id").agg(F.count("*").alias("item_count"), F.sum("price").alias("items_value"),
                                       F.sum("freight_value").alias("freight_value"))
    pt = pay.groupBy("order_id").agg(F.sum("payment_value").alias("payment_value"),
                                     F.size(F.collect_set("payment_type")).alias("n_types"))
    d_ver = dim_c.select(F.col("customer_unique_id").alias("d_uid"), "customer_key", "valid_from", "valid_to")
    f = (oc.select("order_id", "customer_unique_id", "status", "purchase_ts", "delivered_ts", "estimated_ts")
         .join(d_ver, (F.col("customer_unique_id") == F.col("d_uid")) & (F.col("purchase_ts") >= F.col("valid_from"))
               & (F.col("purchase_ts") < F.col("valid_to")), "left")
         .join(it, "order_id", "left").join(pt, "order_id", "left").join(rv.select("order_id", "review_score"), "order_id", "left")
         .select("order_id", F.coalesce("customer_key", F.lit("unknown")).alias("customer_key"), "status", "purchase_ts",
                 F.coalesce("item_count", F.lit(0)).alias("item_count"),
                 F.coalesce("items_value", F.lit(0)).alias("items_value"),
                 F.coalesce("freight_value", F.lit(0)).alias("freight_value"),
                 F.coalesce("payment_value", F.lit(0)).alias("payment_value"), "n_types",
                 F.col("delivered_ts").isNotNull().alias("is_delivered"),
                 F.when(F.col("delivered_ts").isNotNull(),
                        (F.unix_timestamp("delivered_ts") - F.unix_timestamp("purchase_ts")) / F.lit(86400.0)).alias("delivery_days"),
                 F.when(F.col("delivered_ts").isNotNull() & F.col("estimated_ts").isNotNull(),
                        F.to_date("delivered_ts") > F.to_date("estimated_ts")).alias("is_late")))
    f = f.cache()

    dim_p_rows = products.count() + 1
    dim_s = sellers.select(F.col("seller_id").alias("seller_key")).unionByName(
        spark.createDataFrame([("unknown",)], "seller_key string"))
    dim_p = products.select(F.col("product_id").alias("product_key"), "category").unionByName(
        spark.createDataFrame([("unknown", "unknown")], "product_key string, category string"))
    fi = (items.join(f.select("order_id", F.col("status").alias("st")), "order_id")
          .join(dim_p.select(F.col("product_key").alias("pk")), items.product_id == F.col("pk"), "left")
          .join(dim_s.select(F.col("seller_key").alias("sk")), items.seller_id == F.col("sk"), "left")
          .select("order_id", "st", F.coalesce("pk", F.lit("unknown")).alias("product_key"),
                  F.coalesce("sk", F.lit("unknown")).alias("seller_key"), "price", "freight_value").cache())
    return dict(f=f, fi=fi, dim_customer=dim_customer, dim_p_rows=dim_p_rows, dim_s_rows=sellers.count() + 1, dim_p=dim_p)


def num(df, expr):
    return float(df.agg(expr.alias("x")).collect()[0]["x"])


def compute(spark, d):
    t = build(spark, d)
    f, fi, dc = t["f"], t["fi"], t["dim_customer"]
    days = spark.sql("select explode(sequence(to_date('2016-01-01'), to_date('2018-12-31'), interval 1 day)) as d")
    ok = F.col("status").isin("canceled", "unavailable") == False  # noqa: E712
    monthly = (f.filter(ok & (F.col("item_count") > 0)).groupBy(F.date_format("purchase_ts", "yyyy-MM").alias("ym"))
               .agg(F.count("*").alias("orders"), F.sum("items_value").alias("rev")))
    first = (f.join(dc.select("customer_key", "customer_unique_id"), "customer_key").filter(ok)
             .select("customer_unique_id", F.trunc("purchase_ts", "month").alias("m")).cache())
    cohort0 = first.groupBy("customer_unique_id").agg(F.min("m").alias("cm"))
    act = (first.join(cohort0, "customer_unique_id")
           .withColumn("off", ((F.year("m") - F.year("cm")) * 12 + (F.month("m") - F.month("cm"))).cast("int"))
           .groupBy("cm", "off").agg(F.countDistinct("customer_unique_id").alias("active")))
    fio = fi.filter(F.col("st").isin("canceled", "unavailable") == False)  # noqa: E712
    sellers = fio.groupBy("seller_key").agg(F.sum("price").alias("revenue"))
    cats = fio.join(t["dim_p"].select(F.col("product_key").alias("p2"), "category"), fio.product_key == F.col("p2")) \
              .groupBy("category").agg(F.sum("price").alias("revenue"))
    return {
        "dim_customer rows": float(dc.count()),
        "dim_customer current versions": num(dc, F.sum(F.col("is_current").cast("int"))),
        "dim_date rows": float(days.count()),
        "dim_date weekend days": float(days.filter(F.dayofweek("d").isin(1, 7)).count()),
        "dim_product rows": float(t["dim_p_rows"]),
        "dim_seller rows": float(t["dim_s_rows"]),
        "fct_orders rows": float(f.count()),
        "fct_orders sum payment_value": num(f, F.sum("payment_value")),
        "fct_orders sum items_value": num(f, F.sum("items_value")),
        "fct_orders sum freight_value": num(f, F.sum("freight_value")),
        "fct_orders late orders": num(f, F.sum(F.col("is_late").cast("int"))),
        "fct_orders delivered orders": num(f, F.sum(F.col("is_delivered").cast("int"))),
        "fct_orders sum floor(delivery_days)": num(f, F.sum(F.floor("delivery_days"))),
        "fct_orders orders with a payment_types value": float(f.filter(F.col("n_types") > 0).count()),
        "fct_order_items rows": float(fi.count()),
        "fct_order_items sum price": num(fi, F.sum("price")),
        "fct_order_items sum line_total": num(fi, F.sum(F.col("price") + F.col("freight_value"))),
        "mart_monthly_sales rows": float(monthly.count()),
        "mart_monthly_sales sum orders": num(monthly, F.sum("orders")),
        "mart_monthly_sales sum items_revenue": num(monthly, F.sum("rev")),
        "mart_cohort_retention rows": float(act.count()),
        "mart_cohort_retention sum active": num(act, F.sum("active")),
        "mart_cohort_retention max month_offset": num(act, F.max("off")),
        "mart_seller_scorecard rows": float(sellers.count()),
        "mart_seller_scorecard sum revenue": num(sellers, F.sum("revenue")),
        "mart_category_sales rows": float(cats.count()),
        "mart_category_sales sum revenue": num(cats, F.sum("revenue")),
    }


if __name__ == "__main__":
    data = sys.argv[1] if len(sys.argv) > 1 else "data"
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    got = compute(spark, data)
    want = json.load(open(EXPECTED))
    bad = 0
    for name, w in want.items():
        okv = math.isclose(got[name], w, rel_tol=1e-9, abs_tol=0.01)
        bad += not okv
        print(f"{'MATCH   ' if okv else 'MISMATCH'} {name:48s} spark={got[name]:,.2f}  postgres={w:,.2f}")
    print(f"\n{len(want) - bad}/{len(want)} control totals computed by PySpark from the raw CSVs match the PostgreSQL build")
    spark.stop()
    sys.exit(1 if bad else 0)
