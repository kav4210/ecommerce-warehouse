# E-Commerce Data Warehouse (dbt + PostgreSQL)

A star-schema warehouse built with **dbt on PostgreSQL 16** over the real **Olist Brazilian e-commerce dataset** (about 99K orders, Sept 2016 to Oct 2018). Raw CSVs are loaded untouched, dbt types and cleans them in staging, and the marts layer is a Kimball star schema with an SCD Type 2 customer dimension derived from real order history. 50 dbt tests, including reconciliation back to the raw source, were mutation-checked so they are known to fail when the pipeline is broken.

## Run it

```bash
docker compose up --build        # Postgres 16 + dbt: fetch -> load raw -> dbt run -> dbt test
```

Or without Docker (needs a local Postgres with a `dbt` user and `ecom` database):

```bash
pip install -r requirements.txt
sh scripts/run_all.sh
```

Full rebuild from an empty database, including the data download, takes about 20 seconds.

## Architecture

```
raw (8 tables, text, untouched COPY)
  -> staging (views: typing, trimming, dedup)
      -> marts (tables, indexed)
           dim_date   dim_product   dim_seller   dim_customer (SCD2)
           fct_orders (1 row per order)   fct_order_items (1 row per order line)
           mart_monthly_sales   mart_seller_scorecard   mart_category_sales   mart_cohort_retention
```

| Table | Rows |
|---|---|
| fct_orders | 99,441 |
| fct_order_items | 112,650 |
| dim_customer (versions) | 96,221 (96,096 people + unknown member) |
| dim_product | 32,952 |
| dim_seller | 3,096 |
| dim_date | 1,096 |

## Design decisions

- **Raw stays raw.** Everything lands as text so a bad value cannot fail the load. Casting happens in staging where dbt can test it.
- **SCD2 from real history, not a fake.** Olist gives every order its own `customer_id`, but `customer_unique_id` is the person. When one person's orders come from a different city or state over time, that is an observed move. Each consecutive run of the same city and state becomes one version with half-open validity `[valid_from, valid_to)`, and `fct_orders` joins to the version in effect at purchase time. It is derived from order history, not a CDC feed, so a customer who moved but never reordered is invisible.
- **Unknown members.** Dimensions carry an `unknown` row so an unmatched key degrades to a visible bucket instead of dropping fact rows.
- **One review per order.** 547 orders have multiple reviews. Staging keeps the most recently answered one so the order-grain fact cannot fan out.
- **Indexes via post-hooks.** The point-in-time join took over 4 minutes without indexes on the mart tables and under a second with them.
- **Revenue** excludes `canceled` and `unavailable` orders and excludes freight.

## Testing

50 dbt tests: 46 hard assertions and 4 monitors for known source defects.

- Keys, not-null, accepted values, and relationships on every staging and mart table.
- **Reconciliation to source:** fact revenue equals raw item revenue to the cent, fact payments equal raw payments, order count is unchanged by every join, and every order's SCD2 version carries the exact city and state that order was placed from.
- **SCD2 integrity:** no gaps or overlaps between versions, positive width, exactly one current version per person.
- **Known Olist defects, reported not hidden** (warn severity): 8 orders marked delivered with no delivery date, 775 orders with no items, 304 orders whose payments do not equal items plus freight.

**Mutation check.** I broke the pipeline on purpose to confirm the tests can fail: changing the SCD2 boundary from `<` to `<=` fails 6 tests (124 duplicated orders, one per extra version); removing review dedup fails the uniqueness and fan-out tests (547 duplicated orders); an inner join that silently drops items is caught only by the revenue reconciliation (2,033 of 112,650 items lost), which is why that test exists.

## What the data says

Descriptive findings on this dataset, not causal claims.

- Orders delivered late (after the estimated date) are **6.8%** of deliveries and average a **2.27** review score versus **4.29** for on-time orders.
- Only **3.0%** of customers order more than once. Month-1 retention for 2017 cohorts is about **0.5%**.
- The top 1% of sellers (31 of 3,053) generate **26.2%** of item revenue.
- November 2017 (Black Friday) is the volume peak at 7,421 orders; March 2018 has the worst late rate (19.0%) and the lowest review average (3.76) of 2018.
- Total item revenue is R$13.49M across 98,199 non-canceled orders with items.

## Verified vs not verified

- Verified: the full pipeline on PostgreSQL 16 and dbt 1.12.7 / dbt-postgres 1.9.1, from an empty database, with source row counts checked against the published dataset.
- Verified: `docker compose up --build` on Docker Desktop (Windows) ran fetch, load, `dbt run` (17 models) and `dbt test` (47 pass, 3 warn, 0 fail) and exited with code 0.
- Not run on a cloud warehouse. The dataset's geolocation table is unused and not loaded.

## Data source

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (CC BY-NC-SA 4.0). `scripts/fetch_data.py` pulls a GitHub mirror and fails loudly if any row count differs from the published numbers; download from Kaggle into `data/` if the mirror moves.
