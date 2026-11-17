#!/usr/bin/env bash
# fetch -> load raw into Snowflake -> dbt run -> dbt test -> compare marts to the Postgres control totals.
# Output is saved to evidence/ so the run is documented after the 30-day trial account is suspended.
set -eo pipefail
: "${SNOWFLAKE_ACCOUNT:?set SNOWFLAKE_ACCOUNT, e.g. myorg-myaccount}"
mkdir -p evidence
{
  python scripts/fetch_data.py data
  python scripts/load_raw_snowflake.py data
  dbt run  --target snowflake --profiles-dir .
  dbt test --target snowflake --profiles-dir .
  python scripts/verify_marts.py --warehouse snowflake
} 2>&1 | tee evidence/snowflake_run.log
