#!/usr/bin/env sh
# fetch -> load raw -> dbt run -> dbt test. Exits non-zero if any hard test fails (warn-level tests do not fail the run).
set -e
python scripts/fetch_data.py data
python scripts/load_raw.py data
dbt run  --profiles-dir .
dbt test --profiles-dir .
