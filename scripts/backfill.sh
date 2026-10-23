#!/usr/bin/env bash
# Manual backfill trigger for a month range.
#
#   scripts/backfill.sh START END      # both YYYY-MM, inclusive
#
# Triggers taxi_monthly_ingest once per month. The DAG's max_active_runs=1
# serializes the runs; this script only queues them and does not wait.
# Run id: manual__bf__YYYY-MM__<UTC stamp>. Airflow reserves backfill__ for its
# own runs; the stamp lets a month be re-backfilled without an id collision.
# Pure integer month math: works on macOS bash 3.2 / BSD date.
set -euo pipefail

DAG_ID="taxi_monthly_ingest"

usage() {
  echo "usage: $(basename "$0") START END   (YYYY-MM, START <= END)" >&2
  exit 2
}

# YYYY-MM -> months since year 0
to_index() {
  [[ "$1" =~ ^([0-9]{4})-(0[1-9]|1[0-2])$ ]] || {
    echo "invalid month: $1" >&2
    usage
  }
  echo $((10#${BASH_REMATCH[1]} * 12 + 10#${BASH_REMATCH[2]} - 1))
}

[[ $# -eq 2 ]] || usage
start=$(to_index "$1")
end=$(to_index "$2")
((start <= end)) || {
  echo "START ($1) is after END ($2)" >&2
  usage
}

stamp=$(date -u +%Y%m%dT%H%M%SZ)

for ((i = start; i <= end; i++)); do
  year=$((i / 12))
  month=$(printf "%02d" $((i % 12 + 1)))
  run_id="manual__bf__${year}-${month}__${stamp}"
  echo "triggering ${DAG_ID} for ${year}-${month} (run_id=${run_id})"
  docker compose run --rm airflow-cli dags trigger "${DAG_ID}" \
    --conf "{\"download_month\":\"${year}${month}\"}" \
    --run-id "${run_id}"
done
