# tests/singular/

- `assert_trip_zone_version_covers_pickup.sql`: every `fact_trips` zone version key
  exists in `dim_zones` and its `[valid_from, valid_to)` covers the trip's pickup
  time. It is the only test that catches a key pointing at the wrong version (see
  the README runbook for CRZ seed changes).
- `assert_dim_zones_one_current_version.sql`: every lookup zone's history starts at
  the 1900-01-01 baseline and has exactly one current version. Together with
  `mutually_exclusive_ranges` on `dim_zones`, every pickup time maps to exactly one
  version.

- `assert_dq_flags_match_rules.sql`: re-derives every DQ rule from `fact_trips`' own
  columns and checks the stored flags and `is_valid_trip` against them, in one scan.
  Catches months built before a rule or threshold change (see the README runbook).
  The rule logic itself, boundaries included, is unit-tested in
  `models/staging/stg_trips_unit_tests.yml`.

- `assert_schema_drift_boundaries.sql`: per `source_month`, `cbd_congestion_fee` has
  no values before 2025-01 and is not all NULL in any month from 2025-01 on; the same
  for `request_source` at 2026-06. The `cbd_congestion_fee` half is the
  architecture.md §6 "mid-year gap" check. One scan of `fact_trips`.
- `assert_fact_reconciles_with_manifest.sql`: per `source_month`, `fact_trips` holds
  exactly the `row_count` in `control.manifest` (yellow), built from the manifest's
  current `loaded_at`. A month never built, stale (reloaded, whatever its count) or
  orphaned fails as a count mismatch.
