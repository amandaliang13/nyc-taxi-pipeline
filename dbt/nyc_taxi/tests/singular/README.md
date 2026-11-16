# tests/singular/

- `assert_trip_zone_version_covers_pickup.sql`: every `fact_trips` zone version key
  exists in `dim_zones` and its `[valid_from, valid_to)` covers the trip's pickup
  time. It is the only test that catches a key pointing at the wrong version (see
  the README runbook for CRZ seed changes).
- `assert_dim_zones_one_current_version.sql`: every lookup zone's history starts at
  the 1900-01-01 baseline and has exactly one current version. Together with
  `mutually_exclusive_ranges` on `dim_zones`, every pickup time maps to exactly one
  version.

Still to come (Phase 6): DQ flag consistency on `fact_trips` (the rule logic itself is
unit-tested on `stg_trips`), schema drift boundaries, and the `fact_trips` ↔
`control.manifest` reconciliation.
