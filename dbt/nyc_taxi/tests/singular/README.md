# tests/singular/

- `assert_trip_zone_version_covers_pickup.sql`: every `fact_trips` zone version key
  exists in `dim_zones` and its `[valid_from, valid_to)` covers the trip's pickup
  time. It is the only test that catches a key pointing at the wrong version (see
  the README runbook for CRZ seed changes).

Still to come (Phase 6): one .sql test per DQ rule (architecture.md §3) — negative
fares, bad timestamps, zero-distance nonzero-fare, duration > 24h, unknown location,
pickup outside the source month.
