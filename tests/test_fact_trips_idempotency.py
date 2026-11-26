"""
Validates pipeline *behavior* for dbt's fact_trips (incremental by source_month):
  - an out-of-order backfill (month N before N+2) leaves the later month untouched
  - a rerun with no new load rebuilds nothing
  - reloading a month replaces only that month
  - a month whose loaded_at moved back still rebuilds ("is distinct from")
  - fact_rebuild_from rebuilds that month onward; a malformed value fails to compile

"Untouched" is checked with Postgres's xmin, the transaction that wrote the row
version: an unchanged xmin means the rows weren't rewritten. The ingestion layer's
side lives in test_idempotency.py.
"""

import pytest
from dbt.cli.main import dbtRunnerResult
from sqlalchemy import text

pytestmark = pytest.mark.dbt

RECONCILE_TEST = "assert_fact_reconciles_with_manifest"


def _dbt_error_text(result: dbtRunnerResult) -> str:
    """Every error message of a dbt invocation, whether raised or reported per node."""
    parts = [str(result.exception)] if result.exception else []
    parts += [r.message or "" for r in getattr(result.result, "results", [])]
    return "\n".join(parts)


def _test_status(result: dbtRunnerResult, name: str) -> str:
    """Status ('pass', 'fail', 'error', ...) of the dbt node ``name`` in ``result``."""
    statuses = {
        r.node.name: str(r.status) for r in getattr(result.result, "results", [])
    }
    assert name in statuses, f"{name!r} not in dbt results {sorted(statuses)}: " + (
        _dbt_error_text(result)
    )
    return statuses[name]


@pytest.fixture
def build_fact(run_dbt):
    """Seed once, then return ``build(*extra)``: dbt run of fact_trips and its parents."""
    seeded = run_dbt("seed")
    assert seeded.success, _dbt_error_text(seeded)

    def _build(*extra: str) -> None:
        result = run_dbt("run", "--select", "+fact_trips", *extra)
        assert result.success, _dbt_error_text(result)

    return _build


def _reconciliation(run_dbt) -> str:
    return _test_status(run_dbt("test", "--select", RECONCILE_TEST), RECONCILE_TEST)


def _fact_xmins(engine) -> dict[str, frozenset[str]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_month, array_agg(DISTINCT xmin::text) "
                "FROM marts.fact_trips GROUP BY source_month"
            )
        )
        return {month: frozenset(xmins) for month, xmins in rows}


def _fact_counts(engine) -> dict[str, int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_month, count(*) FROM marts.fact_trips GROUP BY source_month"
            )
        )
        return dict(rows.all())


def test_fact_out_of_order_backfill_leaves_later_month_untouched(
    engine, put_month, ingest, build_fact, run_dbt
):
    put_month("2024-03", rows=3)
    put_month("2024-05", rows=4)

    ingest("2024-05")
    build_fact()
    later_month = _fact_xmins(engine)["2024-05"]
    ingest("2024-03")
    build_fact()

    assert _fact_xmins(engine)["2024-05"] == later_month
    assert _fact_counts(engine) == {"2024-03": 3, "2024-05": 4}
    assert _reconciliation(run_dbt) == "pass"

    # Nothing changed since the last build, so a rerun rebuilds nothing.
    before = _fact_xmins(engine)
    build_fact()
    assert _fact_xmins(engine) == before


def test_fact_reload_replaces_only_that_month(
    engine, put_month, ingest, build_fact, run_dbt
):
    put_month("2024-03", rows=3)
    put_month("2024-05", rows=4)
    ingest("2024-03")
    ingest("2024-05")
    build_fact()
    before = _fact_xmins(engine)

    put_month("2024-05", rows=6)  # TLC republishes 2024-05
    ingest("2024-05")
    build_fact()

    after = _fact_xmins(engine)
    assert after["2024-03"] == before["2024-03"]
    assert after["2024-05"].isdisjoint(before["2024-05"])
    assert _fact_counts(engine) == {"2024-03": 3, "2024-05": 6}
    assert _reconciliation(run_dbt) == "pass"


def test_fact_rebuilds_month_whose_loaded_at_moved_back(
    engine, put_month, ingest, build_fact, run_dbt
):
    # Regression test for "is distinct from": a reload whose loaded_at is earlier
    # than the one the month was built from (clock step back, manifest restored by
    # hand) must still rebuild. A "newer than" check would skip it.
    put_month("2024-03", rows=3)
    put_month("2024-05", rows=4)
    ingest("2024-03")
    ingest("2024-05")
    build_fact()
    before = _fact_xmins(engine)

    with engine.begin() as conn:
        stepped_back = conn.execute(
            text(
                "UPDATE control.manifest SET loaded_at = loaded_at - interval '1 hour' "
                "WHERE source_month = '2024-05' RETURNING loaded_at"
            )
        ).scalar_one()
    assert _reconciliation(run_dbt) == "fail"  # 2024-05 is stale

    build_fact()

    after = _fact_xmins(engine)
    assert after["2024-03"] == before["2024-03"]
    assert after["2024-05"].isdisjoint(before["2024-05"])
    with engine.connect() as conn:
        built_from = conn.execute(
            text(
                "SELECT DISTINCT raw_loaded_at FROM marts.fact_trips "
                "WHERE source_month = '2024-05'"
            )
        ).scalars()
        assert list(built_from) == [stepped_back]
    assert _reconciliation(run_dbt) == "pass"


def test_fact_rebuild_from_rebuilds_that_month_onward(
    engine, put_month, ingest, build_fact, run_dbt
):
    for month in ("2024-03", "2024-05", "2024-07"):
        put_month(month, rows=3)
        ingest(month)
    build_fact()
    before = _fact_xmins(engine)

    build_fact("--vars", '{fact_rebuild_from: "2024-05"}')

    after = _fact_xmins(engine)
    assert after["2024-03"] == before["2024-03"]
    assert after["2024-05"].isdisjoint(before["2024-05"])
    assert after["2024-07"].isdisjoint(before["2024-07"])
    assert _fact_counts(engine) == {"2024-03": 3, "2024-05": 3, "2024-07": 3}
    assert _reconciliation(run_dbt) == "pass"


def test_fact_rebuild_from_malformed_fails_to_compile(
    engine, put_month, ingest, build_fact, run_dbt
):
    put_month("2024-03", rows=3)
    ingest("2024-03")
    build_fact()
    before = _fact_xmins(engine)

    # Not zero-padded: as a string, '2024-5' > '2024-12', so it would pick wrong months.
    result = run_dbt(
        "run", "--select", "fact_trips", "--vars", '{fact_rebuild_from: "2024-5"}'
    )

    assert not result.success
    assert "fact_rebuild_from must be 'YYYY-MM'" in _dbt_error_text(result)
    assert _fact_xmins(engine) == before
