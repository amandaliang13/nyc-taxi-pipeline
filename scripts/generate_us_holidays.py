"""Generate dbt/nyc_taxi/seeds/us_holidays.csv for dim_date.is_holiday.

US federal holidays from the `holidays` package (dev dependency), on their actual
dates -- observed weekday substitutes are left out, matching the seed's convention.
The range is the dim_date spine, read from the dim_date_start / dim_date_end vars in
dbt_project.yml (end exclusive), so the two can't drift apart. Rerun after
changing those vars:

    uv run python scripts/generate_us_holidays.py
"""

import csv
from datetime import date, timedelta
from pathlib import Path

import holidays
import yaml

DBT_PROJECT = Path(__file__).resolve().parents[1] / "dbt/nyc_taxi"
SEED = DBT_PROJECT / "seeds/us_holidays.csv"


def _spine_bounds() -> tuple[date, date]:
    """Return the dim_date spine as inclusive (start, end) dates."""
    dbt_vars = yaml.safe_load((DBT_PROJECT / "dbt_project.yml").read_text())["vars"]
    start = date.fromisoformat(str(dbt_vars["dim_date_start"]))
    end_exclusive = date.fromisoformat(str(dbt_vars["dim_date_end"]))
    return start, end_exclusive - timedelta(days=1)


def main() -> None:
    start, end = _spine_bounds()
    calendar = holidays.US(years=range(start.year, end.year + 1), observed=False)
    rows = sorted((d, name) for d, name in calendar.items() if start <= d <= end)
    with SEED.open("w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
        f.write("holiday_date,holiday_name\n")
        for d, name in rows:
            f.write(f"{d.isoformat()},")
            writer.writerow([name])
    print(f"wrote {len(rows)} holidays ({start} .. {end}) to {SEED}")


if __name__ == "__main__":
    main()
