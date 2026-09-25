import os
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

BUCKET = "github-data-492709"
PROJECT = "kestra-sandbox-492709"
DATASET = "github"

MAX_PARTITIONS = 5

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))

from dagster import InMemoryIOManager, materialize  # noqa: E402

from assets.bigquery import github_commits_bq  # noqa: E402
from assets.github import github_commits_raw  # noqa: E402
from assets.processed import github_commits_parquet  # noqa: E402
from resources.bigquery import BigQueryResource  # noqa: E402
from resources.gcs import GCSResource  # noqa: E402


def date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def main() -> None:
    start = date.fromisoformat(os.environ.get("BACKFILL_START", "2026-09-21"))
    end = date.fromisoformat(os.environ.get("BACKFILL_END", "2026-09-22"))
    partitions = date_range(start, end)

    if len(partitions) > MAX_PARTITIONS:
        raise SystemExit(
            f"Refusing to backfill {len(partitions)} partitions "
            f"(cap {MAX_PARTITIONS}) to respect the 60 req/hr GitHub limit"
        )

    resources = {
        "gcs": GCSResource(bucket=BUCKET, project=PROJECT),
        "bigquery": BigQueryResource(project=PROJECT, dataset=DATASET),
        "io_manager": InMemoryIOManager(),
    }

    summary: list[tuple[str, object]] = []
    for partition in partitions:
        key = partition.isoformat()
        result = materialize(
            [github_commits_raw, github_commits_parquet, github_commits_bq],
            partition_key=key,
            resources=resources,
        )
        assert result.success

        rows = None
        for event in result.get_asset_materialization_events():
            materialization = event.event_specific_data.materialization
            if materialization.asset_key.to_user_string() == "github_commits_bq":
                rows = materialization.metadata["rows"].value

        summary.append((key, rows))
        print(f"partition {key}: loaded {rows} rows")

    print("backfill summary:")
    for key, rows in summary:
        print(f"  {key}: {rows} rows")


if __name__ == "__main__":
    main()
