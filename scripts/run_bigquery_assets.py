import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

BUCKET = "github-data-492709"
PROJECT = "kestra-sandbox-492709"
DATASET = "github"
PARTITION = os.environ.get("PARTITION", date.today().isoformat())

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))

from dagster import InMemoryIOManager, materialize  # noqa: E402

from assets.bigquery import github_commits_bq, github_repositories_bq  # noqa: E402
from assets.github import (  # noqa: E402
    github_commits_raw,
    github_repositories_raw,
)
from assets.processed import (  # noqa: E402
    github_commits_parquet,
    github_repositories_parquet,
)
from resources.bigquery import BigQueryResource  # noqa: E402
from resources.gcs import GCSResource  # noqa: E402


def print_result(result) -> None:
    for event in result.get_asset_materialization_events():
        materialization = event.event_specific_data.materialization
        print(f"asset : {materialization.asset_key.to_user_string()}")
        for key, value in materialization.metadata.items():
            if key == "path":
                continue
            print(f"  {key} = {value.value}")


def main() -> None:
    resources = {
        "gcs": GCSResource(bucket=BUCKET, project=PROJECT),
        "bigquery": BigQueryResource(project=PROJECT, dataset=DATASET),
        "io_manager": InMemoryIOManager(),
    }

    repositories = materialize(
        [
            github_repositories_raw,
            github_repositories_parquet,
            github_repositories_bq,
        ],
        resources=resources,
    )
    assert repositories.success
    print_result(repositories)

    commits = materialize(
        [github_commits_raw, github_commits_parquet, github_commits_bq],
        partition_key=PARTITION,
        resources=resources,
    )
    assert commits.success
    print_result(commits)


if __name__ == "__main__":
    main()
