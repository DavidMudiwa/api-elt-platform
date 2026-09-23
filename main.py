from dagster import Definitions, InMemoryIOManager

from assets.bigquery import github_commits_bq, github_repositories_bq
from assets.github import github_commits_raw, github_repositories_raw
from assets.processed import github_commits_parquet, github_repositories_parquet
from resources.bigquery import BigQueryResource
from resources.gcs import GCSResource

defs = Definitions(
    assets=[
        github_repositories_raw,
        github_commits_raw,
        github_repositories_parquet,
        github_commits_parquet,
        github_repositories_bq,
        github_commits_bq,
    ],
    resources={
        "gcs": GCSResource(),
        "bigquery": BigQueryResource(),
        "io_manager": InMemoryIOManager(),
    },
)
