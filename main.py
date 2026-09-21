from dagster import Definitions
from assets.github import github_repositories_raw, github_commits_raw
from resources.gcs import GCSResource

defs = Definitions(
    assets=[github_repositories_raw, github_commits_raw],
    resources={"gcs": GCSResource()},
)