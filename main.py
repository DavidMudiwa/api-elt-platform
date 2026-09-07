from dagster import Definitions
from assets.github import github_repositories_raw

defs = Definitions(
    assets=[github_repositories_raw],
    )