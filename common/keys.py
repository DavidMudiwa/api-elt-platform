import re
from datetime import date, datetime

RAW_LAYER = "raw"
PROCESSED_LAYER = "processed"

REPOSITORIES = "repositories"
COMMITS = "commits"

_DT_PATTERN = re.compile(r"dt=(\d{4})-(\d{2})-(\d{2})")


def repo_partition(repo: str) -> str:
    """Hive partition for a repo; flatten '/' so it doesn't create pseudo-folders."""
    return f"repo={repo.replace('/', '__')}"


def raw_prefix(dataset: str, repo: str | None = None) -> str:
    """Prefix (with trailing slash) under which a dataset's raw objects live."""
    parts = [RAW_LAYER, dataset]
    if repo:
        parts.append(repo_partition(repo))
    return "/".join(parts) + "/"


def raw_key(
    dataset: str,
    run_time: datetime,
    repo: str | None = None,
) -> str:
    """Immutable, timestamped raw object key (append-only)."""
    return (
        f"{raw_prefix(dataset, repo)}"
        f"dt={run_time:%Y-%m-%d}/{dataset}_{run_time:%Y%m%d_%H%M%S}.json"
    )


def processed_prefix(dataset: str, repo: str | None = None) -> str:
    """Prefix (with trailing slash) under which a dataset's processed objects live."""
    parts = [PROCESSED_LAYER, dataset]
    if repo:
        parts.append(repo_partition(repo))
    return "/".join(parts) + "/"


def processed_key(
    dataset: str,
    partition_date: date,
    repo: str | None = None,
    extension: str = "parquet",
) -> str:
    """Deterministic, overwrite-safe processed object key (idempotent)."""
    return (
        f"{processed_prefix(dataset, repo)}"
        f"dt={partition_date:%Y-%m-%d}/{dataset}.{extension}"
    )


def partition_date_from_key(key: str) -> date:
    """Extract the `dt=YYYY-MM-DD` partition from an object key."""
    match = _DT_PATTERN.search(key)
    if match is None:
        raise ValueError(f"No dt=YYYY-MM-DD partition found in key: {key!r}")
    year, month, day = (int(part) for part in match.groups())
    return date(year, month, day)
