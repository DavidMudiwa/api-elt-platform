from datetime import date, datetime

RAW_LAYER = "raw"
PROCESSED_LAYER = "processed"

REPOSITORIES = "repositories"
COMMITS = "commits"


def _repo_partition(repo: str) -> str:
    """Hive partition for a repo; flatten '/' so it doesn't create pseudo-folders."""
    return f"repo={repo.replace('/', '__')}"


def raw_key(
    dataset: str,
    run_time: datetime,
    repo: str | None = None,
) -> str:
    """Immutable, timestamped raw object key (append-only)."""
    parts = [RAW_LAYER, dataset]
    if repo:
        parts.append(_repo_partition(repo))
    parts.append(f"dt={run_time:%Y-%m-%d}")
    parts.append(f"{dataset}_{run_time:%Y%m%d_%H%M%S}.json")
    return "/".join(parts)


def processed_key(
    dataset: str,
    partition_date: date,
    repo: str | None = None,
    extension: str = "parquet",
) -> str:
    """Deterministic, overwrite-safe processed object key (idempotent)."""
    parts = [PROCESSED_LAYER, dataset]
    if repo:
        parts.append(_repo_partition(repo))
    parts.append(f"dt={partition_date:%Y-%m-%d}")
    parts.append(f"{dataset}.{extension}")
    return "/".join(parts)
