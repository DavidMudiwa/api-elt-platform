from dagster import AssetExecutionContext, Config, RetryPolicy, asset

from common.keys import (
    COMMITS,
    REPOSITORIES,
    partition_date_from_key,
    processed_key,
    raw_prefix,
)
from common.normalize import normalize_commit, normalize_repository
from common.parquet import records_to_parquet_bytes
from resources.gcs import GCSResource

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"


class CommitsProcessedConfig(Config):
    """Which repository's raw commits to convert to Parquet."""

    repo: str = "dagster-io/dagster"


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def github_repositories_parquet(
    context: AssetExecutionContext,
    gcs: GCSResource,
) -> str:
    """Convert the newest raw repositories JSON into Parquet in GCS."""

    source_key = gcs.find_latest_key(raw_prefix(REPOSITORIES))
    raw_records = gcs.download_json(source_key)
    partition = partition_date_from_key(source_key)
    records = [normalize_repository(record, partition) for record in raw_records]

    target_key = processed_key(REPOSITORIES, partition)
    parquet_bytes = records_to_parquet_bytes(records)
    uri = gcs.upload_bytes(
        target_key, parquet_bytes, content_type=PARQUET_CONTENT_TYPE
    )

    context.add_output_metadata(
        {
            "records": len(records),
            "source_raw": source_key,
            "gcs_uri": uri,
            "size_bytes": len(parquet_bytes),
        }
    )
    context.log.info(f"Converted {len(records)} repositories -> {uri}")

    return uri


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def github_commits_parquet(
    context: AssetExecutionContext,
    config: CommitsProcessedConfig,
    gcs: GCSResource,
) -> str:
    """Convert the newest raw commits JSON for a repo into Parquet in GCS."""

    source_key = gcs.find_latest_key(raw_prefix(COMMITS, repo=config.repo))
    raw_records = gcs.download_json(source_key)
    partition = partition_date_from_key(source_key)
    records = [
        normalize_commit(record, config.repo, partition) for record in raw_records
    ]

    target_key = processed_key(COMMITS, partition, repo=config.repo)
    parquet_bytes = records_to_parquet_bytes(records)
    uri = gcs.upload_bytes(
        target_key, parquet_bytes, content_type=PARQUET_CONTENT_TYPE
    )

    context.add_output_metadata(
        {
            "records": len(records),
            "repo": config.repo,
            "source_raw": source_key,
            "gcs_uri": uri,
            "size_bytes": len(parquet_bytes),
        }
    )
    context.log.info(f"Converted {len(records)} commits -> {uri}")

    return uri
