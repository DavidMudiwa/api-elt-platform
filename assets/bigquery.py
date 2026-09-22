from dagster import AssetExecutionContext, Config, RetryPolicy, asset
from common.keys import (
    COMMITS,
    REPOSITORIES,
    partition_date_from_key,
    processed_prefix,
)
from common.schemas import (
    COMMITS_CLUSTERING_FIELDS,
    COMMITS_PARTITION_FIELD,
    COMMITS_SCHEMA,
    REPOSITORIES_PARTITION_FIELD,
    REPOSITORIES_SCHEMA,
)
from resources.bigquery import BigQueryResource
from resources.gcs import GCSResource


class CommitsBigQueryConfig(Config):
    """Which repository's processed commits to load."""

    repo: str = "dagster-io/dagster"


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def github_repositories_bq(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
) -> str:
    """Load the newest processed repositories Parquet into BigQuery."""

    source_key = gcs.find_latest_key(processed_prefix(REPOSITORIES))
    partition = partition_date_from_key(source_key)
    source_uri = gcs.object_uri(source_key)

    rows = bigquery.load_parquet(
        table=REPOSITORIES,
        source_uri=source_uri,
        schema=REPOSITORIES_SCHEMA,
        partition_field=REPOSITORIES_PARTITION_FIELD,
        partition_date=partition,
    )

    table_id = bigquery.table_id(REPOSITORIES)
    context.add_output_metadata(
        {
            "rows": rows,
            "table": table_id,
            "source_parquet": source_uri,
        }
    )
    context.log.info(f"Loaded {rows} rows into {table_id}")

    return table_id


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def github_commits_bq(
    context: AssetExecutionContext,
    config: CommitsBigQueryConfig,
    gcs: GCSResource,
    bigquery: BigQueryResource,
) -> str:
    """Load the newest processed commits Parquet for a repo into BigQuery."""

    source_key = gcs.find_latest_key(processed_prefix(COMMITS, repo=config.repo))
    partition = partition_date_from_key(source_key)
    source_uri = gcs.object_uri(source_key)

    rows = bigquery.load_parquet(
        table=COMMITS,
        source_uri=source_uri,
        schema=COMMITS_SCHEMA,
        partition_field=COMMITS_PARTITION_FIELD,
        partition_date=partition,
        clustering_fields=COMMITS_CLUSTERING_FIELDS,
    )

    table_id = bigquery.table_id(COMMITS)
    context.add_output_metadata(
        {
            "rows": rows,
            "repo": config.repo,
            "table": table_id,
            "source_parquet": source_uri,
        }
    )
    context.log.info(f"Loaded {rows} rows into {table_id}")

    return table_id
