from dagster import AssetExecutionContext, AssetIn, RetryPolicy, asset

from common.keys import COMMITS, REPOSITORIES, partition_date_from_key
from common.schemas import (
    COMMITS_CLUSTERING_FIELDS,
    COMMITS_PARTITION_FIELD,
    COMMITS_SCHEMA,
    REPOSITORIES_PARTITION_FIELD,
    REPOSITORIES_SCHEMA,
)
from resources.bigquery import BigQueryResource
from resources.gcs import GCSResource


@asset(
    ins={"parquet_uri": AssetIn("github_repositories_parquet")},
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def github_repositories_bq(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
    parquet_uri: str,
) -> str:
    """Load the processed repositories Parquet into BigQuery."""

    partition = partition_date_from_key(gcs.key_from_uri(parquet_uri))

    rows = bigquery.load_parquet(
        table=REPOSITORIES,
        source_uri=parquet_uri,
        schema=REPOSITORIES_SCHEMA,
        partition_field=REPOSITORIES_PARTITION_FIELD,
        partition_date=partition,
    )

    table_id = bigquery.table_id(REPOSITORIES)
    context.add_output_metadata(
        {
            "rows": rows,
            "table": table_id,
            "source_parquet": parquet_uri,
        }
    )
    context.log.info(f"Loaded {rows} rows into {table_id}")

    return table_id


@asset(
    ins={"parquet_uri": AssetIn("github_commits_parquet")},
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def github_commits_bq(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
    parquet_uri: str,
) -> str:
    """Load the processed commits Parquet into BigQuery."""

    partition = partition_date_from_key(gcs.key_from_uri(parquet_uri))

    rows = bigquery.load_parquet(
        table=COMMITS,
        source_uri=parquet_uri,
        schema=COMMITS_SCHEMA,
        partition_field=COMMITS_PARTITION_FIELD,
        partition_date=partition,
        clustering_fields=COMMITS_CLUSTERING_FIELDS,
    )

    table_id = bigquery.table_id(COMMITS)
    context.add_output_metadata(
        {
            "rows": rows,
            "table": table_id,
            "source_parquet": parquet_uri,
        }
    )
    context.log.info(f"Loaded {rows} rows into {table_id}")

    return table_id
