from dagster import ConfigurableResource, EnvVar
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pydantic import PrivateAttr


class BigQueryResource(ConfigurableResource):
    """Injectable handle to a BigQuery dataset.

    Project and dataset come from the environment (GOOGLE_CLOUD_PROJECT,
    BIGQUERY_DATASET) so nothing is hardcoded in asset bodies.
    """

    project: str = EnvVar("GOOGLE_CLOUD_PROJECT")
    dataset: str = EnvVar("BIGQUERY_DATASET")

    _client: bigquery.Client | None = PrivateAttr(default=None)

    def get_client(self) -> bigquery.Client:
        """Return a cached BigQuery client (created on first use)."""
        if self._client is None:
            self._client = bigquery.Client(project=self.project)
        return self._client

    def table_id(self, table: str) -> str:
        """Fully-qualified `project.dataset.table` id."""
        return f"{self.project}.{self.dataset}.{table}"

    @staticmethod
    def _schema_fields(
        schema: list[tuple[str, str]],
    ) -> list[bigquery.SchemaField]:
        return [bigquery.SchemaField(name, field_type) for name, field_type in schema]

    def ensure_table(
        self,
        table: str,
        schema: list[tuple[str, str]],
        partition_field: str,
        clustering_fields: list[str] | None = None,
    ) -> None:
        """Create the table (partitioned/clustered) if it does not exist."""
        client = self.get_client()
        table_id = self.table_id(table)
        try:
            client.get_table(table_id)
            return
        except NotFound:
            pass

        new_table = bigquery.Table(table_id, schema=self._schema_fields(schema))
        new_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        if clustering_fields:
            new_table.clustering_fields = clustering_fields
        client.create_table(new_table)

    def load_parquet(
        self,
        table: str,
        source_uri: str,
        schema: list[tuple[str, str]],
        partition_field: str,
        partition_date,
        clustering_fields: list[str] | None = None,
    ) -> int:
        """Load a Parquet object into its dt partition, replacing that partition."""
        self.ensure_table(table, schema, partition_field, clustering_fields)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=self._schema_fields(schema),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        destination = f"{self.table_id(table)}${partition_date:%Y%m%d}"
        job = self.get_client().load_table_from_uri(
            source_uri, destination, job_config=job_config
        )
        job.result()
        return job.output_rows
