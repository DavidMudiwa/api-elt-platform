"""Explicit BigQuery data contracts (schema, partitioning, clustering).

Stored as column tuples so the contract is pure data; `to_table_schema`
renders it as Dagster metadata, and `BigQueryResource` converts it to
`bigquery.SchemaField` objects at load time.
"""

from dagster import TableColumn, TableSchema

REPOSITORIES_SCHEMA: list[tuple[str, str]] = [
    ("repository_id", "INT64"),
    ("name", "STRING"),
    ("full_name", "STRING"),
    ("private", "BOOL"),
    ("html_url", "STRING"),
    ("description", "STRING"),
    ("fork", "BOOL"),
    ("owner_login", "STRING"),
    ("dt", "DATE"),
]

COMMITS_SCHEMA: list[tuple[str, str]] = [
    ("sha", "STRING"),
    ("repository", "STRING"),
    ("author", "STRING"),
    ("message", "STRING"),
    ("committed_at", "TIMESTAMP"),
    ("dt", "DATE"),
]

REPOSITORIES_PARTITION_FIELD = "dt"
COMMITS_PARTITION_FIELD = "dt"
COMMITS_CLUSTERING_FIELDS = ["repository"]


def to_table_schema(columns: list[tuple[str, str]]) -> TableSchema:
    """Render a column contract as Dagster `TableSchema` metadata."""
    return TableSchema(
        columns=[TableColumn(name, field_type) for name, field_type in columns]
    )
