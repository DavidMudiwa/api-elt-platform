"""Explicit BigQuery data contracts (schema, partitioning, clustering).

Kept free of the BigQuery client so it stays pure data; Step 11 converts
these column tuples into `bigquery.SchemaField` objects at load time.
"""

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
