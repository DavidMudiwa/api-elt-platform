import io
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

BUCKET = "github-data-492709"
PROJECT = "kestra-sandbox-492709"

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))

from dagster import materialize  # noqa: E402

from assets.processed import (  # noqa: E402
    github_commits_parquet,
    github_repositories_parquet,
)
from resources.gcs import GCSResource  # noqa: E402


def main() -> None:
    gcs = GCSResource(bucket=BUCKET, project=PROJECT)

    result = materialize(
        [github_repositories_parquet, github_commits_parquet],
        resources={"gcs": gcs},
    )
    assert result.success

    for event in result.get_asset_materialization_events():
        materialization = event.event_specific_data.materialization
        print(f"asset : {materialization.asset_key.to_user_string()}")
        uri = None
        for key, value in materialization.metadata.items():
            if key == "path":
                continue
            print(f"  {key} = {value.value}")
            if key == "gcs_uri":
                uri = value.value

        object_key = uri.split(f"gs://{BUCKET}/", 1)[1]
        table = pq.read_table(io.BytesIO(gcs.download_bytes(object_key)))
        print(f"  parquet rows = {table.num_rows}, columns = {table.num_columns}")
        fields = ", ".join(f"{field.name}:{field.type}" for field in table.schema)
        print(f"  schema = {fields}")


if __name__ == "__main__":
    main()
