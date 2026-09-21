import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

BUCKET = "github-data-492709"
PROJECT = "kestra-sandbox-492709"

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))

from dagster import materialize  # noqa: E402

from assets.github import github_repositories_raw  # noqa: E402
from resources.gcs import GCSResource  # noqa: E402


def main() -> None:
    result = materialize(
        [github_repositories_raw],
        resources={"gcs": GCSResource(bucket=BUCKET, project=PROJECT)},
    )
    assert result.success

    for event in result.get_asset_materialization_events():
        materialization = event.event_specific_data.materialization
        print(f"asset      : {materialization.asset_key.to_user_string()}")
        for key, value in materialization.metadata.items():
            print(f"metadata   : {key} = {value.value}")


if __name__ == "__main__":
    main()
