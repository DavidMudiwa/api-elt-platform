import os
from pathlib import Path

import google.auth
from google.cloud import bigquery, storage


def _ensure_local_adc() -> None:
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    candidate = Path(__file__).resolve().parents[1] / "secrets" / "adc.json"
    if candidate.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(candidate)


def main() -> None:
    _ensure_local_adc()

    credentials, detected_project = google.auth.default()
    project = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or getattr(credentials, "quota_project_id", None)
        or detected_project
    )

    print(f"ADC source     : {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
    print(f"Project        : {project}")
    print(f"Credential type: {type(credentials).__name__}")
    print(f"Service acct   : {getattr(credentials, 'service_account_email', None) or '(user credentials)'}")

    gcs = storage.Client(project=project)
    buckets = list(gcs.list_buckets(max_results=5))
    print(f"GCS auth OK    : {len(buckets)} bucket(s) visible")
    for bucket in buckets:
        print(f"  - gs://{bucket.name}")

    bq = bigquery.Client(project=project)
    datasets = list(bq.list_datasets(max_results=5))
    print(f"BQ auth OK     : {len(datasets)} dataset(s) visible")
    for dataset in datasets:
        print(f"  - {dataset.dataset_id}")


if __name__ == "__main__":
    main()
