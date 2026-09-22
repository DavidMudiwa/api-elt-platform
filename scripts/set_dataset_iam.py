import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))

from google.cloud import bigquery  # noqa: E402
from google.cloud.bigquery import AccessEntry  # noqa: E402

PROJECT = "kestra-sandbox-492709"
DATASET = "github"
MEMBER = (
    "serviceAccount:api-platform-dev@kestra-sandbox-492709.iam.gserviceaccount.com"
)
ROLE = "roles/bigquery.dataEditor"


def _has_entry(entries: list[AccessEntry], wanted: AccessEntry) -> bool:
    return any(
        entry.role == wanted.role
        and entry.entity_type == wanted.entity_type
        and entry.entity_id == wanted.entity_id
        for entry in entries
    )


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    dataset = client.get_dataset(DATASET)

    entry = AccessEntry(role=ROLE, entity_type="iamMember", entity_id=MEMBER)
    entries = list(dataset.access_entries)
    if not _has_entry(entries, entry):
        entries.append(entry)
    dataset.access_entries = entries
    client.update_dataset(dataset, ["access_entries"])

    print(f"dataset {PROJECT}.{DATASET} access entries:")
    for entry in client.get_dataset(DATASET).access_entries:
        print(f"  {entry.role}: {entry.entity_id}")


if __name__ == "__main__":
    main()
