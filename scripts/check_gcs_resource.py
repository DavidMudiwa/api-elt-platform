import os
import sys
from pathlib import Path
from resources.gcs import GCSResource 

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))
os.environ.setdefault("GCS_BUCKET", "github-data-492709")
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "kestra-sandbox-492709")

 


def main() -> None:
    gcs = GCSResource(
        bucket=os.environ["GCS_BUCKET"],
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
    )
    key = "_meta/connectivity_check.json"
    payload = {"status": "ok", "checked_by": "GCSResource"}

    uri = gcs.upload_json(key, payload)
    print(f"uploaded  : {uri}")

    round_tripped = gcs.download_json(key)
    assert round_tripped == payload, round_tripped
    print(f"downloaded: {round_tripped}")

    gcs.get_client().bucket(gcs.bucket).blob(key).delete()
    print("cleaned up: test object deleted")


if __name__ == "__main__":
    main()
