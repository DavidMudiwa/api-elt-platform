import json
from typing import Any

from dagster import ConfigurableResource, EnvVar
from google.cloud import storage
from pydantic import PrivateAttr


class GCSResource(ConfigurableResource):
    """Injectable handle to a Google Cloud Storage bucket.

    Configuration is read from the environment (GCS_BUCKET,
    GOOGLE_CLOUD_PROJECT) so the same asset code runs locally and in the
    container without hardcoding bucket names or credentials.
    """

    bucket: str = EnvVar("GCS_BUCKET")
    project: str = EnvVar("GOOGLE_CLOUD_PROJECT")

    _client: storage.Client | None = PrivateAttr(default=None)

    def get_client(self) -> storage.Client:
        """Return a cached GCS client (created on first use)."""
        if self._client is None:
            self._client = storage.Client(project=self.project)
        return self._client

    def object_uri(self, key: str) -> str:
        """Build the `gs://` URI for an object key."""
        return f"gs://{self.bucket}/{key}"

    def upload_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload raw bytes and return the resulting gs:// URI."""
        blob = self.get_client().bucket(self.bucket).blob(key)
        blob.upload_from_string(data, content_type=content_type)
        return self.object_uri(key)

    def download_bytes(self, key: str) -> bytes:
        """Download an object's raw bytes."""
        blob = self.get_client().bucket(self.bucket).blob(key)
        return blob.download_as_bytes()

    def upload_json(self, key: str, payload: Any) -> str:
        """Serialise a Python object to JSON and upload it."""
        data = json.dumps(payload, indent=2).encode("utf-8")
        return self.upload_bytes(key, data, content_type="application/json")

    def download_json(self, key: str) -> Any:
        """Download a JSON object and deserialise it."""
        return json.loads(self.download_bytes(key).decode("utf-8"))

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys under a prefix (recursive)."""
        blobs = self.get_client().list_blobs(self.bucket, prefix=prefix)
        return [blob.name for blob in blobs]

    def find_latest_key(self, prefix: str) -> str:
        """Return the greatest key under a prefix.

        Raw keys embed a sortable UTC timestamp, so lexicographic max == newest.
        """
        keys = self.list_keys(prefix)
        if not keys:
            raise FileNotFoundError(
                f"No objects found under gs://{self.bucket}/{prefix}"
            )
        return max(keys)
