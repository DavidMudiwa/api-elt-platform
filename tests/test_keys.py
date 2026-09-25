import unittest
from datetime import date, datetime, timezone

from common.keys import partition_date_from_key, processed_key, raw_key

RUN_TIME = datetime(2026, 9, 21, 12, 36, 9, tzinfo=timezone.utc)
PARTITION = date(2026, 9, 21)


class RawKeyTest(unittest.TestCase):
    def test_repositories_key(self):
        self.assertEqual(
            raw_key("repositories", RUN_TIME),
            "raw/repositories/dt=2026-09-21/repositories_20260921_123609.json",
        )

    def test_commits_key_flattens_repo_slash(self):
        self.assertEqual(
            raw_key("commits", RUN_TIME, repo="dagster-io/dagster"),
            "raw/commits/repo=dagster-io__dagster/dt=2026-09-21/"
            "commits_20260921_123609.json",
        )

    def test_explicit_partition_date_overrides_run_time_date(self):
        self.assertEqual(
            raw_key(
                "commits",
                RUN_TIME,
                repo="dagster-io/dagster",
                partition_date=date(2026, 9, 1),
            ),
            "raw/commits/repo=dagster-io__dagster/dt=2026-09-01/"
            "commits_20260921_123609.json",
        )


class ProcessedKeyTest(unittest.TestCase):
    def test_repositories_key_is_deterministic(self):
        self.assertEqual(
            processed_key("repositories", PARTITION),
            "processed/repositories/dt=2026-09-21/repositories.parquet",
        )

    def test_commits_key_flattens_repo_slash(self):
        self.assertEqual(
            processed_key("commits", PARTITION, repo="dagster-io/dagster"),
            "processed/commits/repo=dagster-io__dagster/dt=2026-09-21/"
            "commits.parquet",
        )


class PartitionDateFromKeyTest(unittest.TestCase):
    def test_extracts_date(self):
        self.assertEqual(
            partition_date_from_key(
                "raw/commits/repo=dagster-io__dagster/dt=2026-09-21/"
                "commits_20260921_123609.json"
            ),
            PARTITION,
        )

    def test_raises_without_partition(self):
        with self.assertRaises(ValueError):
            partition_date_from_key("raw/repositories/repositories.json")


if __name__ == "__main__":
    unittest.main()
