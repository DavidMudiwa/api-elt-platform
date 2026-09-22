import unittest
from datetime import date, datetime, timezone

from common.normalize import (
    normalize_commit,
    normalize_repository,
    parse_timestamp,
)

PARTITION = date(2026, 9, 21)


class ParseTimestampTest(unittest.TestCase):
    def test_parses_z_suffix(self):
        self.assertEqual(
            parse_timestamp("2026-09-21T12:00:00Z"),
            datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc),
        )

    def test_none_passes_through(self):
        self.assertIsNone(parse_timestamp(None))


class NormalizeRepositoryTest(unittest.TestCase):
    def test_projects_columns_and_adds_dt(self):
        raw = {
            "id": 1,
            "name": "grit",
            "full_name": "mojombo/grit",
            "private": False,
            "html_url": "https://github.com/mojombo/grit",
            "description": "Grit is a Ruby library for extracting information",
            "fork": False,
            "owner": {"login": "mojombo"},
        }
        self.assertEqual(
            normalize_repository(raw, PARTITION),
            {
                "repository_id": 1,
                "name": "grit",
                "full_name": "mojombo/grit",
                "private": False,
                "html_url": "https://github.com/mojombo/grit",
                "description": "Grit is a Ruby library for extracting information",
                "fork": False,
                "owner_login": "mojombo",
                "dt": PARTITION,
            },
        )


class NormalizeCommitTest(unittest.TestCase):
    def test_projects_columns_and_sets_repository_and_dt(self):
        raw = {
            "sha": "abc123",
            "commit": {
                "author": {
                    "name": "Ada",
                    "email": "ada@example.com",
                    "date": "2026-09-21T09:30:00Z",
                },
                "message": "Fix bug",
            },
        }
        self.assertEqual(
            normalize_commit(raw, "dagster-io/dagster", PARTITION),
            {
                "sha": "abc123",
                "repository": "dagster-io/dagster",
                "author": "Ada",
                "message": "Fix bug",
                "committed_at": datetime(2026, 9, 21, 9, 30, tzinfo=timezone.utc),
                "dt": PARTITION,
            },
        )


if __name__ == "__main__":
    unittest.main()
