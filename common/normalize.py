from datetime import date, datetime


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse a GitHub ISO-8601 timestamp (e.g. '2026-09-21T12:00:00Z')."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_repository(record: dict, partition_date: date) -> dict:
    """Project a raw GitHub repository record onto the warehouse contract."""
    owner = record.get("owner") or {}
    return {
        "repository_id": record["id"],
        "name": record["name"],
        "full_name": record["full_name"],
        "private": record["private"],
        "html_url": record["html_url"],
        "description": record.get("description"),
        "fork": record.get("fork"),
        "owner_login": owner.get("login"),
        "dt": partition_date,
    }


def normalize_commit(record: dict, repository: str, partition_date: date) -> dict:
    """Project a raw GitHub commit record onto the warehouse contract."""
    commit = record.get("commit") or {}
    author = commit.get("author") or {}
    return {
        "sha": record["sha"],
        "repository": repository,
        "author": author.get("name"),
        "message": commit.get("message"),
        "committed_at": parse_timestamp(author.get("date")),
        "dt": partition_date,
    }
