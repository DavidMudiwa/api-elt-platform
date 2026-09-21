import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from dagster import AssetExecutionContext, RetryPolicy, asset

from resources.gcs import GCSResource


def extract_paginated(
    context: AssetExecutionContext,
    url: str,
    item_label: str,
    per_page: int,
    max_pages: int,
) -> list[dict]:
    """Fetch GitHub API pages until a page is empty or max_pages is reached."""
    items: list[dict] = []

    context.log.info(f"Starting {item_label} extraction")
    context.log.info(f"{item_label} per page: {per_page}")
    context.log.info(f"Maximum pages: {max_pages}")

    for page in range(1, max_pages + 1):
        context.log.info(f"Requesting GitHub {item_label} page {page}")

        params = {
            "page": page,
            "per_page": per_page,
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            page_data = response.json()
        except requests.RequestException as error:
            context.log.error(
                f"GitHub API request failed on {item_label} page {page}: {error}"
            )
            raise

        context.log.info(f"Page {page} returned {len(page_data)} {item_label}")

        if not page_data:
            context.log.info(f"Page {page} is empty. No more {item_label}.")
            break

        items.extend(page_data)

    return items


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=5,
    )
)
def github_repositories_raw(
    context: AssetExecutionContext,
    gcs: GCSResource,
) -> str:
    """Extract public repositories from GitHub and store the raw JSON in GCS."""

    repositories = extract_paginated(
        context=context,
        url="https://api.github.com/repositories",
        item_label="repositories",
        per_page=100,
        max_pages=3,
    )

    now = datetime.now(timezone.utc)
    key = (
        f"raw/repositories/dt={now:%Y-%m-%d}/"
        f"repositories_{now:%Y%m%d_%H%M%S}.json"
    )

    uri = gcs.upload_json(key, repositories)

    context.add_output_metadata(
        {
            "records": len(repositories),
            "gcs_uri": uri,
        }
    )

    context.log.info(f"Total repositories collected: {len(repositories)}")
    context.log.info(f"Raw data written to: {uri}")

    return uri


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=5,
    )
)
def github_commits_raw(context: AssetExecutionContext):
    """Extract recent commits from the Dagster GitHub repo and save the raw response as JSON."""

    url = "https://api.github.com/repos/dagster-io/dagster/commits"

    per_page = 100
    max_pages = 3

    commits = []

    context.log.info("Starting GitHub commits extraction")
    context.log.info(f"Commits per page: {per_page}")
    context.log.info(f"Maximum pages: {max_pages}")

    for page in range(1, max_pages + 1):

        context.log.info(f"Requesting GitHub commits page {page}")

        params = {
            "page": page,
            "per_page": per_page,
        }

        try:
            response = requests.get(
                url,
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            page_data = response.json()

            context.log.info(
                f"Page {page} returned {len(page_data)} commits"
            )

            if not page_data:
                context.log.info(
                    f"Page {page} is empty. No more commits."
                )
                break

            commits.extend(page_data)

        except requests.RequestException as error:
            context.log.error(
                f"GitHub API request failed on page {page}: {error}"
            )
            raise

        except Exception as error:
            context.log.error(
                f"Unexpected error on page {page}: {error}"
            )
            raise

    output_dir = Path("data/raw/github")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"commits_{timestamp}.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(commits, file, indent=2)

    context.log.info(
        f"Total commits collected: {len(commits)}"
    )

    context.log.info(
        f"Raw data written to: {output_file}"
    )

    return str(output_file)