import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from dagster import asset


@asset
def github_repositories_raw(context):
    """Extract public repositories from GitHub and save the raw response as JSON."""

    url = "https://api.github.com/repositories"

    context.log.info("Starting GitHub repositories extraction")
    context.log.info(f"Request URL: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        context.log.info(f"Successfully received {len(data)} repositories")

        output_dir = Path("data/raw/github")
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"repositories_{timestamp}.json"

        with output_file.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)

        context.log.info(f"Raw data written to: {output_file}")

        return str(output_file)

    except requests.RequestException as error:
        context.log.error(f"GitHub API request failed: {error}")
        raise

    except Exception as error:
        context.log.error(f"Unexpected error during extraction: {error}")
        raise
    