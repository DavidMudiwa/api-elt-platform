from datetime import date, timedelta

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from assets.bigquery import github_commits_bq
from assets.github import github_commits_raw
from assets.processed import github_commits_parquet

commits_job = define_asset_job(
    name="commits_job",
    selection=AssetSelection.assets(
        github_commits_raw,
        github_commits_parquet,
        github_commits_bq,
    ),
)


@schedule(
    cron_schedule="0 9 * * *",
    job=commits_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
def commits_daily_schedule(context: ScheduleEvaluationContext):
    """Materialize yesterday's commits partition (UTC) each day at 09:00."""
    partition = (date.today() - timedelta(days=1)).isoformat()
    context.log.info(f"Requesting commits partition {partition}")
    yield RunRequest(partition_key=partition, run_key=partition)
