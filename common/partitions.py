from dagster import DailyPartitionsDefinition

COMMITS_PARTITIONS = DailyPartitionsDefinition(start_date="2026-09-01")
