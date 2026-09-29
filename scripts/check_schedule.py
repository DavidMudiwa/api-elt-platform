import sys
from pathlib import Path
from dagster import build_schedule_context  
from main import defs  
from schedules.commits import commits_daily_schedule  

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

def main() -> None:
    context = build_schedule_context(repository_def=defs.get_repository_def())
    tick = commits_daily_schedule.evaluate_tick(context)
    if not tick.run_requests:
        print("schedule produced no run requests")
        return
    for request in tick.run_requests:
        print(f"partition_key : {request.partition_key}")
        print(f"run_key       : {request.run_key}")


if __name__ == "__main__":
    main()
