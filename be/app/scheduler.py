import time
from datetime import datetime, timezone
from croniter import croniter
from app.lakeviewer import LakeView
from app.insights.runner import InsightsRunner
from app.models import InsightRun
from app.models import JobSchedule
from app.storage import get_storage, StorageInterface

def execute_job(schedule: JobSchedule, storage: StorageInterface):
    """
    Takes a schedule object and executes the insight run.
    This function now lives in the scheduler, where it belongs.
    """
    lv = LakeView()
    # The runner is now created here, using the storage connection
    # that the scheduler already has open.
    runner = InsightsRunner(lv, storage_adapter=storage)

    target = schedule.namespace
    if schedule.table_name:
        target += f".{schedule.table_name}"
    print(f"Executing job for target '{target}'")

    runner.run_for_table(target, rule_ids=schedule.rules_requested, type="auto")

    print(f"Execution finished for schedule {schedule.id}.")

def run_scheduler_cycle(schedule_storage: StorageInterface ):
    """
    This function runs once to check for and trigger due jobs.
    """
    print(f"[{datetime.now()}] Scheduler checking for due jobs...")
    # 1. Find all active jobs that are due to run.
    now = datetime.now(timezone.utc)
    schedules_to_run = schedule_storage.find_by_raw_query(
        "SELECT * FROM jobschedules WHERE is_enabled = TRUE AND next_run_timestamp <= :now_timestamp",
        {"now_timestamp": now}
    )

    insight_run_storage = get_storage(model=InsightRun)
    insight_run_storage.connect()
    insight_run_storage.ensure_table()

    for schedule in schedules_to_run:
        print(f"Triggering job for schedule: {schedule.id}")
        
        # 2. Trigger the job execution (ideally asynchronously).
        # This function (defined in the next section) does the actual work.
        execute_job(schedule, insight_run_storage) 
        
        # 3. Update the schedule for its next run.
        base_time = now
        iterator = croniter(schedule.cron_schedule, base_time)
        schedule.next_run_timestamp = iterator.get_next(datetime)
        schedule.last_run_timestamp = now
        
        schedule_storage.save(schedule) # Save the updated timestamps

    insight_run_storage.disconnect()

if __name__ == "__main__":
    # This loop makes the script run forever.
    # In production, you'd use a real daemon or a cron job to run it.
    print("Starting scheduler process")
    while True:
        print("Checking schedules to run")
        schedule_storage = get_storage(model=JobSchedule)
        schedule_storage.connect()
        schedule_storage.ensure_table()
        run_scheduler_cycle(schedule_storage)
        time.sleep(60) # Wait for 60 seconds