import time
from datetime import datetime, timezone
from croniter import croniter
from app.lakeviewer import LakeView
from app.insights.runner import InsightsRunner
from app.models import InsightRun, InsightRecord, JobSchedule, ActiveInsight, BackgroundJob, QueuedTask
from app.storage import get_storage, StorageInterface
from app.utils import get_bool_env
from app.insights.utils import get_namespace_and_table_name
import logging
import uuid

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
    lv = LakeView()
    insight_run_storage = get_storage(model=InsightRun)
    insight_run_storage.connect()
    insight_run_storage.ensure_table()
    insight_record_storage = get_storage(model=InsightRecord)
    insight_record_storage.connect()
    insight_record_storage.ensure_table()
    active_insight_storage = get_storage(model=ActiveInsight)
    active_insight_storage.connect()
    active_insight_storage.ensure_table()
    background_job_storage = get_storage(model=BackgroundJob)
    background_job_storage.connect()
    background_job_storage.ensure_table()
    queued_task_storage = get_storage(model=QueuedTask)
    queued_task_storage.connect()
    queued_task_storage.ensure_table()

    for schedule in schedules_to_run:
        print(f"Enqueuing generator task for schedule: {schedule.id}")
        
        # 1. Create a Batch record
        batch_id = str(uuid.uuid4())
        new_batch = BackgroundJob(
            id=batch_id,
            namespace=schedule.namespace,
            table_name=schedule.table_name,
            rules_requested=schedule.rules_requested,
            status="pending",
            details=f"Scheduled run from schedule {schedule.id}"
        )
        background_job_storage.save(new_batch)

        # 2. Create ONE QueuedTask that mirrors the schedule
        # If table_name is None, the worker will treat it as a generator.
        task = QueuedTask(
            batch_id=batch_id,
            namespace=schedule.namespace,
            table_name=schedule.table_name,
            rules_requested=schedule.rules_requested,
            priority=10, 
            run_type="auto"
        )
        
        # 3. Save the single task
        queued_task_storage.save(task)
        
        # 3. Update the schedule for its next run.
        base_time = now
        iterator = croniter(schedule.cron_schedule, base_time)
        schedule.next_run_timestamp = iterator.get_next(datetime)
        schedule.last_run_timestamp = now
        
        schedule_storage.save(schedule) # Save the updated timestamps
        print(f"Finished job for schedule: {schedule.id}")

    insight_run_storage.disconnect()
    insight_record_storage.disconnect()
    active_insight_storage.disconnect()
    background_job_storage.disconnect()
    queued_task_storage.disconnect()

if __name__ == "__main__":
    # This loop makes the script run forever.
    # In production, you'd use a real daemon or a cron job to run it.
    print("Starting scheduler process")

    if not get_bool_env('LAKEVISION_HEALTH_ENABLED'):
        print("Health feature is disabled. Scheduler will not run.")
        exit()  # Exit the script immediately

    while True:
        try:
            print("Checking schedules to run")
            schedule_storage = get_storage(model=JobSchedule)
            schedule_storage.connect()
            schedule_storage.ensure_table()
            run_scheduler_cycle(schedule_storage)
            print("Finish checking schedules to run")
            time.sleep(600) # Wait for 600 seconds
        except Exception as e:
            logging.error(f"Error running scheduler: {str(e)}")
