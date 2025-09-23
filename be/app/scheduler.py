import time
from datetime import datetime, timezone
from croniter import croniter
from app.insights.runner import execute_job
from app.insights.job_schedule import JobSchedule
import os
from app.storage import get_storage, StorageInterface

def run_scheduler_cycle(schedule_storage: StorageInterface ):
    """
    This function runs once to check for and trigger due jobs.
    """
    print(f"[{datetime.now()}] Scheduler checking for due jobs...")
    
    # 1. Find all active jobs that are due to run.
    now = datetime.now(timezone.utc)
    # This query is conceptual; your storage implementation would handle it.
    schedules_to_run = schedule_storage.find_by_raw_query(
        "SELECT * FROM jobschedules WHERE is_enabled = TRUE AND next_run_timestamp <= :now_timestamp",
        {"now_timestamp": now}
    )
    for schedule in schedules_to_run:
        print(f"Triggering job for schedule: {schedule.id}")
        
        # 2. Trigger the job execution (ideally asynchronously).
        # This function (defined in the next section) does the actual work.
        execute_job(schedule) 
        
        # 3. Update the schedule for its next run.
        base_time = now
        iterator = croniter(schedule.cron_schedule, base_time)
        schedule.next_run_timestamp = iterator.get_next(datetime)
        schedule.last_run_timestamp = now
        
        schedule_storage.save(schedule) # Save the updated timestamps

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