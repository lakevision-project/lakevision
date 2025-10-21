import time
import uuid
import traceback
from typing import Optional
from datetime import datetime, timezone, timedelta
from app.storage import get_storage, StorageInterface
from app.models import (
    QueuedTask, TaskStatus, InsightRun, InsightRecord, 
    ActiveInsight, BackgroundJob
)
from app.lakeviewer import LakeView
from app.insights.runner import InsightsRunner
from sqlalchemy import text
from collections import defaultdict

WORKER_ID = str(uuid.uuid4())
JOB_TIMEOUT_MINUTES = 30 # Max time a job can be "running" before it's considered stale

def update_batch_status(
    batch_id: str, 
    task_storage: StorageInterface[QueuedTask], 
    batch_storage: StorageInterface[BackgroundJob]
):
    """
    Checks if all tasks for a batch are complete, and if so,
    updates the parent BackgroundJob's status.
    """
    print(f"Checking batch status for {batch_id}...")

    # 1. Check if any tasks are still active (pending or running)
    active_tasks = task_storage.get_by_attributes({
        "batch_id": batch_id,
        "status": [TaskStatus.PENDING, TaskStatus.RUNNING]
    }, limit=1) # We only need to know if at least one exists

    if active_tasks:
        # Batch is not finished, do nothing.
        print(f"Batch {batch_id} is still running.")
        return

    # 2. No active tasks. The batch is finished.
    # Now, let's aggregate the results.
    print(f"Batch {batch_id} is finished. Aggregating results...")
    
    # Get all tasks to count statuses
    all_tasks = task_storage.get_by_attributes({"batch_id": batch_id})
    if not all_tasks:
        print(f"Warning: No tasks found for completed batch {batch_id}.")
        return

    status_counts = defaultdict(int)
    for task in all_tasks:
        status_counts[task.status] += 1
    
    total_tasks = len(all_tasks)
    failed_count = status_counts[TaskStatus.FAILED]
    complete_count = status_counts[TaskStatus.COMPLETE]

    # 3. Get and update the parent batch job
    batch_job = batch_storage.get_by_id(batch_id)
    if not batch_job:
        print(f"Error: Could not find parent batch job {batch_id} to update status.")
        return

    # Only update if it's not already marked as complete
    if batch_job.status in ["pending", "running"]:
        if failed_count > 0:
            batch_job.status = "failed"
            batch_job.details = f"Job finished with {failed_count} / {total_tasks} tasks failed."
        else:
            batch_job.status = "complete"
            batch_job.details = f"Job finished successfully. ({complete_count} / {total_tasks} tasks)"
            
        batch_job.finished_at = datetime.now(timezone.utc)
        
        # 4. Save the final status
        batch_storage.save(batch_job)
        print(f"Updated batch {batch_id} status to {batch_job.status}.")
    else:
        print(f"Batch {batch_id} status already set to {batch_job.status}. Skipping update.")

def get_atomic_job(task_storage: StorageInterface[QueuedTask]) -> Optional[QueuedTask]:
    """
    Atomically fetches and locks a single job from the queue.
    This operation is transactional and compatible with Postgres and SQLite.
    """
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=JOB_TIMEOUT_MINUTES)
    
    query = """
        SELECT * FROM {table}
        WHERE (status = :pending_status) OR (status = :running_status AND started_at < :stale_time)
        ORDER BY priority ASC, created_at ASC
        LIMIT 1
    """.format(table=task_storage.table_name)
    
    try:
        # task_storage.db_session() handles the transaction commit/rollback
        with task_storage.db_session() as session:
            
            # 1. Find a potential job
            job_to_run_raw = session.execute(text(query), {
                "pending_status": TaskStatus.PENDING,
                "running_status": TaskStatus.RUNNING,
                "stale_time": stale_time
            }).fetchone()
            
            if not job_to_run_raw:
                return None # No jobs in queue
            
            job_id = job_to_run_raw.id
            
            # 2. Attempt to "lock" the job by updating its status.
            # This version does NOT use 'RETURNING *' to be SQLite compatible.
            update_query = """
                UPDATE {table}
                SET status = :running_status, started_at = :now, worker_id = :worker
                WHERE id = :job_id AND (status = :pending_status OR status = :running_status)
                """.format(table=task_storage.table_name) 
            
            update_result = session.execute(text(update_query), {
                "running_status": TaskStatus.RUNNING,
                "now": datetime.now(timezone.utc),
                "worker": WORKER_ID,
                "job_id": job_id,
                "pending_status": TaskStatus.PENDING
            })
            
            # 3. Check if we successfully locked the job.
            # If rowcount is 1, we won the race.
            if update_result.rowcount == 1:
                
                # 4. We won. Use the data we fetched in step 1.
                # We must deserialize it using the adapter's method.
                raw_data = job_to_run_raw._asdict()
                deserialized_data = task_storage._deserialize_row(raw_data) 
                
                return QueuedTask(**deserialized_data)
                
            else:
                # We "lost" the race. Another worker got the job
                # between our SELECT and UPDATE.
                return None
                
    except Exception as e:
        print(f"Error fetching/locking job: {e}")
        return None

def run_worker_cycle(task_storage, batch_storage, runner):
    task = get_atomic_job(task_storage)
    
    if not task:
        # No jobs to run, sleep for a bit
        return False # Indicates no work was done

    print(f"[{WORKER_ID}] Picked up job {task.id} for {task.namespace}.{task.table_name}")
    
    try:
        # 3. Execute the *actual* work
        # We only call run_for_table, as all jobs are now at the table level.
        runner.run_for_table(
            table_identifier=f"{task.namespace}.{task.table_name}",
            rule_ids=task.rules_requested,
            type=task.run_type
        )
        
        # 4. Mark as complete
        task.status = TaskStatus.COMPLETE
        task.finished_at = datetime.now(timezone.utc)
        task.error_details = None
        
    except Exception as e:
        # 5. Mark as failed
        print(f"[{WORKER_ID}] Job {task.id} FAILED: {e}")
        traceback.print_exc()
        task.status = TaskStatus.FAILED
        task.finished_at = datetime.now(timezone.utc)
        task.error_details = traceback.format_exc()
        
    finally:
        # 6. Save the final state
        task_storage.save(task)
        update_batch_status(task.batch_id, task_storage, batch_storage)
        print(f"[{WORKER_ID}] Finished job {task.id} with status {task.status}")
        return True # Indicates work was done

if __name__ == "__main__":
    print(f"Starting worker process {WORKER_ID}")
    
    # These storages are opened once and passed to the runner
    run_storage = get_storage(model=InsightRun)
    insight_record_storage = get_storage(model=InsightRecord)
    active_insight_storage = get_storage(model=ActiveInsight)
    task_storage = get_storage(model=QueuedTask)
    batch_storage = get_storage(model=BackgroundJob)
    
    # Connect all storages
    run_storage.connect()
    insight_record_storage.connect()
    active_insight_storage.connect()
    task_storage.connect()
    batch_storage.connect()
    
    # Ensure all tables exist
    run_storage.ensure_table()
    insight_record_storage.ensure_table()
    active_insight_storage.ensure_table()
    task_storage.ensure_table()
    batch_storage.ensure_table()
    
    # Create a single runner instance
    runner = InsightsRunner(
        lakeview=LakeView(),
        run_storage=run_storage,
        insight_storage=insight_record_storage,
        active_insight_storage=active_insight_storage
    )

    # This is the worker's main loop. It runs *fast*.
    while True:
        try:
            work_done = run_worker_cycle(task_storage, batch_storage, runner)
            if not work_done:
                # No jobs found, sleep longer to avoid spamming the DB
                time.sleep(5) # Poll every 5 seconds
            else:
                # Work was done, check for more immediately
                time.sleep(0.1) 
                
        except Exception as e:
            print(f"[{WORKER_ID}] Unhandled error in worker loop: {e}")
            time.sleep(15) # Wait a bit before retrying on major error