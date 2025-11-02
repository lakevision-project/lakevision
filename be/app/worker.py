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
from app.insights.utils import get_namespace_and_table_name, qualified_table_name
from app.lakeviewer import LakeView
from app.insights.runner import InsightsRunner
from app.utils import get_bool_env
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
    It uses a two-step process to prevent race conditions:
    1. Reap stale jobs (set 'running' -> 'pending').
    2. Claim a 'pending' job.
    """
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=JOB_TIMEOUT_MINUTES)
    
    try:
        # task_storage.db_session() handles the transaction commit/rollback
        with task_storage.db_session() as session:
            
            # 1. --- Reap Stale Jobs ---
            reap_query = """
                UPDATE {table}
                SET status = :pending_status, worker_id = NULL
                WHERE status = :running_status AND started_at < :stale_time
            """.format(table=task_storage.table_name)
            
            session.execute(text(reap_query), {
                "pending_status": TaskStatus.PENDING,
                "running_status": TaskStatus.RUNNING,
                "stale_time": stale_time
            })

            # 2. --- Find a Pending Job ---
            query = """
                SELECT * FROM {table}
                WHERE status = :pending_status
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
            """.format(table=task_storage.table_name)
            
            job_to_run_raw = session.execute(text(query), {
                "pending_status": TaskStatus.PENDING
            }).fetchone()
            
            if not job_to_run_raw:
                return None # No jobs in queue
            
            job_id = job_to_run_raw.id

            # 3. --- Atomically Lock the Job ---
            # We need to capture the values we are about to write.
            lock_time = datetime.now(timezone.utc)
            lock_worker_id = WORKER_ID

            update_query = """
                UPDATE {table}
                SET status = :running_status, started_at = :now, worker_id = :worker
                WHERE id = :job_id AND status = :pending_status
                """.format(table=task_storage.table_name) 
            
            update_result = session.execute(text(update_query), {
                "running_status": TaskStatus.RUNNING,
                "now": lock_time,
                "worker": lock_worker_id,
                "job_id": job_id,
                "pending_status": TaskStatus.PENDING
            })
            
            # 4. --- Check if we won the race ---
            if update_result.rowcount == 1:
                # We won. Deserialize the raw data we selected in step 2.
                raw_data = job_to_run_raw._asdict()
                deserialized_data = task_storage._deserialize_row(raw_data) 
                
                task_object = QueuedTask(**deserialized_data)

                # We must manually update the in-memory object to match
                # the values we just wrote to the database.
                task_object.status = TaskStatus.RUNNING
                task_object.started_at = lock_time
                task_object.worker_id = lock_worker_id
                
                return task_object
                
            else:
                # We "lost" the race. Another worker got the job
                # between our SELECT and UPDATE.
                return None
                
    except Exception as e:
        print(f"Error fetching/locking job: {e}")
        return None

def execute_table_task(
    task: QueuedTask, 
    runner: InsightsRunner
):
    """
    Executes a single table-level insight run.
    This was the original logic of run_worker_cycle.
    """
    print(f"[{WORKER_ID}] Executing job {task.id} for {task.namespace}.{task.table_name}")
    try:
        runner.run_for_table(
            table_identifier=f"{task.namespace}.{task.table_name}",
            rule_ids=task.rules_requested,
            type=task.run_type
        )
        task.status = TaskStatus.COMPLETE
        task.error_details = None
        
    except Exception as e:
        print(f"[{WORKER_ID}] Job {task.id} FAILED: {e}")
        traceback.print_exc()
        task.status = TaskStatus.FAILED
        task.error_details = traceback.format_exc()
        
    finally:
        task.finished_at = datetime.now(timezone.utc)
        print(f"[{WORKER_ID}] Finished table task {task.id} with status {task.status}")


def execute_generator_task(
    task: QueuedTask, 
    task_storage: StorageInterface[QueuedTask], 
    lv: LakeView
):
    """
    Generates and enqueues all child tasks for a namespace or '*' job.
    """
    print(f"[{WORKER_ID}] Generating child tasks for job {task.id} (Namespace: {task.namespace})")
    try:
        # 1. Find all tables (this is the slow part)
        tables_to_run = []
        if task.namespace == "*":
            all_namespaces = lv.get_namespaces(include_nested=True)
            for ns in all_namespaces:
                tables_to_run.extend([qualified_table_name(t_ident) for t_ident in lv.get_tables(".".join(ns))])
        else:
            # Assuming lv.get_tables can be recursive
            tables_to_run = [qualified_table_name(t_ident) for t_ident in lv.get_tables(task.namespace)]

        print(f"Found {len(tables_to_run)} tables for job {task.id}.")

        # 2. Create new QueuedTask records
        new_child_tasks = []
        for table_ident in tables_to_run:
            # Use your helper to get clean names
            ns, tbl = get_namespace_and_table_name(table_ident)
            
            new_child_tasks.append(QueuedTask(
                batch_id=task.batch_id,
                namespace=ns,
                table_name=tbl,
                rules_requested=task.rules_requested,
                priority=task.priority, # Inherit priority
                run_type=task.run_type    # Inherit run type
                # All other fields get defaults (new ID, PENDING status, etc.)
            ))
        print(f"have list of new tasks {len(new_child_tasks)}")
        # 3. Save new tasks to the queue
        if new_child_tasks:
            # This could be a large save, but it's happening in the worker,
            # not blocking the API.
            task_storage.save_many(new_child_tasks)
            print(f"Enqueued {len(new_child_tasks)} child tasks for batch {task.batch_id}.")
        
        task.status = TaskStatus.COMPLETE
        task.error_details = None

    except Exception as e:
        print(f"[{WORKER_ID}] Generator task {task.id} FAILED: {e}")
        traceback.print_exc()
        task.status = TaskStatus.FAILED
        task.error_details = traceback.format_exc()
        
    finally:
        task.finished_at = datetime.now(timezone.utc)
        print(f"[{WORKER_ID}] Finished generator task {task.id} with status {task.status}")


def run_worker_cycle(
    task_storage: StorageInterface[QueuedTask], 
    batch_storage: StorageInterface[BackgroundJob],
    runner: InsightsRunner,
    lv: LakeView
):
    # 1. Get a job (could be generator or table task)
    task = get_atomic_job(task_storage)
    
    if not task:
        return False # No work done

    try:
        # 2. Decide what kind of task it is
        if task.table_name is not None:
            # It's an "execution task"
            execute_table_task(task, runner)
        else:
            # It's a "generator task" (table_name is None)
            execute_generator_task(task, task_storage, lv)
            
    except Exception as e:
        # This is a safety net, but execute functions have their own try/except
        print(f"[{WORKER_ID}] Critical error in worker cycle for task {task.id}: {e}")
        task.status = TaskStatus.FAILED
        task.error_details = traceback.format_exc()
        task.finished_at = datetime.now(timezone.utc)
        
    finally:
        # 3. Save the final state OF THIS TASK (generator or table)
        task_storage.save(task)
        
        # 4. Check if the *entire batch* is complete
        update_batch_status(task.batch_id, task_storage, batch_storage)

        return True # Indicates work was done
if __name__ == "__main__":
    print(f"Starting worker process {WORKER_ID}")

    # --- ADD THIS CHECK ---
    if not get_bool_env('LAKEVISION_HEALTH_ENABLED'):
        print("Health feature is disabled. Worker will not run.")
        exit()  # Exit the script immediately
    
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
    
    lv = LakeView()

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
            work_done = run_worker_cycle(task_storage, batch_storage, runner, lv)
            if not work_done:
                # No jobs found, sleep longer to avoid spamming the DB
                time.sleep(5) # Poll every 5 seconds
            else:
                # Work was done, check for more immediately
                time.sleep(0.1) 
                
        except Exception as e:
            print(f"[{WORKER_ID}] Unhandled error in worker loop: {e}")
            time.sleep(15) # Wait a bit before retrying on major error