import uuid
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response, status
from croniter import croniter
from app.insights.utils import get_namespace_and_table_name, qualified_table_name
from app.dependencies import lv, background_job_storage, schedule_storage, queued_task_storage
from app.insights.runner import InsightsRunner
from app.models import JobSchedule, QueuedTask, TaskStatus
from app.storage import get_storage
from app.models import (
    RunRequest, RunResponse, StatusResponse, BackgroundJob, InsightRun, 
    JobScheduleRequest, JobScheduleResponse, JobScheduleUpdateRequest,
    InsightRecord, ActiveInsight
)

router = APIRouter()

# --- Manual Job Endpoints ---
@router.post("/api/start-run", response_model=RunResponse, status_code=202)
def start_manual_run(run_request: RunRequest):
    batch_id = str(uuid.uuid4())

    # 1. Create the Batch record
    new_batch = BackgroundJob( # This is your 'JobBatch'
        id=batch_id, 
        namespace=run_request.namespace, 
        table_name=run_request.table_name,
        rules_requested=run_request.rules_requested, 
        status="pending", # Status is now 'pending' (i.e., enqueued)
        details="Job is being queued."
    )
    background_job_storage.save(new_batch)

    # 2. Get the list of tables to run (this logic moves from the runner)
    tables_to_run = []
    if run_request.namespace == "*":
        all_namespaces = lv.get_namespaces(include_nested=True)
        for ns in all_namespaces:
            tables_to_run.extend([qualified_table_name(t_ident) for t_ident in lv.get_tables(".".join(ns))])
    elif run_request.table_name:
        tables_to_run = [f"{run_request.namespace}.{run_request.table_name}"]
    else:
        tables_to_run = [qualified_table_name(t_ident) for t_ident in lv.get_tables(run_request.namespace)]

    # 3. Create an atomic QueuedTask for each table
    tasks = []
    for table_ident in tables_to_run:
        ns, tbl = get_namespace_and_table_name(table_ident)
        tasks.append(QueuedTask(
            batch_id=batch_id,
            namespace=ns,
            table_name=tbl,
            rules_requested=run_request.rules_requested,
            priority=1, # High priority for manual runs
            run_type="manual"
        ))

    if tasks:
        queued_task_storage.save_many(tasks)
    else:
        # No tables found, mark batch as complete
        new_batch.status = "complete"
        new_batch.details = "No tables found to process."
        background_job_storage.save(new_batch)

    return RunResponse(run_id=batch_id)

@router.get("/run-status/{run_id}", response_model=StatusResponse)
def get_run_status(run_id: str): # run_id is now a batch_id
    batch_job = background_job_storage.get_by_id(run_id)
    if not batch_job:
        raise HTTPException(status_code=404, detail="Run ID not found.")

    # Get task statuses
    # This could be a raw query for efficiency:
    # SELECT status, COUNT(*) FROM queuedtask WHERE batch_id = :run_id GROUP BY status
    tasks = queued_task_storage.get_by_attributes({"batch_id": run_id})
    
    status_counts = defaultdict(int)
    for task in tasks:
        status_counts[task.status] += 1
        
    total_tasks = len(tasks)
    pending = status_counts[TaskStatus.PENDING]
    running = status_counts[TaskStatus.RUNNING]
    complete = status_counts[TaskStatus.COMPLETE]
    failed = status_counts[TaskStatus.FAILED]

    # Update the overall batch status
    if total_tasks == 0:
        batch_job.status = "complete"
        batch_job.details = "No tasks were generated for this job."
    elif pending == 0 and running == 0:
        # All jobs are finished
        if failed > 0:
            batch_job.status = "failed"
            batch_job.details = f"Job finished with {failed} / {total_tasks} tasks failed."
        else:
            batch_job.status = "complete"
            batch_job.details = f"Job finished successfully. ({complete} / {total_tasks} tasks)"
        batch_job.finished_at = datetime.now(timezone.utc)
    else:
        # Job is still going
        batch_job.status = "running"
        batch_job.details = f"Processing: {complete} complete, {running} running, {failed} failed, {pending} pending."
    
    background_job_storage.save(batch_job) # Save the updated summary
    
    return StatusResponse.from_job(batch_job)

@router.get("/api/jobs/running", response_model=List[StatusResponse])
def get_running_jobs(namespace: str, table_name: Optional[str] = None):
    criteria = {"namespace": namespace, "status": ["pending", "running"]}
    if table_name:
        criteria["table_name"] = table_name
    running_jobs = background_job_storage.get_by_attributes(criteria)
    return [StatusResponse.from_job(job) for job in running_jobs] if running_jobs else []

# --- Schedule Endpoints ---
@router.post("/api/schedules", response_model=JobScheduleResponse, status_code=201)
def create_schedule(schedule_request: JobScheduleRequest):
    try:
        now = datetime.now(timezone.utc)
        iterator = croniter(schedule_request.cron_schedule, now)
        next_run = iterator.get_next(datetime)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid cron_schedule: {e}")
    
    new_schedule = JobSchedule(next_run_timestamp=next_run, **schedule_request.model_dump())
    schedule_storage.save(new_schedule)
    return new_schedule

@router.put("/api/schedules/{schedule_id}", response_model=JobScheduleResponse)
def update_schedule(schedule_id: str, update_req: JobScheduleUpdateRequest):
    schedule = schedule_storage.get_by_id(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found.")
    
    update_data = update_req.model_dump(exclude_unset=True)
    if "cron_schedule" in update_data:
        try:
            iterator = croniter(update_data["cron_schedule"], datetime.now(timezone.utc))
            schedule.next_run_timestamp = iterator.get_next(datetime)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron_schedule: {e}")
    
    for key, value in update_data.items():
        setattr(schedule, key, value)
    
    schedule_storage.save(schedule)
    return schedule

@router.delete("/api/schedules/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_schedule(schedule_id: str):
    if not schedule_storage.get_by_id(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found.")
    schedule_storage.delete(schedule_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@router.get("/api/schedules", response_model=List[JobScheduleResponse])
def list_schedules(namespace: str, table_name: Optional[str] = None):
    # If namespace is '*', the criteria is to find schedules
    # where the table_name is not set (is NULL).
    if namespace == "*":
        criteria = {"table_name": None}
    else:
        # Otherwise, use the original logic.
        criteria = {"namespace": namespace}
        if table_name:
            criteria["table_name"] = table_name
            
    return schedule_storage.get_by_attributes(criteria)