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

@router.post("/api/start-run", response_model=RunResponse, status_code=202)
def start_manual_run(run_request: RunRequest):
    batch_id = str(uuid.uuid4())
    
    # 1. Create the Batch record
    new_batch = BackgroundJob( 
        id=batch_id, 
        namespace=run_request.namespace, 
        table_name=run_request.table_name,
        rules_requested=run_request.rules_requested, 
        status="pending",
        details="Job is queued."
    )
    background_job_storage.save(new_batch)

    # 2. Create ONE task to represent the job.    
    # If a specific table is requested, it's a simple "execution" task
    if run_request.table_name:
        task = QueuedTask(
            batch_id=batch_id,
            namespace=run_request.namespace,
            table_name=run_request.table_name,
            rules_requested=run_request.rules_requested,
            priority=1,
            run_type="manual"
        )
    else:
        print("not_table")
        # If a namespace or '*' is requested, it's a "generator" task.
        # We signify this by setting table_name=None.
        task = QueuedTask(
            batch_id=batch_id,
            namespace=run_request.namespace,
            table_name=None,
            rules_requested=run_request.rules_requested,
            priority=1, 
            run_type="manual"
        )
        
    # 3. Save the single task
    queued_task_storage.save(task)
        
    return RunResponse(run_id=batch_id)

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