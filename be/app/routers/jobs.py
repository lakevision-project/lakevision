import uuid
import traceback
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response, status
from croniter import croniter

from app.dependencies import lv, background_job_storage, schedule_storage
from app.insights.runner import InsightsRunner
from app.models import JobSchedule
from app.storage import get_storage
from app.models import (
    RunRequest, RunResponse, StatusResponse, BackgroundJob, InsightRun, 
    JobScheduleRequest, JobScheduleResponse, JobScheduleUpdateRequest,
    InsightRecord, ActiveInsight
)

router = APIRouter()

# --- Background Task Logic ---
def run_insights_job(run_id: str, namespace: str, table_name: str | None, rules: List[str]):
    job = background_job_storage.get_by_id(run_id)
    if not job:
        print(f"Error: Job {run_id} not found.")
        return

    job.status = "running"
    job.started_at = datetime.now(timezone.utc)
    target = f"{namespace}.{table_name}" if table_name else namespace
    job.details = f"Processing {len(rules)} rules for '{target}'"
    background_job_storage.save(job)
    print(job.details)
    run_storage = get_storage(model=InsightRun)
    insights_storage = get_storage(model=InsightRecord)
    active_insights_storage = get_storage(model=ActiveInsight)
    try:
        run_storage.connect()
        insights_storage.connect()
        active_insights_storage.connect()
        run_storage.ensure_table()
        insights_storage.ensure_table()
        active_insights_storage.ensure_table()
        runner = InsightsRunner(lv, run_storage, insights_storage, active_insights_storage)
        if namespace=="*":
            results = runner.run_for_lakehouse(rule_ids=rules)
        elif table_name:
            results = runner.run_for_table(f"{namespace}.{table_name}", rule_ids=rules)
        else:
            results = runner.run_for_namespace(namespace, rule_ids=rules)
        
        results_list = [res.__dict__ for res in results]

        job = background_job_storage.get_by_id(run_id)
        job.status = "complete"
        job.finished_at = datetime.now(timezone.utc)
        job.details = "Job finished successfully."
        job.results = results_list
        background_job_storage.save(job)
        print(f"Job {run_id} completed successfully.")

    except Exception as e:
        print(traceback.format_exc())
        job = background_job_storage.get_by_id(run_id)
        job.status = "failed"
        job.finished_at = datetime.now(timezone.utc)
        job.details = f"An error occurred: {str(e)}"
        background_job_storage.save(job)
        print(f"Job {run_id} failed: {e}")
    finally:
        run_storage.disconnect()
        active_insights_storage.disconnect()
        insights_storage.disconnect()

# --- Manual Job Endpoints ---
@router.post("/api/start-run", response_model=RunResponse, status_code=202)
def start_manual_run(run_request: RunRequest, background_tasks: BackgroundTasks):
    run_id = str(uuid.uuid4())
    new_job = BackgroundJob(
        id=run_id, namespace=run_request.namespace, table_name=run_request.table_name,
        rules_requested=run_request.rules_requested, status="pending", details="Job is queued."
    )
    background_job_storage.save(new_job)
    background_tasks.add_task(run_insights_job, run_id, run_request.namespace, run_request.table_name, run_request.rules_requested)
    return RunResponse(run_id=run_id)

@router.get("/run-status/{run_id}", response_model=StatusResponse)
def get_run_status(run_id: str):
    job = background_job_storage.get_by_id(run_id)
    if not job:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return StatusResponse.from_job(job)

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
    print(namespace)
    if namespace == "*":
        print("true")
        criteria = {"table_name": None}
    else:
        print("false")
        # Otherwise, use the original logic.
        criteria = {"namespace": namespace}
        if table_name:
            criteria["table_name"] = table_name
            
    return schedule_storage.get_by_attributes(criteria)