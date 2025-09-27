from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any
from datetime import datetime, timezone
import dataclasses
from dataclasses import dataclass, field

# --- General ---
class TokenRequest(BaseModel):
    code: str

# --- Background Jobs ---
class RunRequest(BaseModel):
    namespace: str
    table_name: str | None = None
    rules_requested: List[str]

class RunResponse(BaseModel):
    run_id: str
    message: str = "Job accepted and is running in the background."

@dataclass
class BackgroundJob:
    """Represents the state of a background insight run in the database."""
    id: str  # This will be the run_id
    namespace: str
    table_name: Optional[str]
    rules_requested: List[str]
    status: Literal["pending", "running", "complete", "failed"]
    details: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    results: Optional[List[Dict[str, Any]]] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class StatusResponse(BaseModel):
    run_id: str
    status: Literal["pending", "running", "complete", "failed"]
    details: str | None = None
    rules_requested: List[str]
    started_at: datetime | None = None
    finished_at: datetime | None = None
    results: List[Dict[str, Any]] | None = None

    @classmethod
    def from_job(cls, job: BackgroundJob) -> "StatusResponse":
        """Creates a StatusResponse instance from a BackgroundJob dataclass."""
        from dateutil.parser import parse
        job_dict = dataclasses.asdict(job)
        job_dict['run_id'] = job_dict.pop('id')
        if job_dict.get('started_at') and isinstance(job_dict['started_at'], str):
            job_dict['started_at'] = parse(job_dict['started_at'])
        if job_dict.get('finished_at') and isinstance(job_dict['finished_at'], str):
            job_dict['finished_at'] = parse(job_dict['finished_at'])
        return cls(**job_dict)

# --- Scheduling ---
class JobScheduleUpdateRequest(BaseModel):
    namespace: Optional[str] = None
    table_name: Optional[str] = None
    rules_requested: Optional[List[str]] = None
    cron_schedule: Optional[str] = None
    is_enabled: Optional[bool] = None

class JobScheduleRequest(BaseModel):
    namespace: str
    table_name: Optional[str] = None
    rules_requested: List[str]
    cron_schedule: str
    created_by: str

class JobScheduleResponse(JobScheduleRequest):
    id: str
    is_enabled: bool = True
    next_run_timestamp: datetime
    last_run_timestamp: Optional[datetime] = None
    created_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))