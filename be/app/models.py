from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any
from datetime import datetime, timezone
import dataclasses
from dataclasses import dataclass, field
import uuid

from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record

class TokenRequest(BaseModel):
    code: str

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


@dataclass
class JobSchedule:
    """
    Defines a scheduled job for running insights.
    This maps directly to a 'jobschedules' table in your DB.
    """

    namespace: str
    table_name: Optional[str] # NULL if it's a namespace-level job
    rules_requested: List[str]

    cron_schedule: str # e.g., "0 * * * *" (every hour at minute 0)

    next_run_timestamp: datetime

    created_by: str # e.g., user email
    last_run_timestamp: Optional[datetime] = None
    created_timestamp: datetime = field(default_factory=datetime.utcnow)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    is_enabled: bool = True


@dataclass
class Rule:
    id: str
    name: str
    description: str
    method: Any


class RuleOut(BaseModel):
    id: str
    name: str
    description: str

    class Config:
        from_attributes = True


@dataclass
class Insight:
    code: str
    table: str
    message: str
    severity: str
    suggested_action: str


@dataclass
class InsightRun:
    namespace: str
    table_name: str
    rules_requested: List[str]
    run_type: Literal['manual', 'auto']
    results: List[Insight]
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ColumnFilter(BaseModel):
    name: str
    value: str


class TableFile(BaseModel):
    path: str
    format: str
    partition: List[ColumnFilter]
    records: int
    size_bytes: int

    @classmethod
    def from_task(cls, task: FileScanTask):
        re: Record = task.file.partition
        partition = []
        for key, value in re.__dict__.items():
            partition.append(ColumnFilter(name=key, value=str(value)))
        return cls(
            path = task.file.file_path,
            format = task.file.file_format,
            partition = partition,
            records = task.file.record_count,
            size_bytes = task.file.file_size_in_bytes
        )