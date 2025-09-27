import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

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