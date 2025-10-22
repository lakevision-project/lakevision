import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from datetime import datetime, timezone

# These models are needed to construct mock return values and request bodies
from app.models import BackgroundJob, JobSchedule, StatusResponse

# The 'client' fixture is provided by conftest.py

def test_start_manual_run(client: TestClient):
    """Test starting a new background job."""
    run_request = {
        "namespace": "ns1", "table_name": "table1", "rules_requested": ["RULE_A"]
    }
    
    # Patch the storage object where it's used in the jobs router, and the background task scheduler
    with patch("app.api.jobs.background_job_storage", MagicMock()) as mock_job_storage, \
         patch("fastapi.BackgroundTasks.add_task") as mock_add_task:
        
        response = client.post("/api/start-run", json=run_request)

    assert response.status_code == 202
    run_id = response.json()["run_id"]
    assert isinstance(run_id, str)
    
    mock_job_storage.save.assert_called_once()
    saved_job = mock_job_storage.save.call_args[0][0]
    assert isinstance(saved_job, BackgroundJob)
    assert saved_job.id == run_id
    assert saved_job.status == "pending"

    mock_add_task.assert_called_once()
    args = mock_add_task.call_args[0]
    # The first arg is the function `run_insights_job`, the rest are its arguments
    assert args[1:] == (run_id, "ns1", "table1", ["RULE_A"])

def test_get_run_status_found(client: TestClient):
    """Test getting the status of an existing job."""
    mock_job = BackgroundJob(
        id="job-123", status="running", details="Processing...", 
        namespace="ns1", table_name="table1", rules_requested=[]
    )
    
    with patch("app.api.jobs.background_job_storage", MagicMock()) as mock_job_storage:
        mock_job_storage.get_by_id.return_value = mock_job
        
        response = client.get(f"/run-status/{mock_job.id}")
    
    assert response.status_code == 200
    status = StatusResponse(**response.json())
    assert status.run_id == mock_job.id
    assert status.status == "running"
    mock_job_storage.get_by_id.assert_called_once_with(mock_job.id)

def test_get_run_status_not_found(client: TestClient):
    """Test getting the status of a non-existent job."""
    with patch("app.api.jobs.background_job_storage", MagicMock()) as mock_job_storage:
        mock_job_storage.get_by_id.return_value = None
        response = client.get("/run-status/job-not-found")

    assert response.status_code == 404
    assert response.json()["detail"] == "Run ID not found."

@patch('app.api.jobs.get_storage')
@patch('app.api.jobs.InsightsRunner')
def test_run_insights_job_success(mock_Runner, mock_get_storage):
    """An integration test for the background job function itself."""
    from app.routers.jobs import run_insights_job

    # Setup mocks
    mock_storage = MagicMock()
    mock_job = BackgroundJob(id="job-123", namespace="ns1", table_name="table1", rules_requested=[], status="pending")
    mock_storage.get_by_id.return_value = mock_job
    mock_get_storage.return_value = mock_storage

    # The background task uses a global storage object for the *first* get_by_id call.
    # We must patch this one as well.
    with patch('app.api.jobs.background_job_storage', mock_storage):
        run_insights_job("job-123", "ns1", "table1", ["RULE_A"])

    # Check that the runner was instantiated and used
    mock_Runner.assert_called_once()
    runner_instance = mock_Runner.return_value
    runner_instance.run_for_table.assert_called_once_with("ns1.table1", rule_ids=["RULE_A"])
    
    # Check that the job status was updated to complete
    # The job object is fetched and modified multiple times. We check the last `save` call.
    final_saved_job = mock_storage.save.call_args[0][0]
    assert final_saved_job.status == "complete"
    assert "Job finished successfully" in final_saved_job.details

def test_create_schedule_success(client: TestClient):
    """Test creating a valid job schedule."""
    schedule_request = {
        "namespace": "ns1", 
        "table_name": "table1",
        "cron_schedule": "0 0 * * *", 
        "rules_requested": ["ALL"], 
        "created_by": "testuser"
    }
    with patch("app.api.jobs.schedule_storage", MagicMock()) as mock_schedule_storage:
        response = client.post("/api/schedules", json=schedule_request)

    assert response.status_code == 201
    mock_schedule_storage.save.assert_called_once()
    saved_schedule = mock_schedule_storage.save.call_args[0][0]
    assert isinstance(saved_schedule, JobSchedule)
    assert saved_schedule.cron_schedule == "0 0 * * *"
    assert saved_schedule.created_by == "testuser"

def test_create_schedule_invalid_cron(client: TestClient):
    """Test that an invalid cron schedule returns a 400 error."""
    schedule_request = {
        "namespace": "ns1", "table_name": "table1",
        "cron_schedule": "invalid-cron-string", 
        "rules_requested": ["ALL"], "created_by": "testuser"
    }
    # No need to patch storage, as the endpoint fails before calling it.
    response = client.post("/api/schedules", json=schedule_request)
    assert response.status_code == 400
    assert "Invalid cron_schedule" in response.json()["detail"]

def test_delete_schedule_success(client: TestClient):
    """Test deleting an existing schedule."""
    mock_schedule = JobSchedule(
        id="sched-123", namespace="ns1", table_name="t1", 
        cron_schedule="* * * * *", rules_requested=[],
        next_run_timestamp=datetime.now(timezone.utc), created_by="user"
    )
    with patch("app.api.jobs.schedule_storage", MagicMock()) as mock_schedule_storage:
        mock_schedule_storage.get_by_id.return_value = mock_schedule
        response = client.delete("/api/schedules/sched-123")

    assert response.status_code == 204
    mock_schedule_storage.delete.assert_called_once_with("sched-123")

def test_delete_schedule_not_found(client: TestClient):
    """Test that deleting a non-existent schedule returns 404."""
    with patch("app.api.jobs.schedule_storage", MagicMock()) as mock_schedule_storage:
        mock_schedule_storage.get_by_id.return_value = None
        response = client.delete("/api/schedules/sched-non-existent")

    assert response.status_code == 404
    assert response.json()["detail"] == "Schedule not found."

def test_list_schedules_for_namespace(client: TestClient):
    """Test listing schedules for a specific namespace."""
    with patch("app.api.jobs.schedule_storage", MagicMock()) as mock_schedule_storage:
        mock_schedule_storage.get_by_attributes.return_value = []
        response = client.get("/api/schedules?namespace=ns1")
    
    assert response.status_code == 200
    mock_schedule_storage.get_by_attributes.assert_called_once_with({"namespace": "ns1"})

def test_list_schedules_for_lakehouse(client: TestClient):
    """Test listing schedules for the whole lakehouse."""
    with patch("app.api.jobs.schedule_storage", MagicMock()) as mock_schedule_storage:
        mock_schedule_storage.get_by_attributes.return_value = []
        response = client.get("/api/schedules?namespace=*")
    
    assert response.status_code == 200
    # Check that it correctly queries for schedules with no table_name
    mock_schedule_storage.get_by_attributes.assert_called_once_with({"table_name": None})

