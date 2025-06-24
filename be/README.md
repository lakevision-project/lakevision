# Lakevision Backend (FastAPI)

This is the FastAPI backend of Lakevision. It connects to the Apache Iceberg catalog, handles authentication/authorization, and exposes API endpoints consumed by the frontend.

## Running locally

### Prerequisites

- Python 3.10+
- Environment variables configured via `../my.env`

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Start the server
```bash
set -a; source ../my.env; set +a
PYTHONPATH=app uvicorn app.api:app --reload --port 8000
```