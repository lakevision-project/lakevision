![License](https://img.shields.io/github/license/lakevision-project/lakevision)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)
![GitHub stars](https://img.shields.io/github/stars/lakevision-project/lakevision?style=social)

# Lakevision

Lakevision is a tool that provides insights into your Data Lakehouse based on the **Apache Iceberg** table format.

It lists every namespace and table in your Lakehouse—along with each table’s schema, properties, snapshots, partitions, sort-orders, references/tags, and sample data—and supports nested namespaces. This helps you quickly understand data layout, file locations, and change history.

Lakevision is built with **pyiceberg**, a `FastAPI` backend, and a `SvelteKit` frontend, keeping other dependencies to a minimum.







https://github.com/user-attachments/assets/b6b2eef5-9f27-40ca-a80d-27b88d4a8cfd



## Features

* Search and view all namespaces in your Lakehouse
* Search and view all tables in your Lakehouse
* Display schema, properties, partition specs, and a summary of each table
* Show record count, file count, and size per partition
* List all snapshots with details
* Graphical summary of record additions over time
* OIDC/OAuth-based authentication support
* Pluggable authorization
* Optional “Chat with Lakehouse” capability

## 🚀 Quick Start (Docker)

The easiest way to run Lakevision is with Docker.

1. **Clone the repository** and `cd` into the project root.

2. **Build the image**

   ```bash
   docker build -t lakevision:1.0 .
   ```

3. **Configure environment variables**

   Copy `my.env` to `.env`:

   ```bash
   cp my.env .env
   ```

   Then edit `.env` to provide your Iceberg catalog URL, authentication, and (optionally) AWS or GCP credentials. This prevents accidental changes to `my.env` from being committed to version control.

4. **Run the container**

   ```bash
   docker run --env-file .env -p 8081:8081 lakevision:1.0 /app/start.sh
   ```

Once started, the backend listens on port 8000 and Nginx runs on port 8081. Visit [http://localhost:8081](http://localhost:8081) to explore the UI.

> ✅ Tested on Linux and macOS with the Iceberg REST catalog. Other PyIceberg-compatible catalogs should work too.

## 🛠️ Running Locally (Terminal or VS Code)

### Prerequisites

* Python 3.10+
* Node.js 18+
* A running Iceberg catalog

### 🔀 With Makefile (recommended)

```bash
make init-be       # Set up Python backend
make init-fe       # Install frontend dependencies
make run-be        # Start backend (FastAPI)
make run-fe        # Start frontend (SvelteKit)
make help          # List all Makefile commands
```

Once running, visit [http://localhost:8081](http://localhost:8081) to use the app.

### ⚙️ Manual Setup (for advanced use)

#### 1. Configure environment

Start by copying `my.env` to `.env`:

```bash
cp my.env .env
```

Then edit `.env` to provide values for your Iceberg catalog URL, authentication, and (optionally) AWS or GCP credentials.
This avoids modifying `my.env`, which is version-controlled and used as a template.

> 💡 **Frontend note:**
> All environment variables that begin with `PUBLIC_` must be available in a separate `.env` file inside the `/fe` folder.
> You can do this manually, or by running:

```bash
make prepare-fe-env
```

This ensures the frontend build system (Vite) can access the variables during development.

#### 2. Backend

```bash
cd be
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a; source ../.env; set +a
PYTHONPATH=app uvicorn app.api:app --reload --port 8000
```

#### 3. Frontend

```bash
cd ../fe
npm install
npm run dev -- --port 8081
```

## 🧭 Roadmap

* Chat with Lakehouse capability using an LLM
* Table-level reports (most snapshots, partitions, columns, size, etc.)
* Optimization recommendations
* Limited SQL capabilities ✅
* Partition details (name, file count, records, size) ✅
* Sample data by partition ✅
* Table-level insights
* Time-travel queries

## 🤝 Contributing

Contributions are welcome!

1. **Fork** the repository and clone it locally.
2. **Create** a branch for your change, referencing an issue if one exists.
3. **Add tests** for new functionality where appropriate.
4. **Open a pull request** with a clear description of the changes.
