# Airflow on Docker Compose — Setup Guide

**Master Class DataOps for Modern Data Platforms · Session 2/11**

This guide walks you through standing up a local Airflow stack with Docker Compose. The same stack is reused throughout the rest of the course, so keep it around once it works.

## 1. Stack

| Component | Tool | Port |
|---|---|---|
| Orchestrator | Apache Airflow 2.9.3 | — |
| Executor | Celery Executor | — |
| Airflow UI | Webserver | `8080` (login `airflow` / `airflow`) |
| Message broker | Redis 7.2 | 6379 (internal) |
| Metadata DB | PostgreSQL 13 | 5432 (internal) |
| Worker/queue monitoring | Flower | `5555` (behind a profile, see step 3) |
| Deployment | Docker Compose | — |

## 2. Prerequisites

- Docker Desktop (or Docker Engine + Compose v2) installed and running.
- At least **4GB RAM** allocated to Docker (Docker Desktop → Settings → Resources). Less than that and services will die randomly.
- Ports `8080` and `5555` free on your machine.

## 3. Start the stack

```bash
# 1. Go to the lab directory
cd session_02_airflow_intro/lab

# 2. Create .env from the template
#    Linux: set AIRFLOW_UID to the output of `id -u`
#    macOS/Windows: keep the defaults
cp .env.example .env

# 3. Start the stack — NOTE: --profile flower is required for the Flower UI
docker compose --profile flower up -d

# 4. Wait ~1-2 minutes, then check: every service must be (healthy)
docker compose ps
```

The `airflow-init` container runs once (DB migration + admin user creation) and exits with code 0 — that is expected, not a failure.

## 4. Verify

Open both UIs in your browser:

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Flower | http://localhost:5555 | — |

The stack is healthy when:

- `docker compose ps` shows all services `(healthy)` and `airflow-init` as `Exited (0)`.
- The Airflow UI loads with no warning banner about the scheduler.
- Flower shows at least one worker online.

## 5. Day-to-day operations

```bash
# Stop the stack (keeps all state — recommended between sessions)
docker compose --profile flower stop

# Start it again
docker compose --profile flower start

# Tear down containers (keeps the metadata DB volume)
docker compose --profile flower down

# Full reset — also deletes the metadata DB (users, run history)
docker compose --profile flower down -v
```

DAG files are mounted from `dags/` into the containers — edit a file and the scheduler picks it up within ~30 seconds. No restart needed.

## 6. Configuration

`.env` (created from `.env.example`):

```bash
AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3
AIRFLOW_UID=50000                    # Linux: replace with the output of `id -u`
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=airflow   # Airflow UI login
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS=        # leave empty — build a custom image if you need extra libs
```

## 7. Directory layout

```
lab/
├── README.md              # this file
├── docker-compose.yaml    # Airflow 2.9.3 + Celery + Redis + Postgres + Flower
├── .env.example
├── dags/                  # mounted into the containers as /opt/airflow/dags
│   └── common/
├── logs/                  # auto-generated, gitignored
├── plugins/
└── config/
```

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Permission errors on `logs/` (Linux) | `.env` missing or `AIRFLOW_UID` not set | `cp .env.example .env`, set `AIRFLOW_UID=$(id -u)`, restart |
| No Flower at :5555 | Started without `--profile flower` | `docker compose --profile flower up -d` |
| 502 / connection refused right after startup | `airflow-init` hasn't finished | Wait, then confirm with `docker compose ps` before opening the UI |
| Services randomly unhealthy or restarting | Docker has < 4GB RAM | Increase in Docker Desktop → Settings → Resources |
| DAG code changes not visible | Nothing — it just takes a moment | Wait ~30s; do NOT `docker compose restart` everything |
| Container/volume name conflicts with other course sessions | Another session's stack ran under the same Compose project name | Project name is pinned via `name:` in `docker-compose.yaml`; bring down stale stacks with `docker compose -p <name> down` |

## 9. Health reference

| What | Where |
|---|---|
| Per-service health | `docker compose ps` (STATUS column — healthy) |
| Task logs | Airflow UI → click a task → Logs |
| Worker status, worker count | Flower → Workers tab |
| Queued tasks | Flower → Tasks / Queues tab |
| Scheduler health | `curl http://localhost:8974/health` (inside the container) or the Airflow UI warning banner |
