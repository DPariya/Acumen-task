# Backend Developer Technical Assessment

A 3-service data pipeline using Flask, FastAPI, and PostgreSQL — all orchestrated with Docker Compose.

## Architecture

```
Flask Mock Server (5000)  →  FastAPI Pipeline (8000)  →  PostgreSQL (5432)
       ↑                            ↑
  Serves 22 customers        POST /api/ingest
  from JSON file             fetches & upserts all
```

## Prerequisites

- Docker Desktop (running)
- `docker-compose` v2+

## Quick Start

```bash
# 1. Clone / enter the project directory
cd project-root

# 2. Start all services
docker-compose up -d

# 3. Wait ~5 seconds for Postgres to be ready, then ingest data
curl -X POST http://localhost:8000/api/ingest
# → {"status":"success","records_processed":22}
```

## Project Structure

```
project-root/
├── docker-compose.yml
├── README.md
├── mock-server/
│   ├── app.py                  # Flask REST API
│   ├── data/customers.json     # 22 customer records
│   ├── Dockerfile
│   └── requirements.txt
└── pipeline-service/
    ├── main.py                 # FastAPI app + endpoints
    ├── database.py             # SQLAlchemy engine / session
    ├── models/
    │   └── customer.py         # ORM model
    ├── services/
    │   └── ingestion.py        # Fetch → parse → upsert logic
    ├── Dockerfile
    └── requirements.txt
```

## API Reference

### Flask Mock Server — `http://localhost:5000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/customers?page=1&limit=10` | Paginated customer list |
| GET | `/api/customers/{id}` | Single customer by ID |

**Example response (`/api/customers`):**
```json
{
  "data": [...],
  "total": 22,
  "page": 1,
  "limit": 10,
  "pages": 3
}
```

---

### FastAPI Pipeline — `http://localhost:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| POST | `/api/ingest` | Fetch all data from Flask → upsert to DB |
| GET | `/api/customers?page=1&limit=10` | Paginated customers from DB |
| GET | `/api/customers/{id}` | Single customer from DB |
| GET | `/docs` | Auto-generated Swagger UI |

---

## Test Commands

```bash
# --- Flask ---
# Health
curl http://localhost:5000/api/health

# Paginated list
curl "http://localhost:5000/api/customers?page=1&limit=5"

# Single customer
curl http://localhost:5000/api/customers/CUST-001

# 404
curl http://localhost:5000/api/customers/CUST-999

# --- FastAPI ---
# Trigger ingestion
curl -X POST http://localhost:8000/api/ingest

# Paginated customers from DB
curl "http://localhost:8000/api/customers?page=1&limit=5"

# Single customer from DB
curl http://localhost:8000/api/customers/CUST-001

# Swagger UI
open http://localhost:8000/docs
```

## Stopping Services

```bash
docker-compose down          # stop containers
docker-compose down -v       # stop + remove volumes (wipes DB)
```

## Design Decisions

- **dlt as primary ingestion engine** — the `POST /api/ingest` endpoint runs a `dlt` pipeline with `write_disposition="merge"` and `primary_key="customer_id"`, which handles schema creation and upsert natively. SQLAlchemy upsert is kept as an automatic fallback.
- **Auto-pagination** — the ingestion service loops through all Flask pages automatically regardless of dataset size.
- **Healthchecks on both Postgres and mock-server** — `depends_on` with `condition: service_healthy` prevents race conditions at startup. The pipeline service won't start until Flask is confirmed ready.
- **Retry logic** — `wait_for_mock_server()` retries up to 10 times with a 2 s delay inside the ingestion service as a belt-and-suspenders fallback.
- **Idempotent ingest** — safe to call `POST /api/ingest` multiple times; records are upserted, never duplicated.
