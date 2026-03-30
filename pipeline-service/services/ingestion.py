import os
import time
import logging
from datetime import date, datetime
from decimal import Decimal

import httpx
import dlt
from dlt.sources.helpers import requests as dlt_requests
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from models.customer import Customer

logger = logging.getLogger(__name__)

MOCK_SERVER_URL = os.getenv("MOCK_SERVER_URL", "http://localhost:5000")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/customer_db")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def wait_for_mock_server(retries: int = 10, delay: float = 2.0):
    """Block until the Flask mock server is reachable."""
    for attempt in range(1, retries + 1):
        try:
            resp = httpx.get(f"{MOCK_SERVER_URL}/api/health", timeout=5.0)
            if resp.status_code == 200:
                logger.info("Mock server is ready.")
                return
        except httpx.RequestError:
            pass
        logger.warning(f"Mock server not ready (attempt {attempt}/{retries}), retrying in {delay}s…")
        time.sleep(delay)
    raise RuntimeError("Mock server did not become ready in time.")


def fetch_all_customers() -> list[dict]:
    """Fetch every customer from Flask, auto-handling pagination."""
    all_customers: list[dict] = []
    page, limit = 1, 50

    with httpx.Client(timeout=30.0) as client:
        while True:
            resp = client.get(
                f"{MOCK_SERVER_URL}/api/customers",
                params={"page": page, "limit": limit},
            )
            resp.raise_for_status()
            body = resp.json()

            data = body.get("data", [])
            all_customers.extend(data)

            if len(all_customers) >= body.get("total", 0) or not data:
                break
            page += 1

    return all_customers


# ---------------------------------------------------------------------------
# dlt pipeline
# ---------------------------------------------------------------------------

@dlt.resource(name="customers", write_disposition="merge", primary_key="customer_id")
def customers_resource(raw_customers: list[dict]):
    """dlt resource that yields coerced customer dicts."""
    for raw in raw_customers:
        yield _coerce(raw)


def run_dlt_pipeline(raw_customers: list[dict]) -> int:
    """
    Use the dlt library to load customers into PostgreSQL.
    dlt handles schema creation and upsert (merge) automatically.
    """
    pipeline = dlt.pipeline(
        pipeline_name="customer_pipeline",
        destination=dlt.destinations.postgres(DATABASE_URL),
        dataset_name="public",  # write into the public schema
    )

    load_info = pipeline.run(customers_resource(raw_customers))
    logger.info("dlt load info: %s", load_info)
    return len(raw_customers)


# ---------------------------------------------------------------------------
# SQLAlchemy upsert (fallback / direct DB path)
# ---------------------------------------------------------------------------

def _coerce(raw: dict) -> dict:
    """Coerce raw JSON types to Python types expected by the DB schema."""
    dob = raw.get("date_of_birth")
    if isinstance(dob, str):
        dob = date.fromisoformat(dob)

    created = raw.get("created_at")
    if isinstance(created, str):
        created = datetime.fromisoformat(created.replace("Z", "+00:00"))

    balance = raw.get("account_balance")
    if balance is not None:
        balance = Decimal(str(balance))

    return {
        "customer_id": raw["customer_id"],
        "first_name": raw["first_name"],
        "last_name": raw["last_name"],
        "email": raw["email"],
        "phone": raw.get("phone"),
        "address": raw.get("address"),
        "date_of_birth": dob,
        "account_balance": balance,
        "created_at": created,
    }


def upsert_customers(db: Session, customers: list[dict]) -> int:
    """Upsert customers via SQLAlchemy — idempotent, safe to re-run."""
    if not customers:
        return 0

    parsed = [_coerce(c) for c in customers]
    stmt = insert(Customer).values(parsed)
    stmt = stmt.on_conflict_do_update(
        index_elements=["customer_id"],
        set_={
            "first_name": stmt.excluded.first_name,
            "last_name": stmt.excluded.last_name,
            "email": stmt.excluded.email,
            "phone": stmt.excluded.phone,
            "address": stmt.excluded.address,
            "date_of_birth": stmt.excluded.date_of_birth,
            "account_balance": stmt.excluded.account_balance,
            "created_at": stmt.excluded.created_at,
        },
    )
    db.execute(stmt)
    db.commit()
    return len(parsed)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_ingestion(db: Session) -> dict:
    """
    Full pipeline:
      1. Wait for Flask mock server
      2. Fetch all pages
      3. Load via dlt (primary) — falls back to SQLAlchemy upsert on error
    """
    wait_for_mock_server()
    raw_customers = fetch_all_customers()

    try:
        count = run_dlt_pipeline(raw_customers)
        method = "dlt"
    except Exception as dlt_err:
        logger.warning("dlt pipeline failed (%s), falling back to SQLAlchemy upsert.", dlt_err)
        count = upsert_customers(db, raw_customers)
        method = "sqlalchemy"

    return {"status": "success", "records_processed": count, "method": method}
