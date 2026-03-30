from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

import database
from models.customer import Customer
from services.ingestion import run_ingestion


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    database.Base.metadata.create_all(bind=database.engine)
    yield


app = FastAPI(
    title="Customer Pipeline Service",
    description="Ingests customer data from Flask mock server into PostgreSQL",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "pipeline-service"}


@app.post("/api/ingest")
def ingest(db: Session = Depends(database.get_db)):
    """
    Fetch all customer data from the Flask mock server and upsert into PostgreSQL.
    Handles pagination automatically.
    """
    try:
        result = run_ingestion(db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@app.get("/api/customers")
def get_customers(
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=10, ge=1, le=100, description="Records per page"),
    db: Session = Depends(database.get_db),
):
    """Return paginated list of customers from the database."""
    total = db.query(func.count(Customer.customer_id)).scalar()
    offset = (page - 1) * limit
    customers = db.query(Customer).offset(offset).limit(limit).all()

    return {
        "data": [customer_to_dict(c) for c in customers],
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit if total else 0,
    }


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(database.get_db)):
    """Return a single customer by ID."""
    customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")
    return {"data": customer_to_dict(customer)}


def customer_to_dict(c: Customer) -> dict:
    return {
        "customer_id": c.customer_id,
        "first_name": c.first_name,
        "last_name": c.last_name,
        "email": c.email,
        "phone": c.phone,
        "address": c.address,
        "date_of_birth": c.date_of_birth.isoformat() if c.date_of_birth else None,
        "account_balance": float(c.account_balance) if c.account_balance is not None else None,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }
