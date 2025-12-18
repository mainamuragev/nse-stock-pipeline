from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Root route for a friendly landing page
@app.get("/")
def read_root():
    return {"message": "NSE Stock Pipeline API is live!"}

# Health check endpoint
@app.get("/api/health")
def health_check():
    return {"status": "ok"}

# Example: connect to Postgres using env vars
def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        cursor_factory=RealDictCursor
    )
    return conn

# Example endpoint: company metrics by date
@app.get("/api/company/{symbol}/metrics/{date}")
def company_metrics(symbol: str, date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM company_metrics
        WHERE symbol = %s AND date = %s
        """,
        (symbol, date)
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return {"symbol": symbol, "date": date, "metrics": result}

# Example endpoint: market overview
@app.get("/api/market/overview/{date}")
def market_overview(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM market_overview
        WHERE date = %s
        """,
        (date,)
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return {"date": date, "overview": result}

