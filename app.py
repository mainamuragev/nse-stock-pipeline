from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal
import logging
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# --- DB connection ---
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
        cursor_factory=RealDictCursor
    )

# --- Helpers ---
def json_safe(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def convert_result(result):
    return [{k: json_safe(v) for k, v in row.items()} for row in result]

def query_db(sql, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

def fetch_and_return(sql, params, key, date):
    result = query_db(sql, params)
    if not result:
        return {"date": date, key: [], "message": "No data available"}
    return {"date": date, key: convert_result(result)}

# --- Aggregation functions ---
def update_company_metrics(trade_date: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO company_metrics (symbol, date, open, close, high, low, volume, returns, volatility)
                SELECT c.name, f.trade_date,
                       AVG(f.open_price), AVG(f.close_price),
                       MAX(f.high_price), MIN(f.low_price),
                       SUM(f.volume),
                       (AVG(f.close_price) - AVG(f.open_price)) / AVG(f.open_price),
                       STDDEV(f.close_price)
                FROM fact_daily_price f
                JOIN dim_company c ON f.company_id = c.id
                WHERE f.trade_date = %s
                GROUP BY c.name, f.trade_date
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (trade_date,))
        conn.commit()
    logging.info(f"Company metrics updated for {trade_date}")

def update_market_overview(trade_date: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_overview (date, total_volume, advancers, decliners, unchanged, market_cap)
                SELECT f.trade_date,
                       SUM(f.volume),
                       COUNT(*) FILTER (WHERE f.close_price > f.open_price),
                       COUNT(*) FILTER (WHERE f.close_price < f.open_price),
                       COUNT(*) FILTER (WHERE f.close_price = f.open_price),
                       SUM(f.close_price * f.volume)
                FROM fact_daily_price f
                WHERE f.trade_date = %s
                GROUP BY f.trade_date
                ON CONFLICT (date) DO NOTHING;
            """, (trade_date,))
        conn.commit()
    logging.info(f"Market overview updated for {trade_date}")

# --- Scheduler setup ---
scheduler = BackgroundScheduler()

def scheduled_update():
    today = date.today().isoformat()
    update_company_metrics(today)
    update_market_overview(today)

scheduler.add_job(scheduled_update, "cron", hour=0, minute=0)  # runs daily at midnight
scheduler.start()

# --- Endpoints ---
@app.get("/api/company/{symbol}/metrics/{date}")
def company_metrics(symbol: str, date: str):
    return fetch_and_return(
        "SELECT * FROM company_metrics WHERE symbol = %s AND date = %s",
        (symbol, date),
        "metrics",
        date
    )

@app.get("/api/market/overview/{date}")
def market_overview(date: str):
    return fetch_and_return(
        "SELECT * FROM market_overview WHERE date = %s",
        (date,),
        "overview",
        date
    )

