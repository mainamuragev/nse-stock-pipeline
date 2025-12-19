from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "NSE Stock Pipeline API is live!"}

@app.get("/api/health")
def health_check():
    return {"status": "ok"}

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

# Helper to convert Decimal → float
def json_safe(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def convert_result(result):
    return [{k: json_safe(v) for k, v in row.items()} for row in result]

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": str(exc), "path": str(request.url.path)}
    )

# Company metrics
@app.get("/api/company/{symbol}/metrics/{date}")
def company_metrics(symbol: str, date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM company_metrics WHERE symbol = %s AND date = %s", (symbol, date))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"symbol": symbol, "date": date, "metrics": [], "message": "No data available"}
    return {"symbol": symbol, "date": date, "metrics": convert_result(result)}

# Market overview
@app.get("/api/market/overview/{date}")
def market_overview(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM market_overview WHERE date = %s", (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "overview": None, "message": "No data available"}
    return {"date": date, "overview": convert_result(result)}

# Top gainers
@app.get("/api/top-gainers/{date}")
def top_gainers(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM top_gainers WHERE date = %s", (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "gainers": [], "message": "No data available"}
    return {"date": date, "gainers": convert_result(result)}

# Top losers
@app.get("/api/top-losers/{date}")
def top_losers(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM top_losers WHERE date = %s", (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "losers": [], "message": "No data available"}
    return {"date": date, "losers": convert_result(result)}

# Volatility leaders
@app.get("/api/volatility-leaders/{date}")
def volatility_leaders(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM volatility_leaders WHERE date = %s", (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "volatility_leaders": [], "message": "No data available"}
    return {"date": date, "volatility_leaders": convert_result(result)}

# Sector performance
@app.get("/api/sector-performance/{date}")
def sector_performance(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sector_performance WHERE date = %s", (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "sectors": [], "message": "No data available"}
    return {"date": date, "sectors": convert_result(result)}

