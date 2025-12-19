from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Root route
@app.get("/")
def read_root():
    return {"message": "NSE Stock Pipeline API is live!"}

# Health check
@app.get("/api/health")
def health_check():
    return {"status": "ok"}

# Database connection helper
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

# Company metrics
@app.get("/api/company/{symbol}/metrics/{date}")
def company_metrics(symbol: str, date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            symbol,
            date,
            open::FLOAT,
            close::FLOAT,
            high::FLOAT,
            low::FLOAT,
            volume::BIGINT,
            returns::FLOAT,
            volatility::FLOAT
        FROM company_metrics
        WHERE symbol = %s AND date = %s
    """, (symbol, date))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"symbol": symbol, "date": date, "metrics": [], "message": "No data available"}
    return {"symbol": symbol, "date": date, "metrics": result}

# Market overview
@app.get("/api/market/overview/{date}")
def market_overview(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            date,
            total_volume::BIGINT,
            advancers::INT,
            decliners::INT,
            unchanged::INT,
            market_cap::FLOAT
        FROM market_overview
        WHERE date = %s
    """, (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "overview": None, "message": "No data available"}
    return {"date": date, "overview": result}

# Top gainers
@app.get("/api/top-gainers/{date}")
def top_gainers(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            date,
            symbol,
            change_pct::FLOAT
        FROM top_gainers
        WHERE date = %s
    """, (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "gainers": [], "message": "No data available"}
    return {"date": date, "gainers": result}

# Top losers
@app.get("/api/top-losers/{date}")
def top_losers(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            date,
            symbol,
            change_pct::FLOAT
        FROM top_losers
        WHERE date = %s
    """, (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "losers": [], "message": "No data available"}
    return {"date": date, "losers": result}

# Volatility leaders
@app.get("/api/volatility-leaders/{date}")
def volatility_leaders(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            date,
            symbol,
            volatility::FLOAT
        FROM volatility_leaders
        WHERE date = %s
    """, (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "volatility_leaders": [], "message": "No data available"}
    return {"date": date, "volatility_leaders": result}

# Sector performance
@app.get("/api/sector-performance/{date}")
def sector_performance(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            date,
            sector,
            change_pct::FLOAT
        FROM sector_performance
        WHERE date = %s
    """, (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    if not result:
        return {"date": date, "sectors": [], "message": "No data available"}
    return {"date": date, "sectors": result}

