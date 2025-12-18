from fastapi import FastAPI
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file (local dev)
load_dotenv()

app = FastAPI(title="NSE Pipeline API", version="1.3.1")

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        sslmode="require"
    )

# ---------------- Investor Metrics ----------------

@app.get("/api/top-gainers/{date}", tags=["Investor Metrics"])
def top_gainers(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.symbol, c.name, s.daily_return
        FROM snapshot s
        JOIN dim_company c ON s.company_id = c.id
        WHERE s.trade_date = %s
        ORDER BY s.daily_return DESC
        LIMIT 5;
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return [{"symbol": r[0], "name": r[1], "daily_return": float(r[2]) if r[2] is not None else None} for r in rows]

@app.get("/api/top-losers/{date}", tags=["Investor Metrics"])
def top_losers(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.symbol, c.name, s.daily_return
        FROM snapshot s
        JOIN dim_company c ON s.company_id = c.id
        WHERE s.trade_date = %s
        ORDER BY s.daily_return ASC
        LIMIT 5;
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return [{"symbol": r[0], "name": r[1], "daily_return": float(r[2]) if r[2] is not None else None} for r in rows]

@app.get("/api/volatility-leaders/{date}", tags=["Investor Metrics"])
def volatility_leaders(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.symbol, c.name, s.volatility_30d
        FROM snapshot s
        JOIN dim_company c ON s.company_id = c.id
        WHERE s.trade_date = %s
        ORDER BY s.volatility_30d DESC
        LIMIT 5;
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return [{"symbol": r[0], "name": r[1], "volatility_30d": float(r[2]) if r[2] is not None else None} for r in rows]

@app.get("/api/sector-performance/{date}", tags=["Investor Metrics"])
def sector_performance(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.sector, AVG(s.daily_return)
        FROM snapshot s
        JOIN dim_company c ON s.company_id = c.id
        WHERE s.trade_date = %s
        GROUP BY c.sector;
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return [{"sector": r[0], "avg_return": float(r[1]) if r[1] is not None else None} for r in rows]

# ---------------- Company-Level ----------------

@app.get("/api/company/{symbol}/metrics/{date}", tags=["Company"])
def company_metrics(symbol: str, date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.symbol, c.name, s.daily_return, s.avg_7d_close, s.avg_30d_close, s.volatility_30d, s.volume
        FROM snapshot s
        JOIN dim_company c ON s.company_id = c.id
        WHERE c.symbol = %s AND s.trade_date = %s;
    """, (symbol, date))
    row = cur.fetchone()
    conn.close()
    if row:
        return {
            "symbol": row[0],
            "name": row[1],
            "daily_return": float(row[2]) if row[2] is not None else None,
            "avg_7d_close": float(row[3]) if row[3] is not None else None,
            "avg_30d_close": float(row[4]) if row[4] is not None else None,
            "volatility_30d": float(row[5]) if row[5] is not None else None,
            "volume": int(row[6]) if row[6] is not None else None
        }
    return {"error": "No data found"}

@app.get("/api/company/{symbol}/history", tags=["Company"])
def company_history(symbol: str, start: str, end: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.trade_date, s.daily_return, s.avg_7d_close, s.avg_30d_close, s.volatility_30d, s.volume
        FROM snapshot s
        JOIN dim_company c ON s.company_id = c.id
        WHERE c.symbol = %s AND s.trade_date BETWEEN %s AND %s
        ORDER BY s.trade_date;
    """, (symbol, start, end))
    rows = cur.fetchall()
    conn.close()
    return [{
        "date": str(r[0]),
        "daily_return": float(r[1]) if r[1] is not None else None,
        "avg_7d_close": float(r[2]) if r[2] is not None else None,
        "avg_30d_close": float(r[3]) if r[3] is not None else None,
        "volatility_30d": float(r[4]) if r[4] is not None else None,
        "volume": int(r[5]) if r[5] is not None else None
    } for r in rows]

# ---------------- Market-Level ----------------

@app.get("/api/market/overview/{date}", tags=["Market"])
def market_overview(date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT AVG(s.daily_return), AVG(s.avg_30d_close), AVG(s.volatility_30d), SUM(s.volume)
        FROM snapshot s
        WHERE s.trade_date = %s;
    """, (date,))
    row = cur.fetchone()
    conn.close()
    return {
        "avg_return": float(row[0]) if row[0] is not None else None,
        "avg_30d_close": float(row[1]) if row[1] is not None else None,
        "volatility_index": float(row[2]) if row[2] is not None else None,
        "total_volume": int(row[3]) if row[3] is not None else None
    }

@app.get("/api/market/trends", tags=["Market"])
def market_trends(period: str = "30d"):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.trade_date, AVG(s.daily_return), AVG(s.avg_30d_close), AVG(s.volatility_30d), SUM(s.volume)
        FROM snapshot s
        GROUP BY s.trade_date
        ORDER BY s.trade_date DESC
        LIMIT 30;
    """)
    rows = cur.fetchall()
    conn.close()
    return [{
        "date": str(r[0]),
        "avg_return": float(r[1]) if r[1] is not None else None,
        "avg_30d_close": float(r[2]) if r[2] is not None else None,
        "volatility_index": float(r[3]) if r[3] is not None else None,
        "total_volume": int(r[4]) if r[4] is not None else None
    } for r in rows]

# ---------------- Utility ----------------

@app.get("/api/health", tags=["Utility"])
def health():
    return {"status": "ok"}

@app.get("/api/version", tags=["Utility"])
def version():
    return {"version": "1.3.1", "service": "NSE Pipeline API"}

