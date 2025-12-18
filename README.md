# NSE Stock Pipeline API

FastAPI backend for Nairobi Securities Exchange (NSE) metrics.

## Endpoints
- `/api/company/{symbol}/metrics/{date}`
- `/api/company/{symbol}/history?start=YYYY-MM-DD&end=YYYY-MM-DD`
- `/api/market/overview/{date}`
- `/api/market/trends`
- `/api/top-gainers/{date}`
- `/api/top-losers/{date}`
- `/api/volatility-leaders/{date}`
- `/api/sector-performance/{date}`

## Deployment
Start command:
```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
