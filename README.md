```markdown
# NSE Stock Pipeline API

A FastAPI backend that powers investor-ready metrics for the **Nairobi Securities Exchange (NSE)**.  
It ingests market data, stores it in Postgres, and exposes clean JSON endpoints for dashboards and analysis.

---

##  Features
- **FastAPI** backend with production-ready endpoints
- **Postgres** database integration (via psycopg2)
- **Investor metrics**: OHLC, volume, returns, volatility
- **Market snapshots**: overview, top gainers/losers, sector performance
- **JSON outputs** designed for frontend dashboards
- **Swagger/OpenAPI docs** auto-generated at `/docs` and `/redoc`

---

##  Project Structure
```
nse-stock-pipeline/
├── app.py              # FastAPI application
├── requirements.txt    # Minimal dependencies
└── README.md           # Project documentation
```

---

##  Environment Variables
Set these in a `.env` file (local) or in Render’s Environment tab:

```env
DB_NAME=defaultdb
DB_USER=avnadmin
DB_PASS=yourpassword
DB_HOST=pg-xxxxxxx.i.aivencloud.com
DB_PORT=14040
```

---

##  Running Locally
```bash
# Clone the repo
git clone https://github.com/mainamuragev/nse-stock-pipeline.git
cd nse-stock-pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run FastAPI server
uvicorn app:app --reload
```

Visit:
- `http://127.0.0.1:8000/` → Landing page  
- `http://127.0.0.1:8000/docs` → Swagger UI  
- `http://127.0.0.1:8000/redoc` → ReDoc

---

## 📊 API Endpoints

### Root
`GET /` → Landing page  
```json
{"message": "NSE Stock Pipeline API is live!"}
```

### Health
`GET /api/health` → Service check  
```json
{"status": "ok"}
```

### Company Metrics
`GET /api/company/{symbol}/metrics/{date}`  
Example: `/api/company/KCB/metrics/2025-12-18`

### Market Overview
`GET /api/market/overview/{date}`  
Example: `/api/market/overview/2025-12-18`

### Top Gainers
`GET /api/top-gainers/{date}`

### Top Losers
`GET /api/top-losers/{date}`

### Volatility Leaders
`GET /api/volatility-leaders/{date}`

### Sector Performance
`GET /api/sector-performance/{date}`

---

##  Collaboration
Backend by **Maina Murage**  
Frontend integration by **[@gregorytechKE](https://twitter.com/gregorytechKE)**  

---
