# New Directory Starter (Postgres-backed API)

## 1) Create a fresh folder & unzip
```bash
mkdir -p ~/water-zip-api-pg && cd ~/water-zip-api-pg
# download the zip I gave you, then:
unzip ~/Downloads/pg-api-starter-newdir.zip
```

## 2) Virtualenv & install
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 3) Point to your Postgres
Make sure the DB from our crosswalk bundle is running (or your own Postgres).
```bash
export DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/water"
```

## 4A) Quick demo seed (so the API works immediately)
```bash
psql "$DATABASE_URL" -f seed_quick.sql
```

## 4B) (Preferred) Load your real CSVs
```bash
# Use your curated CSV + mapping
python load_from_csv.py --data /path/to/top5_ccr_10contaminants_ppb.csv --zipmap /path/to/zip_to_pws_expanded.csv
```

## 5) Run the API
```bash
uvicorn app_pg:app --reload --port 8001
```

## 6) Try it
- http://127.0.0.1:8001/health
- http://127.0.0.1:8001/v1/zip/21201/pws
- http://127.0.0.1:8001/v1/contaminants?zip=21201&top_n=10
