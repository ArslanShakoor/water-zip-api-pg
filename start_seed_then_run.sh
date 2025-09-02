#!/usr/bin/env bash
set -euo pipefail
psql "${DATABASE_URL_SYNC:?missing}" -f schema.sql
python load_from_csv.py --data data/topN_contaminants_ppb.csv --zipmap data/zip_to_pws_all15.csv
exec uvicorn app_pg:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2
