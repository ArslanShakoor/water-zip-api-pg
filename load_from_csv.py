#!/usr/bin/env python3
"""
load_from_csv.py

Purpose
-------
Ingest your curated CSVs into Postgres for the Postgres-backed API.

- Creates tables if they don’t exist: pws, contaminant, measurement, zip_pws
- Loads contaminants/measurements from a "ppb-normalized" CSV
- Loads ZIP→PWS mapping from a CSV
- If the ZIP map CSV is missing and a local generator script
  (build_zip_map_light.py) exists, it auto-generates the map first.

Usage
-----
  # sync driver URL for the loader
  export DATABASE_URL="postgresql://USER@127.0.0.1:5432/water"

  python load_from_csv.py \
    --data   /abs/path/to/top5_ccr_10contaminants_ppb.csv \
    --zipmap /abs/path/to/zip_to_pws_all15.csv

Notes
-----
- It’s OK to use a different (async) URL for the running API.
- PWS name matching is case-sensitive; make sure CSV names match your intent.
"""

import argparse
import os
import sys
import subprocess
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------
# Config & DDL
# ---------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{os.environ.get('USER','postgres')}@127.0.0.1:5432/water")

# Force a sync driver for the loader even if an async URL is provided
if DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)

DDL = """
CREATE TABLE IF NOT EXISTS pws (
  id SERIAL PRIMARY KEY,
  pwsid TEXT UNIQUE,
  name  TEXT UNIQUE NOT NULL,
  state CHAR(2),
  notes TEXT
);

CREATE TABLE IF NOT EXISTS contaminant (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS measurement (
  id BIGSERIAL PRIMARY KEY,
  pws_id INT REFERENCES pws(id),
  contaminant_id INT REFERENCES contaminant(id),
  year INT,
  value_ppb DOUBLE PRECISION,
  basis TEXT,
  source_url TEXT,
  last_updated TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS zip_pws (
  zip CHAR(5),
  pwsid TEXT,
  pws_name TEXT,
  coverage_fraction DOUBLE PRECISION,
  PRIMARY KEY (zip, pws_name)
);
"""

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _coerce_year(v) -> Optional[int]:
    try:
        if pd.isna(v):
            return None
        iv = int(v)
        return iv
    except Exception:
        return None

def _coerce_float(v) -> Optional[float]:
    try:
        if pd.isna(v) or (isinstance(v, str) and not v.strip()):
            return None
        return float(v)
    except Exception:
        return None

def ensure_zipmap(args_zipmap: str):
    """
    If the zipmap CSV is missing but a local generator exists, build it.
    """
    if os.path.exists(args_zipmap):
        return

    script = os.path.join(os.getcwd(), "build_zip_map_light.py")
    if not os.path.exists(script):
        raise FileNotFoundError(
            f"{args_zipmap} not found and generator script missing at {script}. "
            "Either create the ZIP map CSV or place the generator script here."
        )

    print(f"[INFO] {args_zipmap} not found — generating via {script} ...")
    # Make sure generator deps exist
    try:
        import geopandas, shapely, pyogrio, requests, tqdm  # noqa: F401
    except Exception:
        print("[INFO] Installing generator dependencies (one time)...")
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "geopandas", "shapely", "pyogrio", "pandas", "requests", "tqdm"])
    # Run the generator
    subprocess.check_call([sys.executable, script, "--out", args_zipmap])


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Path to ppb-normalized contaminants CSV")
    ap.add_argument("--zipmap", required=True, help="Path to ZIP→PWS CSV ('zip','pws')")
    args = ap.parse_args()

    # Auto-generate ZIP map if missing
    ensure_zipmap(args.zipmap)

    # Connect (sync engine)
    eng = create_engine(DATABASE_URL, future=True)

    # Create tables idempotently
    with eng.begin() as conn:
        conn.execute(text(DDL))

    # -------- Load contaminants/measurements --------
    df = pd.read_csv(args.data)
    required_cols = {"pws","contaminant","value_ppb","basis","year","source_url"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"--data is missing required columns: {sorted(missing)}")

    # Upsert PWS
    unique_pws = sorted(str(x) for x in df["pws"].dropna().unique())
    with eng.begin() as conn:
        for name in unique_pws:
            conn.execute(text(
                "INSERT INTO pws(name) VALUES (:n) ON CONFLICT (name) DO NOTHING"
            ), {"n": name})

    # Upsert contaminants
    unique_conts = sorted(str(x) for x in df["contaminant"].dropna().unique())
    with eng.begin() as conn:
        for cname in unique_conts:
            conn.execute(text(
                "INSERT INTO contaminant(name) VALUES (:n) ON CONFLICT (name) DO NOTHING"
            ), {"n": cname})

    # Build name→id maps
    with eng.begin() as conn:
        pws_rows = conn.execute(text("SELECT id,name FROM pws")).all()
        con_rows = conn.execute(text("SELECT id,name FROM contaminant")).all()
    pws_id_by_name = {r[1]: int(r[0]) for r in pws_rows}
    con_id_by_name = {r[1]: int(r[0]) for r in con_rows}

    # Insert measurements
    m_rows = []
    for _, r in df.iterrows():
        pws_name = str(r["pws"])
        cont_name = str(r["contaminant"])
        pws_id = pws_id_by_name.get(pws_name)
        cont_id = con_id_by_name.get(cont_name)
        if pws_id is None or cont_id is None:
            # Skip rows with unknown references (shouldn't happen if upserts succeeded)
            continue
        m_rows.append({
            "pws_id": pws_id,
            "contaminant_id": cont_id,
            "year": _coerce_year(r.get("year")),
            "value": _coerce_float(r.get("value_ppb")),
            "basis": None if pd.isna(r.get("basis")) else str(r.get("basis")),
            "src": None if pd.isna(r.get("source_url")) else str(r.get("source_url"))
        })

    with eng.begin() as conn:
        conn.execute(text("""
            INSERT INTO measurement (pws_id, contaminant_id, year, value_ppb, basis, source_url)
            VALUES (:pws_id, :contaminant_id, :year, :value, :basis, :src)
        """), m_rows)

    # -------- Load ZIP→PWS map --------
    zmap = pd.read_csv(args.zipmap, dtype={"zip": str})
    if "pws" not in zmap.columns or "zip" not in zmap.columns:
        raise ValueError("--zipmap CSV must have columns: zip,pws")

    zmap["zip"] = zmap["zip"].astype(str).str.zfill(5)
    z_rows = [{"zip": z, "name": n} for z, n in zmap[["zip","pws"]].dropna().itertuples(index=False)]
    with eng.begin() as conn:
        for row in z_rows:
            conn.execute(text("""
                INSERT INTO zip_pws (zip, pwsid, pws_name, coverage_fraction)
                VALUES (:zip, NULL, :name, 1.0)
                ON CONFLICT (zip, pws_name) DO NOTHING
            """), row)

    # -------- Summaries --------
    with eng.begin() as conn:
        counts = {}
        for tbl in ("pws","contaminant","measurement","zip_pws"):
            counts[tbl] = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar_one()
    print("Done.")
    print("Row counts:", counts)


if __name__ == "__main__":
    main()
