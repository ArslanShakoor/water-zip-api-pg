#!/usr/bin/env python3
"""
app_pg.py — FastAPI service backed by Postgres

Env:
  DATABASE_URL  # asyncpg DSN recommended, e.g. postgresql+asyncpg://user:pass@host:5432/water
                # if you pass a sync DSN (postgresql://...), we'll auto-upgrade it to asyncpg.

Tables expected (created by schema.sql / load_from_csv.py):
  pws(id, pwsid, name, ...)
  contaminant(id, name)
  measurement(id, pws_id, contaminant_id, year, value_ppb, basis, source_url, ...)
  zip_pws(zip, pwsid, pws_name, coverage_fraction)
"""

import os
import re
from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# -------------------------
# Config & engine
# -------------------------

RAW_DSN = os.getenv("DATABASE_URL", "")
if not RAW_DSN:
    # local sane default (will work if you followed the local setup)
    RAW_DSN = f"postgresql+asyncpg://{os.environ.get('USER','postgres')}@127.0.0.1:5432/water"

# Auto-upgrade sync DSN to async DSN if needed
if RAW_DSN.startswith("postgresql://"):
    DATABASE_URL = RAW_DSN.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    DATABASE_URL = RAW_DSN

# Create async engine
async_engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,              # recycle idle conns
    pool_size=5,
    max_overflow=5,
)
Session = async_sessionmaker(async_engine, expire_on_commit=False)

# -------------------------
# FastAPI app
# -------------------------

app = FastAPI(title="Water ZIP→PWS API", version="1.0.0")

# CORS (adjust origins in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production clients
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

ZIP_RE = re.compile(r"^\d{5}$")
MAX_PWS_PER_ZIP = 5

# -------------------------
# Pydantic models
# -------------------------

class Candidate(BaseModel):
    zip: str = Field(..., pattern=r"^\d{5}$")
    pws_name: str
    pwsid: Optional[str] = None
    coverage_fraction: Optional[float] = Field(None, ge=0.0, le=1.0)

class ContaminantRow(BaseModel):
    contaminant: str
    value_ppb: Optional[float] = None
    year: Optional[int] = None
    basis: Optional[str] = None
    source_url: Optional[str] = None

class ZipCandidatesResponse(BaseModel):
    zip: str
    candidates: List[Candidate]

class ContaminantsResponse(BaseModel):
    zip: str
    resolved_pws: Candidate
    latest_year_used: Optional[int] = None
    contaminants: List[ContaminantRow]

# -------------------------
# Helpers
# -------------------------

async def fetch_pws_candidates(zip5: str, limit: int = MAX_PWS_PER_ZIP) -> List[dict]:
    """Return candidate PWS rows for a ZIP ordered by coverage_fraction desc."""
    async with Session() as s:
        res = await s.execute(
            text("""
                SELECT zip, pwsid, pws_name, coverage_fraction
                FROM zip_pws
                WHERE zip = :zip
                ORDER BY coverage_fraction DESC NULLS LAST, pws_name ASC
                LIMIT :lim
            """),
            {"zip": zip5, "lim": limit},
        )
        rows = [
            {
                "zip": r[0],
                "pwsid": r[1],
                "pws_name": r[2],
                "coverage_fraction": float(r[3]) if r[3] is not None else None,
            }
            for r in res.fetchall()
        ]
        return rows

async def resolve_latest_year(pws_name: str) -> Optional[int]:
    async with Session() as s:
        res = await s.execute(
            text("""
                SELECT MAX(m.year)
                FROM measurement m
                JOIN pws p ON p.id = m.pws_id
                WHERE p.name = :pws
            """),
            {"pws": pws_name},
        )
        val = res.scalar_one()
        try:
            return int(val) if val is not None else None
        except Exception:
            return None

async def fetch_top_contaminants(
    pws_name: str,
    top_n: int,
    year: Optional[int],
) -> tuple[List[dict], Optional[int]]:
    """
    If year is None, pick the latest available year for that PWS.
    Return (rows, year_used).
    """
    y = year
    if y is None:
        y = await resolve_latest_year(pws_name)

    async with Session() as s:
        # prefer the given or latest year; fall back to all-time if year is None
        if y is not None:
            sql = text("""
                SELECT c.name as contaminant, m.value_ppb, m.year, m.basis, m.source_url
                FROM measurement m
                JOIN pws p ON p.id = m.pws_id
                JOIN contaminant c ON c.id = m.contaminant_id
                WHERE p.name = :pws AND m.year = :y
                ORDER BY m.value_ppb DESC NULLS LAST, c.name ASC
                LIMIT :lim
            """)
            params = {"pws": pws_name, "y": y, "lim": top_n}
        else:
            # all-time, top by value_ppb
            sql = text("""
                SELECT c.name as contaminant, m.value_ppb, m.year, m.basis, m.source_url
                FROM measurement m
                JOIN pws p ON p.id = m.pws_id
                JOIN contaminant c ON c.id = m.contaminant_id
                WHERE p.name = :pws
                ORDER BY m.value_ppb DESC NULLS LAST, m.year DESC NULLS LAST, c.name ASC
                LIMIT :lim
            """)
            params = {"pws": pws_name, "lim": top_n}

        res = await s.execute(sql, params)
        rows = [
            {
                "contaminant": r[0],
                "value_ppb": float(r[1]) if r[1] is not None else None,
                "year": int(r[2]) if r[2] is not None else None,
                "basis": r[3],
                "source_url": r[4],
            }
            for r in res.fetchall()
        ]
        return rows, y

# -------------------------
# Lifecycle & health
# -------------------------

@app.on_event("startup")
async def _startup():
    # Light sanity ping + set a conservative statement timeout (2s)
    async with async_engine.begin() as conn:
        await conn.execute(text("SELECT 1"))
        # statement_timeout applies only to current session; this is a hint/example.
        # You can set it at the DB level in prod if you prefer.
        # await conn.execute(text("SET statement_timeout = 2000"))

@app.get("/health")
async def health() -> dict:
    return {"ok": True}

@app.get("/readyz")
async def readyz() -> dict:
    try:
        async with async_engine.begin() as conn:
            pws_ct = (await conn.execute(text("SELECT COUNT(*) FROM pws"))).scalar_one()
            zip_ct = (await conn.execute(text("SELECT COUNT(*) FROM zip_pws"))).scalar_one()
        ok = pws_ct > 0 and zip_ct > 0
        return {"ok": ok, "pws_rows": pws_ct, "zip_map_rows": zip_ct}
    except Exception:
        raise HTTPException(status_code=503, detail="DB not ready")

# -------------------------
# API routes
# -------------------------

@app.get("/v1/zip/{zip}/pws", response_model=ZipCandidatesResponse)
async def zip_to_pws(
    zip: str = Path(..., description="5-digit ZIP code"),
    limit: int = Query(5, ge=1, le=20, description="Max candidates to return"),
):
    if not ZIP_RE.match(zip):
        raise HTTPException(status_code=400, detail="ZIP must be 5 digits.")
    candidates = await fetch_pws_candidates(zip, limit)
    if not candidates:
        raise HTTPException(status_code=404, detail=f"No PWS mapping found for ZIP {zip}.")
    return {"zip": zip, "candidates": candidates}

@app.get("/v1/contaminants", response_model=ContaminantsResponse)
async def contaminants_by_zip(
    zip: str = Query(..., min_length=5, max_length=5, description="5-digit ZIP code"),
    top_n: int = Query(10, ge=1, le=50, description="Top-N contaminants by ppb"),
    year: Optional[int] = Query(None, description="Filter to specific year; default latest"),
    pws: Optional[str] = Query(None, description="Override PWS by exact name"),
):
    if not ZIP_RE.match(zip):
        raise HTTPException(status_code=400, detail="ZIP must be 5 digits.")

    # Resolve PWS
    if pws:
        # Caller explicitly chose the PWS; still verify it serves the ZIP (best effort)
        candidates = await fetch_pws_candidates(zip, MAX_PWS_PER_ZIP)
        matched = next((c for c in candidates if c["pws_name"] == pws), None)
        if matched is None:
            # Still allow, but mark coverage as unknown
            resolved = {"zip": zip, "pws_name": pws, "pwsid": None, "coverage_fraction": None}
        else:
            resolved = matched
    else:
        candidates = await fetch_pws_candidates(zip, MAX_PWS_PER_ZIP)
        if not candidates:
            raise HTTPException(status_code=404, detail=f"No PWS mapping found for ZIP {zip}.")
        resolved = candidates[0]  # top coverage

    rows, y_used = await fetch_top_contaminants(resolved["pws_name"], top_n, year)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No contaminant rows found for '{resolved['pws_name']}'"
                   f"{' in year '+str(year) if year else ''}."
        )

    return {
        "zip": zip,
        "resolved_pws": resolved,
        "latest_year_used": y_used,
        "contaminants": rows,
    }
