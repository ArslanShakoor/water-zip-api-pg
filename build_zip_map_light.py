
#!/usr/bin/env python3
import os, sys, time, zipfile, argparse, json
from pathlib import Path
import requests, pandas as pd, geopandas as gpd
from shapely.geometry import shape
from shapely.ops import unary_union

CENSUS_ZCTA_URL = "https://www2.census.gov/geo/tiger/TIGER2022/ZCTA520/tl_2022_us_zcta520.zip"

PWS_DEFS = [
    {"name":"New York City (DEP)", "queries":["New York City, New York, USA"]},
    {"name":"Los Angeles (LADWP)", "queries":["Los Angeles, California, USA"]},
    {"name":"Chicago", "queries":["Chicago, Illinois, USA"]},
    {"name":"Philadelphia", "queries":["Philadelphia, Pennsylvania, USA"]},
    {"name":"San Diego (City of)", "queries":["San Diego, California, USA"]},
    {"name":"Dallas Water Utilities (DWU)", "queries":["Dallas, Texas, USA"]},
    {"name":"Phoenix", "queries":["Phoenix, Arizona, USA"]},
    {"name":"Baltimore City DPW", "queries":["Baltimore, Maryland, USA"]},
    {"name":"Houston", "queries":["Houston, Texas, USA"]},
    {"name":"San Antonio (SAWS)", "queries":["San Antonio, Texas, USA"]},
    {"name":"Miami-Dade", "queries":["Miami-Dade County, Florida, USA"]},
    {"name":"WSSC", "queries":["Montgomery County, Maryland, USA","Prince George's County, Maryland, USA"]},
    {"name":"Las Vegas Valley Water District (LVVWD)", "queries":[
        "Las Vegas, Nevada, USA","North Las Vegas, Nevada, USA","Henderson, Nevada, USA",
        "Paradise, Clark County, Nevada, USA","Spring Valley, Clark County, Nevada, USA",
        "Enterprise, Clark County, Nevada, USA","Sunrise Manor, Clark County, Nevada, USA",
        "Winchester, Clark County, Nevada, USA","Whitney, Clark County, Nevada, USA"
    ]},
    {"name":"EBMUD (East Bay MUD)", "queries":[
        "Oakland, California, USA","Berkeley, California, USA","Alameda, California, USA",
        "Richmond, California, USA","San Pablo, California, USA","El Cerrito, California, USA",
        "Albany, California, USA","Piedmont, California, USA","Orinda, California, USA",
        "Moraga, California, USA","Lafayette, California, USA",
        "Kensington, Contra Costa County, California, USA","El Sobrante, Contra Costa County, California, USA"
    ]},
    {"name":"MWRA (Full Service)", "queries":[
        "Boston, Massachusetts, USA","Cambridge, Massachusetts, USA","Somerville, Massachusetts, USA",
        "Newton, Massachusetts, USA","Brookline, Norfolk County, Massachusetts, USA",
        "Quincy, Massachusetts, USA","Malden, Massachusetts, USA","Medford, Massachusetts, USA",
        "Chelsea, Massachusetts, USA","Everett, Massachusetts, USA","Revere, Massachusetts, USA",
        "Watertown, Massachusetts, USA","Arlington, Massachusetts, USA","Belmont, Massachusetts, USA",
        "Lexington, Massachusetts, USA","Milton, Massachusetts, USA","Dedham, Massachusetts, USA"
    ]},
]

def ensure_zcta_shapefile(workdir: Path) -> Path:
    out_dir = workdir / "zcta"
    shp = out_dir / "tl_2022_us_zcta520.shp"
    if shp.exists():
        return shp
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = workdir / "zcta.zip"
    print(f"Downloading ZCTAs → {zip_path}")
    with requests.get(CENSUS_ZCTA_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(1<<20):
                if chunk: f.write(chunk)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    return shp

def nominatim_lookup(query: str, cache_dir: Path, sleep_sec: float = 1.0):
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = query.replace(" ", "_").replace(",", "").replace("/", "-")
    cache_file = cache_dir / f"{key}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "jsonv2", "polygon_geojson": 1, "limit": 1}
    headers = {"User-Agent": "zipmap-builder/0.1 (contact: example@example.com)"}
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    cache_file.write_text(json.dumps(data))
    time.sleep(sleep_sec)
    return data

def boundary_from_query(query: str, cache_dir: Path, sleep_sec: float = 1.0):
    data = nominatim_lookup(query, cache_dir, sleep_sec)
    if not data:
        print(f"[WARN] No OSM result for: {query}")
        return None
    for item in data:
        gj = item.get("geojson")
        if not gj: 
            continue
        try:
            geom = shape(gj)
            if not geom.is_valid:
                geom = geom.buffer(0)
            if not geom.is_empty:
                return geom
        except Exception:
            pass
    print(f"[WARN] No polygon geometry for: {query}")
    return None

def build_pws_polygon(pws_def: dict, cache_dir: Path, sleep_sec: float = 1.0):
    geoms = []
    for q in pws_def["queries"]:
        g = boundary_from_query(q, cache_dir, sleep_sec)
        if g is not None and not g.is_empty:
            geoms.append(g)
    if not geoms:
        print(f"[ERROR] Could not assemble polygon for {pws_def['name']}")
        return None
    return unary_union(geoms)

def zctas_for_polygon(shp_path: Path, poly, method: str = "centroid") -> pd.DataFrame:
    z = gpd.read_file(shp_path).to_crs("EPSG:4326")
    minx, miny, maxx, maxy = gpd.GeoSeries([poly], crs="EPSG:4326").total_bounds
    z = z.cx[minx:maxx, miny:maxy]
    if method == "centroid":
        z["centroid"] = z.geometry.centroid
        mask = z["centroid"].within(poly)
    else:
        mask = z.geometry.intersects(poly)
    res = z.loc[mask, ["ZCTA5CE20"]].rename(columns={"ZCTA5CE20":"zip"})
    res["zip"] = res["zip"].astype(str).str.zfill(5)
    return res

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output CSV path (zip,pws)")
    ap.add_argument("--method", default="centroid", choices=["centroid","intersect"])
    ap.add_argument("--workdir", default="./zipmap_work")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    workdir = Path(args.workdir); workdir.mkdir(parents=True, exist_ok=True)
    cache_dir = workdir / "osm_cache"
    shp = ensure_zcta_shapefile(workdir)

    rows = []
    for pws in PWS_DEFS:
        poly = build_pws_polygon(pws, cache_dir, sleep_sec=args.sleep)
        if poly is None or poly.is_empty:
            print(f"[SKIP] {pws['name']}")
            continue
        zips_df = zctas_for_polygon(shp, poly, method=args.method)
        for zip5 in zips_df["zip"].tolist():
            rows.append({"zip": zip5, "pws": pws["name"]})

    pd.DataFrame(rows).drop_duplicates().sort_values(["pws","zip"]).to_csv(args.out, index=False)
    print(f"Wrote {len(rows)} rows → {args.out}")

if __name__ == "__main__":
    main()

