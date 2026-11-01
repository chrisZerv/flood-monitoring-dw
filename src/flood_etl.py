# =============================================================================
# Flood Monitoring System — ETL Pipeline
# =============================================================================
#
# Main ETL of the warehouse. It pulls Sentinel-1 GFM products from the EODC
# STAC catalog, intersects them with the Greek NUTS-2 regions, computes the
# flooded area and mean likelihood per (day × region), enriches each record
# with Open-Meteo data (rainfall, river discharge) and loads it into
# Fact_Flood with SCD Type 2.
#
# Synchronisation is incremental: the watermark is the most recent date
# already in Fact_Flood, and each run queries the window
# (watermark - 3 days) → today. The 3-day lookback catches EODC revisions of
# products that were already loaded (REVISED).
#
# Every candidate record is classified as one of:
#   NEW       — no current row for this (date, location) pair → insert
#   UNCHANGED — a current row exists with the same values      → skip
#   REVISED   — a current row exists with different values     → supersede (SCD2)
#
# Audit: every run writes a row to etl_run_log with the watermark before/after
# and the counts per category.
#
# -----------------------------------------------------------------------------
# Sources and attribution
# -----------------------------------------------------------------------------
# Two public resources were used:
#
#   • ESA EarthCODE BiDS 2025 tutorial (MIT License,
#     github.com/ESA-EarthCODE/bids2025-tutorial), "STAC and Data Access"
#     section — the STAC search pattern (Client.open → search by collection,
#     bbox and datetime → items).
#
#   • EODC GFM tutorial and documentation (docs.eodc.eu) — how the GFM
#     collection is organised: which assets it exposes (ensemble_flood_extent,
#     ensemble_likelihood) and that a value of 1 means "flooded".
#
# Raster masking uses the standard rasterio.mask API. Everything else (SCD2
# supersede logic, batch commits, watermark sync, event resolution, parallel
# masking with ThreadPoolExecutor, CSV checkpoint for recovery, aggregation
# to NUTS-2 regions) was written for this project.
# =============================================================================

import os
import sys
import time
import logging
import threading
from datetime import datetime, date, timedelta
from collections import defaultdict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
import rasterio
from rasterio.mask import mask as rio_mask
from pystac_client import Client
from sqlalchemy import create_engine, text
from shapely import wkt
from shapely.geometry import box
from shapely.ops import transform as shp_transform
from pyproj import Transformer


# =============================================================================
# CONFIG
# =============================================================================

# --- Database ---------------------------------------------------------------
DB_HOST = os.environ.get("FLOOD_DW_HOST", "localhost")
DB_NAME = os.environ.get("FLOOD_DW_NAME", "flood_dw")
DB_USER = os.environ.get("FLOOD_DW_USER", "root")

# --- Data sources -----------------------------------------------------------
# EODC STAC: home of GFM, the near-real-time flood product of the Copernicus
# Emergency Management Service (Sentinel-1 SAR ensemble).
STAC_API_URL = "https://stac.eodc.eu/api/v1"

# Open-Meteo: free API, no key required. Daily rainfall and river discharge
# at the centroid of each region.
ARCHIVE_API  = "https://archive-api.open-meteo.com/v1/archive"
FLOOD_API    = "https://flood-api.open-meteo.com/v1/flood"

# Bounding box of the whole of Greece — generous enough to include the islands.
GREECE_BBOX  = [19.0, 34.5, 28.5, 42.0]

# --- GFM raster parameters --------------------------------------------------
# From the EODC GFM documentation:
FLOOD_VALUE   = 1     # in ensemble_flood_extent: 1 = flooded pixel
NODATA_VALUE  = 255   # 255 = nodata / outside coverage
PIXEL_AREA_M2 = 400   # each pixel is 20×20 m on the Equi7 grid

# --- Sync parameters --------------------------------------------------------
# Start date used when no watermark exists yet (first run).
DEFAULT_START_DATE      = date(2021, 1, 1)
# How many days behind the watermark each run goes back, to catch products
# that EODC may have revised (REVISED).
WATERMARK_LOOKBACK_DAYS = 3

# --- Early skip filter ------------------------------------------------------
# GFM also returns many "clean" tiles (no or almost no water). Instead of
# opening each raster to find out, we read the same statistics from the STAC
# item properties and drop those items early. This saves hours of processing.
MIN_FLOODED_PIXELS = 1000
MIN_FLOOD_RATIO    = 0.10

# --- Parallelism ------------------------------------------------------------
# Reading GeoTIFFs over HTTP is I/O-bound, so threads help a lot. 4 workers
# were chosen empirically — more made no difference because of the network
# bottleneck.
MAX_WORKERS = 4
BATCH_SIZE  = 50

# --- Checkpoint -------------------------------------------------------------
# Before the final load, the ready-to-write records are saved to CSV. If the
# database connection breaks during the load, the run can continue with
# --resume and skip the extract/transform phases, which take hours.
BASE_DIR        = Path(__file__).resolve().parent
CHECKPOINT_DIR  = BASE_DIR / "checkpoints"
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_FILE = CHECKPOINT_DIR / "records_checkpoint.csv"

# --- Logging ----------------------------------------------------------------
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_filename = LOG_DIR / f"etl_run_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
# rasterio is noisy with warnings; keep only errors.
logging.getLogger("rasterio").setLevel(logging.ERROR)
log = logging.getLogger(__name__)


# =============================================================================
# Database connection
# =============================================================================
#
# The password comes either from a local db_config.py (never committed) or
# from an environment variable, so the code stays free of credentials.
#
# pool_pre_ping=True checks every connection before use — without it, long
# runs failed with "MySQL server has gone away" after a connection had been
# idle for a while. pool_recycle=1800 refreshes connections every 30 minutes.

try:
    from db_config import DB_PASSWORD as db_pwd
except ImportError:
    db_pwd = os.environ.get("FLOOD_DW_PWD")
    if not db_pwd:
        log.error("No DB password found in db_config.py or in FLOOD_DW_PWD.")
        sys.exit(1)

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{db_pwd}@{DB_HOST}/{DB_NAME}",
    pool_size=10, max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
)


# =============================================================================
# Audit log — every run is recorded in etl_run_log
# =============================================================================

def log_run_start(watermark_before):
    """Opens a new etl_run_log row with status RUNNING and returns its run_id."""
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO etl_run_log (run_started, status, watermark_before)
            VALUES (NOW(), 'RUNNING', :wm)
        """), {"wm": watermark_before})
        return result.lastrowid


def log_run_finish(run_id, status, watermark_after=None, **stats):
    """Closes the run's row with the final status and counts."""
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_run_log
            SET run_finished    = NOW(),
                status          = :status,
                watermark_after = :wm,
                items_returned  = :items,
                rows_inserted   = :ins,
                rows_superseded = :sup,
                rows_unchanged  = :unc,
                error_message   = :err
            WHERE run_id = :rid
        """), {
            "status": status, "wm": watermark_after,
            "items":  stats.get("items_returned", 0),
            "ins":    stats.get("rows_inserted", 0),
            "sup":    stats.get("rows_superseded", 0),
            "unc":    stats.get("rows_unchanged", 0),
            "err":    stats.get("error_message"),
            "rid":    run_id,
        })


def get_watermark():
    """
    Watermark = the most recent date already in Fact_Flood (current rows
    only). The next run continues from there.
    """
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT MAX(STR_TO_DATE(CAST(date_key AS CHAR), '%Y%m%d')) AS watermark
            FROM Fact_Flood
            WHERE is_current = 1
        """)).scalar()


def compute_date_range(watermark, max_date):
    """
    Returns the window [start, max_date] to query in STAC.
    Without a watermark (first run) it starts at DEFAULT_START_DATE;
    otherwise it goes WATERMARK_LOOKBACK_DAYS behind the watermark to catch
    revisions.
    """
    if watermark is None:
        start = DEFAULT_START_DATE
        log.info("  Watermark: NONE (first run)")
    else:
        start = watermark - timedelta(days=WATERMARK_LOOKBACK_DAYS)
        log.info(f"  Watermark: {watermark}, lookback {WATERMARK_LOOKBACK_DAYS} days")
    log.info(f"  Window: {start} → {max_date}")
    return start, max_date


# =============================================================================
# Extract — STAC search with early skip
# =============================================================================
#
# The STAC search pattern (Client.open → catalog.search → list items) follows
# the "STAC and Data Access" section of the ESA EarthCODE BiDS 2025 tutorial.

def stac_extract_with_filter(start_date, end_date):
    """
    Searches the GFM collection for items inside the Greece bbox and the
    given time window. Before returning, it drops items without enough flood
    signal — using the STAC properties alone, without opening any raster.
    """
    log.info(f"\n  EXTRACT — STAC search: datetime={start_date}/{end_date}")

    catalog = Client.open(STAC_API_URL)
    search = catalog.search(
        collections=["GFM"],
        bbox=GREECE_BBOX,
        datetime=f"{start_date}/{end_date}",
    )
    all_items = list(search.items())
    log.info(f"    → {len(all_items)} items returned by STAC")

    keep_items = []
    skipped_zero = 0     # practically no water
    skipped_noise = 0    # some water, but below the ratio threshold (noise)

    for item in all_items:
        props = item.properties
        flooded = props.get('flooded_pixels', 0)
        ratio   = props.get('ratio_after_blob_removal', 0.0)

        if flooded < MIN_FLOODED_PIXELS:
            skipped_zero += 1
            continue
        if ratio < MIN_FLOOD_RATIO:
            skipped_noise += 1
            continue
        keep_items.append(item)

    total_skipped = skipped_zero + skipped_noise
    skip_pct = 100 * total_skipped / len(all_items) if all_items else 0

    log.info(f"    → Early skip:")
    log.info(f"        flooded < {MIN_FLOODED_PIXELS}:   {skipped_zero}")
    log.info(f"        ratio < {MIN_FLOOD_RATIO}:        {skipped_noise}")
    log.info(f"        TOTAL:                   {total_skipped} ({skip_pct:.1f}%)")
    log.info(f"        Items for raster processing: {len(keep_items)}")

    return keep_items, len(all_items), total_skipped


# =============================================================================
# Transform — parallel raster masking per region
# =============================================================================
#
# For each GFM item we open the raster (over HTTP), find which Greek regions
# intersect its bbox, clip the raster to each region's polygon and count the
# flooded pixels. flooded_area_km2 = flooded_pixels × 400 m² / 1e6.
#
# Masking uses the standard rasterio.mask API (mask by geometry, read the
# masked array).

_thread_local = threading.local()


def get_transformer(crs_key):
    """
    Cache of pyproj Transformers per CRS — each GFM tile can sit on a
    different Equi7 sub-grid, and building a transformer is expensive.
    Thread-local to avoid race conditions.
    """
    if not hasattr(_thread_local, "transformers"):
        _thread_local.transformers = {}
    if crs_key not in _thread_local.transformers:
        _thread_local.transformers[crs_key] = Transformer.from_crs(
            "EPSG:4326", crs_key, always_xy=True
        )
    return _thread_local.transformers[crs_key]


def process_one_item(item, regions):
    """
    Processes one STAC item: for every region that intersects its bbox,
    computes flooded/floodable pixels and the likelihood sum over the
    flooded pixels.

    Returns a list of tuples ((date, location_key), measures_dict).
    """
    results = []
    try:
        item_bbox = box(*item.bbox)
        item_date = item.datetime.strftime("%Y-%m-%d")

        # First filter: only regions whose bbox intersects the item.
        intersecting = [r for r in regions if r["bbox_wgs84"].intersects(item_bbox)]
        if not intersecting:
            return results

        flood_url = item.assets["ensemble_flood_extent"].href
        like_url  = item.assets["ensemble_likelihood"].href

        with rasterio.open(flood_url) as src_flood, \
             rasterio.open(like_url) as src_like:

            crs_key = str(src_flood.crs)
            transformer = get_transformer(crs_key)

            for region in intersecting:
                # Reproject the NUTS-2 polygon to the raster CRS.
                polygon_raster = shp_transform(
                    transformer.transform, region["polygon_wgs84"]
                )
                # Mask. If the polygon falls outside the raster this raises — skip it.
                try:
                    flood_arr, _ = rio_mask(
                        src_flood, [polygon_raster],
                        crop=True, filled=True, nodata=NODATA_VALUE,
                    )
                except ValueError:
                    continue

                flood_arr = flood_arr[0]
                flooded_px   = int((flood_arr == FLOOD_VALUE).sum())
                floodable_px = int((flood_arr != NODATA_VALUE).sum())
                if floodable_px == 0:
                    continue

                # Likelihood is only needed over the flooded pixels.
                likelihood_sum = 0.0
                if flooded_px > 0:
                    like_arr, _ = rio_mask(
                        src_like, [polygon_raster],
                        crop=True, filled=True, nodata=255,
                    )
                    like_arr = like_arr[0]
                    flood_mask_local = (flood_arr == FLOOD_VALUE)
                    likelihood_sum = float(like_arr[flood_mask_local].sum())

                key = (item_date, region["location_key"])
                results.append((key, {
                    "flooded_pixels":   flooded_px,
                    "floodable_pixels": floodable_px,
                    "likelihood_sum":   likelihood_sum,
                    "source_item":      item.id,
                    "region_code":      region["region_code"],
                    "region_name":      region["region_name"],
                }))
    except Exception as e:
        log.warning(f"    ! Item {item.id}: {e}")
    return results


def transform_parallel(items, regions):
    """
    Runs process_one_item in a ThreadPoolExecutor. One item can produce
    several rows (one per region it intersects). Because a (date, location)
    pair can be covered by several items (overlap at tile edges), the pixel
    counts are summed — otherwise the pair would be duplicated.
    """
    log.info(f"\n  TRANSFORM — parallel masking ({MAX_WORKERS} workers, {len(items)} items)")

    accum = defaultdict(lambda: {
        "flooded_pixels":   0,
        "floodable_pixels": 0,
        "likelihood_sum":   0.0,
        "source_items":     [],
        "region_code":      None,
        "region_name":      None,
    })
    accum_lock = threading.Lock()
    completed_count = 0
    completed_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_one_item, item, regions): item for item in items}

        for future in as_completed(futures):
            try:
                results = future.result()
                with accum_lock:
                    for key, data in results:
                        accum[key]["flooded_pixels"]   += data["flooded_pixels"]
                        accum[key]["floodable_pixels"] += data["floodable_pixels"]
                        accum[key]["likelihood_sum"]   += data["likelihood_sum"]
                        accum[key]["source_items"].append(data["source_item"])
                        accum[key]["region_code"] = data["region_code"]
                        accum[key]["region_name"] = data["region_name"]
            except Exception as e:
                log.warning(f"    ! Future failed: {e}")

            with completed_lock:
                completed_count += 1
                if completed_count % 20 == 0 or completed_count == len(items):
                    log.info(f"    Progress: {completed_count}/{len(items)} items")

    log.info(f"    → {len(accum)} (date × region) pairs aggregated")

    # Build the ready-to-load records from the aggregated pairs.
    records = []
    for (date_str, location_key), data in accum.items():
        if data["floodable_pixels"] == 0:
            continue
        flooded_area_km2 = data["flooded_pixels"] * PIXEL_AREA_M2 / 1_000_000
        flood_ratio = data["flooded_pixels"] / data["floodable_pixels"]
        if data["flooded_pixels"] > 0:
            flood_likelihood = round(
                data["likelihood_sum"] / data["flooded_pixels"] / 100, 4
            )
        else:
            flood_likelihood = None

        records.append({
            "date_key":         int(date_str.replace("-", "")),
            "date_iso":         date_str,
            "location_key":     location_key,
            "region_code":      data["region_code"],
            "flooded_pixels":   data["flooded_pixels"],
            "floodable_pixels": data["floodable_pixels"],
            "flooded_area_km2": round(flooded_area_km2, 4),
            "flood_ratio":      round(flood_ratio, 6),
            "flood_likelihood": flood_likelihood,
            "source_item_ids":  "; ".join(data["source_items"])[:150],
            "data_source":      "GFM",
            "source_collection":"GFM",
        })

    return records


# =============================================================================
# Enrich — rainfall & river discharge from Open-Meteo
# =============================================================================
#
# One archive call and one flood-API call per region that appears in the
# records. The whole date range is fetched at once and then looked up from a
# dict — otherwise the API would be hit once per row.

def enrich_with_meteo(records, regions_by_key):
    if not records:
        return records

    log.info(f"\n  TRANSFORM — Open-Meteo enrichment")

    dates = sorted({r["date_iso"] for r in records})
    date_min, date_max = dates[0], dates[-1]
    location_keys = {r["location_key"] for r in records}
    meteo_cache = {}

    for lk in location_keys:
        region = regions_by_key.get(lk)
        if region is None:
            continue
        lat = float(region["centroid_lat"])
        lon = float(region["centroid_lon"])

        try:
            # Daily precipitation (mm) at the centroid.
            r1 = requests.get(ARCHIVE_API, params={
                "latitude": lat, "longitude": lon,
                "start_date": date_min, "end_date": date_max,
                "daily": "precipitation_sum", "timezone": "auto",
            }, timeout=30)
            rainfall_data = r1.json().get("daily", {})
            rainfall_dict = dict(zip(
                rainfall_data.get("time", []),
                rainfall_data.get("precipitation_sum", []),
            ))

            # River discharge (m³/s) — from the Open-Meteo flood API.
            r2 = requests.get(FLOOD_API, params={
                "latitude": lat, "longitude": lon,
                "start_date": date_min, "end_date": date_max,
                "daily": "river_discharge",
            }, timeout=30)
            disc_data = r2.json().get("daily", {})
            disc_dict = dict(zip(
                disc_data.get("time", []),
                disc_data.get("river_discharge", []),
            ))

            for d in dates:
                meteo_cache[(lk, d)] = (rainfall_dict.get(d), disc_dict.get(d))
        except Exception as e:
            log.warning(f"    ! Open-Meteo error for {region['region_code']}: {e}")
        # Be polite to the free API
        time.sleep(0.3)

    for r in records:
        rainfall, discharge = meteo_cache.get(
            (r["location_key"], r["date_iso"]), (None, None)
        )
        r["rainfall_mm"] = rainfall
        r["river_discharge_m3s"] = discharge

    log.info(f"    → {sum(1 for r in records if r['rainfall_mm'] is not None)}"
             f"/{len(records)} records have rainfall")
    return records


# =============================================================================
# Checkpoint — write/read records to/from CSV
# =============================================================================
#
# Records are saved to CSV before the heavy load. If the database fails
# during the load, the script can be rerun with --resume, skipping extract.

def save_checkpoint(records):
    if not records:
        return
    df = pd.DataFrame(records)
    df.to_csv(CHECKPOINT_FILE, index=False)
    log.info(f"\n  CHECKPOINT — {len(records)} records written:")
    log.info(f"    {CHECKPOINT_FILE}")
    log.info(f"    (after a crash: python flood_etl.py --resume)")


def load_checkpoint():
    if not CHECKPOINT_FILE.exists():
        return None
    log.info(f"\n  CHECKPOINT — found:")
    log.info(f"    {CHECKPOINT_FILE}")
    df = pd.read_csv(CHECKPOINT_FILE)
    records = df.to_dict('records')
    log.info(f"    Loaded {len(records)} records")
    return records


# =============================================================================
# Load — SCD2 with batch commits
# =============================================================================
#
# Each record is compared with the current Fact_Flood row (if any):
#
#   • No current row       → INSERT, valid_from=now, is_current=1      (NEW)
#   • Same values          → do nothing                                 (UNCHANGED)
#   • Different values     → UPDATE the old row to is_current=0,
#                            valid_to=now, and INSERT a new current row (REVISED)
#
# Commit every BATCH_SIZE records — not row by row (slow), and not all at
# once (an error at the end would lose everything).

def load_with_scd2_batched(records, events_df):
    log.info(f"\n  LOAD — SCD Type 2 (commit every {BATCH_SIZE})")

    # Columns compared to decide whether a record has changed.
    COMPARE_COLS = [
        "flooded_area_km2", "flood_likelihood", "flood_ratio",
        "flooded_pixels", "floodable_pixels",
        "rainfall_mm", "river_discharge_m3s",
    ]

    def values_equal(a, b, tol=1e-6):
        """Comparison with a tolerance for floats and handling of NaN/None."""
        a_nan = a is None or (isinstance(a, float) and pd.isna(a))
        b_nan = b is None or (isinstance(b, float) and pd.isna(b))
        if a_nan and b_nan: return True
        if a_nan or b_nan:  return False
        try: return abs(float(a) - float(b)) < tol
        except (TypeError, ValueError): return a == b

    def to_db(x, cast=float):
        """NaN/None → SQL NULL, otherwise cast to the right type."""
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return cast(x)

    def resolve_event_key(date_iso):
        """
        Finds which named event a date belongs to (via the Dim_Event windows).
        Returns None if it falls outside every event.
        """
        d = pd.to_datetime(date_iso).date()
        for _, ev in events_df.iterrows():
            sd = pd.to_datetime(ev["start_date"]).date()
            ed = pd.to_datetime(ev["end_date"]).date()
            if sd <= d <= ed:
                return int(ev["event_key"])
        return None

    INSERT_SQL = text("""
        INSERT INTO Fact_Flood
            (date_key, location_key, event_key,
             flooded_area_km2, flood_likelihood, flood_ratio,
             flooded_pixels, floodable_pixels,
             rainfall_mm, river_discharge_m3s,
             valid_from, valid_to, is_current,
             data_source, source_collection, source_item_id)
        VALUES (:dk, :lk, :ek, :area, :lik, :rat, :fp, :fap,
                :rain, :disc, NOW(), NULL, 1, :ds, :sc, :sid)
    """)
    SELECT_CURRENT_SQL = text("""
        SELECT flood_key, flooded_area_km2, flood_likelihood, flood_ratio,
               flooded_pixels, floodable_pixels, rainfall_mm, river_discharge_m3s
        FROM Fact_Flood
        WHERE date_key = :dk AND location_key = :lk AND is_current = 1
    """)
    SUPERSEDE_SQL = text("""
        UPDATE Fact_Flood SET is_current = 0, valid_to = NOW()
        WHERE flood_key = :fk
    """)

    inserts = supersedes = unchanged = 0
    batch_count = 0

    for batch_start in range(0, len(records), BATCH_SIZE):
        batch = records[batch_start:batch_start + BATCH_SIZE]

        with engine.begin() as conn:
            for r in batch:
                params = {"dk": int(r["date_key"]), "lk": int(r["location_key"])}
                existing = conn.execute(SELECT_CURRENT_SQL, params).mappings().first()

                new_values = {c: r.get(c) for c in COMPARE_COLS}

                if existing is not None:
                    same = all(
                        values_equal(new_values[c], existing[c])
                        for c in COMPARE_COLS
                    )
                    if same:
                        unchanged += 1
                        continue
                    # Values changed — supersede the old row
                    conn.execute(SUPERSEDE_SQL, {"fk": existing["flood_key"]})
                    supersedes += 1

                event_key = resolve_event_key(r["date_iso"])
                conn.execute(INSERT_SQL, {
                    "dk":   int(r["date_key"]),
                    "lk":   int(r["location_key"]),
                    "ek":   event_key,
                    "area": to_db(r["flooded_area_km2"]),
                    "lik":  to_db(r["flood_likelihood"]),
                    "rat":  to_db(r["flood_ratio"]),
                    "fp":   to_db(r["flooded_pixels"], int),
                    "fap":  to_db(r["floodable_pixels"], int),
                    "rain": to_db(r.get("rainfall_mm")),
                    "disc": to_db(r.get("river_discharge_m3s")),
                    "ds":   r.get("data_source", "GFM"),
                    "sc":   r.get("source_collection", "GFM"),
                    "sid":  r.get("source_item_ids"),
                })
                inserts += 1

        batch_count += 1
        if batch_count % 5 == 0:
            log.info(f"    Commit batch {batch_count} "
                     f"({batch_start + len(batch)}/{len(records)})")

    log.info(f"    → NEW:       {inserts}")
    log.info(f"    → REVISED:   {supersedes}")
    log.info(f"    → UNCHANGED: {unchanged}")
    return inserts, supersedes, unchanged


# =============================================================================
# Helper — load the Greek NUTS-2 regions from Dim_Location
# =============================================================================

def load_regions():
    """
    Loads the 13 NUTS-2 regions with their geometry (WKT) and bbox — the
    bbox is used for a fast intersection check before masking.
    """
    regions_df = pd.read_sql("""
        SELECT location_key, region_code, region_name,
               centroid_lat, centroid_lon, geometry_wkt
        FROM Dim_Location ORDER BY region_code
    """, engine)

    regions = []
    by_key = {}
    for _, row in regions_df.iterrows():
        poly = wkt.loads(row["geometry_wkt"])
        rec = {
            "location_key":  int(row["location_key"]),
            "region_code":   row["region_code"],
            "region_name":   row["region_name"],
            "centroid_lat":  float(row["centroid_lat"]),
            "centroid_lon":  float(row["centroid_lon"]),
            "polygon_wgs84": poly,
            "bbox_wgs84":    box(*poly.bounds),
        }
        regions.append(rec)
        by_key[rec["location_key"]] = rec
    return regions, by_key


# =============================================================================
# Main
# =============================================================================

def main(max_date=None, resume=False):
    start_time = time.time()
    if max_date is None:
        max_date = date.today()

    log.info("=" * 76)
    log.info(f"Flood ETL — Started {datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info(f"Settings: workers={MAX_WORKERS}, batch={BATCH_SIZE}, "
             f"flooded_px≥{MIN_FLOODED_PIXELS}, ratio≥{MIN_FLOOD_RATIO}")
    if resume:
        log.info("RESUME mode — from checkpoint")
    log.info("=" * 76)

    watermark_before = get_watermark()
    run_id = log_run_start(watermark_before)
    log.info(f"  Run ID: {run_id}")

    try:
        # ===== RESUME path: load only, from the checkpoint =================
        if resume:
            records = load_checkpoint()
            if records is None:
                raise FileNotFoundError(
                    f"--resume given but no checkpoint found: {CHECKPOINT_FILE}"
                )
            log.info("\n[Phases 1-6 skipped — using checkpoint]")

            events_df = pd.read_sql(
                "SELECT event_key, event_name, start_date, end_date FROM Dim_Event",
                engine,
            )

            log.info("\n[Phase 7] Load (SCD2 + batches)")
            ins, sup, unc = load_with_scd2_batched(records, events_df)

            watermark_after = get_watermark()
            log_run_finish(
                run_id, status='SUCCESS',
                watermark_after=watermark_after,
                rows_inserted=ins, rows_superseded=sup, rows_unchanged=unc,
            )

            elapsed = time.time() - start_time
            log.info("=" * 76)
            log.info(f"  Watermark: {watermark_before} → {watermark_after}")
            log.info(f"  DB: NEW={ins}, REVISED={sup}, UNCHANGED={unc}")
            log.info(f"  Elapsed: {elapsed:.1f}s")
            log.info(f"✓ Resume finished {datetime.now():%Y-%m-%d %H:%M:%S}")
            log.info("=" * 76)
            return 0

        # ===== Full pipeline ================================================
        log.info("\n[Phase 1] Computing the incremental window")
        start_date, end_date = compute_date_range(watermark_before, max_date)

        log.info("\n[Phase 2] Loading regions")
        regions, regions_by_key = load_regions()
        log.info(f"  → {len(regions)} regions")

        log.info("\n[Phase 3] Loading Dim_Event")
        events_df = pd.read_sql(
            "SELECT event_key, event_name, start_date, end_date FROM Dim_Event",
            engine,
        )

        log.info("\n[Phase 4] Extract + early skip")
        items_to_process, total_items, skipped = stac_extract_with_filter(
            start_date, end_date
        )

        if not items_to_process:
            log.info("\n  No items left after the skip filter — nothing to write")
            log_run_finish(run_id, status='SUCCESS',
                          watermark_after=watermark_before,
                          items_returned=total_items)
            return 0

        log.info("\n[Phase 5] Transform (parallel masking)")
        records = transform_parallel(items_to_process, regions)

        if not records:
            log.info("\n  No region intersections — nothing to write")
            log_run_finish(run_id, status='SUCCESS',
                          watermark_after=watermark_before,
                          items_returned=total_items)
            return 0

        log.info("\n[Phase 6] Open-Meteo enrichment")
        records = enrich_with_meteo(records, regions_by_key)

        # Checkpoint before the heavy load — safety net.
        save_checkpoint(records)

        log.info("\n[Phase 7] Load (SCD2 + batches)")
        ins, sup, unc = load_with_scd2_batched(records, events_df)

        log.info("\n[Phase 8] Closing the audit record")
        watermark_after = get_watermark()
        log_run_finish(
            run_id, status='SUCCESS',
            watermark_after=watermark_after,
            items_returned=total_items,
            rows_inserted=ins, rows_superseded=sup, rows_unchanged=unc,
        )

        elapsed = time.time() - start_time
        log.info("=" * 76)
        log.info(f"  Watermark: {watermark_before} → {watermark_after}")
        log.info(f"  Stats: total={total_items}, skipped={skipped} "
                 f"({100*skipped/total_items:.1f}%), processed={len(items_to_process)}")
        log.info(f"  DB: NEW={ins}, REVISED={sup}, UNCHANGED={unc}")
        log.info(f"  Elapsed: {elapsed:.1f}s ({elapsed/60:.1f} min)")
        log.info(f"✓ Finished {datetime.now():%Y-%m-%d %H:%M:%S}")
        log.info("=" * 76)

        log.info(f"\n  Checkpoint kept at {CHECKPOINT_FILE}")
        return 0

    except Exception as e:
        log.exception(f"\n✗ ETL FAILED: {e}")
        log_run_finish(run_id, status='FAILED', error_message=str(e)[:1000])
        if CHECKPOINT_FILE.exists():
            log.info(f"\n  Checkpoint exists: {CHECKPOINT_FILE}")
            log.info(f"  Resume: python flood_etl.py --resume")
        return 1


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--resume" in args:
        sys.exit(main(resume=True))
    elif len(args) > 0:
        # max_date from the CLI (YYYY-MM-DD) — useful for backfills.
        max_date_arg = date.fromisoformat(args[0])
        log.info(f"Using max_date from CLI: {max_date_arg}")
        sys.exit(main(max_date=max_date_arg))
    else:
        sys.exit(main())
