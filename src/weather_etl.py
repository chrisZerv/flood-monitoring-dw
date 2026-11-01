# =============================================================================
# Flood Monitoring System — Weather ETL (Fact_Weather backfill)
# =============================================================================
#
# Second fact table of the warehouse: daily weather (precipitation and
# temperature) per region. Together with Fact_Flood it forms a fact
# constellation over the shared Dim_Date and Dim_Location.
#
# Why a separate fact and not columns inside Fact_Flood:
# Fact_Flood has a "per Sentinel-1 overpass" grain — rows exist only on days
# a satellite passed over. Weather exists every day. In the same table it
# would be missing for 90%+ of days. As a separate table it has full calendar
# coverage (1,461 days × 13 regions = 18,993 rows), which allows antecedent
# rainfall analysis and similar.
#
# The script is idempotent: INSERT ... ON DUPLICATE KEY UPDATE on the
# UNIQUE (date_key, location_key). It can be rerun without duplicates.
#
# -----------------------------------------------------------------------------
# Source
# -----------------------------------------------------------------------------
# Open-Meteo Historical Weather Archive API — free, no API key.
# https://open-meteo.com/en/docs/historical-weather-api
# =============================================================================

import os
import sys
import time
import logging

import requests
from sqlalchemy import create_engine, text


# =============================================================================
# CONFIG
# =============================================================================

DB_HOST = os.environ.get("FLOOD_DW_HOST", "localhost")
DB_NAME = os.environ.get("FLOOD_DW_NAME", "flood_dw")
DB_USER = os.environ.get("FLOOD_DW_USER", "root")

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"

# Backfill range — the whole study period
START_DATE = "2021-01-01"
END_DATE   = "2024-12-31"

# Variables requested from the API. Only what is used: rainfall and daily
# temperature extremes. Other variables (wind, humidity etc.) are left out so
# the table does not grow with unused fields.
DAILY_VARS = ["precipitation_sum", "temperature_2m_max", "temperature_2m_min"]

# Delay between calls — the API is free, so we keep the request rate low.
REQUEST_DELAY_SEC = 1.5


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("weather_etl")


# =============================================================================
# Connection — same pattern as flood_etl.py
# =============================================================================

try:
    from db_config import DB_PASSWORD as db_pwd
except ImportError:
    db_pwd = os.environ.get("FLOOD_DW_PWD")
    if not db_pwd:
        log.error("No DB password found in db_config.py or in FLOOD_DW_PWD.")
        sys.exit(1)

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{db_pwd}@{DB_HOST}/{DB_NAME}",
    pool_pre_ping=True,
    pool_recycle=1800,
)


# =============================================================================
# Extract — one call per region, at its centroid
# =============================================================================
#
# Centroid-based sampling was chosen (rather than e.g. averaging several
# points per region). It is simple and sufficient for this level of analysis,
# but it is a known limitation: the centroid of Thessaly lies in the plain,
# while the Pindus mountains, which received the most rain during Storm
# Daniel, are not sampled. It is a conservative estimate of the rainfall a
# region receives.

def get_locations():
    """Reads the 13 regions and their centroids from Dim_Location."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT location_key, region_name, centroid_lat, centroid_lon
            FROM Dim_Location
            ORDER BY location_key
        """)).fetchall()
    log.info("Found %d regions", len(rows))
    return rows


def fetch_weather(lat, lon):
    """Calls the Open-Meteo Archive API for one centroid, for the whole range."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date":   END_DATE,
        "daily":      ",".join(DAILY_VARS),
        "timezone":   "Europe/Athens",
    }
    r = requests.get(ARCHIVE_API, params=params, timeout=120)
    r.raise_for_status()
    return r.json().get("daily", {})


# =============================================================================
# Transform — from JSON to rows ready for INSERT
# =============================================================================

def build_rows(location_key, daily):
    """Converts the API's daily response into a list of row dicts."""
    dates  = daily.get("time", [])
    precip = daily.get("precipitation_sum", [])
    tmax   = daily.get("temperature_2m_max", [])
    tmin   = daily.get("temperature_2m_min", [])

    rows = []
    for i, d in enumerate(dates):
        # date_key as YYYYMMDD (same convention as the rest of the warehouse)
        date_key = int(d.replace("-", ""))
        rows.append({
            "date_key":         date_key,
            "location_key":     location_key,
            "precipitation_mm": precip[i] if i < len(precip) else None,
            "temp_max_c":       tmax[i]   if i < len(tmax)   else None,
            "temp_min_c":       tmin[i]   if i < len(tmin)   else None,
        })
    return rows


# =============================================================================
# Load — idempotent upsert
# =============================================================================
#
# INSERT ... ON DUPLICATE KEY UPDATE on the table's UNIQUE (date_key,
# location_key):
#   • First run       → plain INSERT
#   • Later runs      → UPDATE of the same rows (if the API values changed)
# No SCD2 here — historical precipitation is not revised the way flood
# products can be revised by EODC.

UPSERT_SQL = text("""
    INSERT INTO Fact_Weather
        (date_key, location_key, precipitation_mm, temp_max_c, temp_min_c)
    VALUES
        (:date_key, :location_key, :precipitation_mm, :temp_max_c, :temp_min_c)
    ON DUPLICATE KEY UPDATE
        precipitation_mm = VALUES(precipitation_mm),
        temp_max_c       = VALUES(temp_max_c),
        temp_min_c       = VALUES(temp_min_c),
        last_updated     = NOW()
""")


def load_rows(rows):
    """Loads one batch of rows."""
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)


# =============================================================================
# Main
# =============================================================================

def main():
    log.info("=" * 60)
    log.info("Weather ETL — Fact_Weather backfill (%s → %s)", START_DATE, END_DATE)
    log.info("=" * 60)

    locations = get_locations()
    total_rows = 0

    for loc in locations:
        location_key = loc.location_key
        region       = loc.region_name
        lat, lon     = float(loc.centroid_lat), float(loc.centroid_lon)

        log.info("[%2d/%d] %-32s ...", location_key, len(locations), region)

        try:
            daily = fetch_weather(lat, lon)
        except Exception as exc:
            log.error("   API error for %s: %s", region, exc)
            continue

        rows = build_rows(location_key, daily)
        if not rows:
            log.warning("   No data returned")
            continue

        load_rows(rows)
        total_rows += len(rows)
        log.info("   → %d days loaded", len(rows))

        time.sleep(REQUEST_DELAY_SEC)

    log.info("-" * 60)
    log.info("Total rows written/updated: %d", total_rows)

    # Check — should be 18,993 (13 × 1,461)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM Fact_Weather")).scalar()
    log.info("Fact_Weather current count: %d", count)
    log.info("Expected: 13 regions × 1,461 days = 18,993")


if __name__ == "__main__":
    main()
