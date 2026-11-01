-- =============================================================================
--  flood_dw  —  Data warehouse of the Flood Monitoring project
-- -----------------------------------------------------------------------------
--  FILE 01 — SCHEMA
--
--  Creates the database and all (empty) tables. Runs first.
--  Structure: a star schema extended into a fact constellation with two fact
--  tables (Fact_Flood, Fact_Weather) over shared dimensions.
--
--        Dim_Date ─────┐         ┌───── Dim_Location
--                      │         │
--                  Fact_Flood ───┤
--                      │         │
--        Dim_Event ────┘         └───── Fact_Weather
--
--  Setup order: see README.md.
-- =============================================================================


-- ----------------------------------------------------------------------------
-- Create the database
-- ----------------------------------------------------------------------------
DROP DATABASE IF EXISTS flood_dw;

CREATE DATABASE flood_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE flood_dw;


-- ============================================================================
-- DIMENSIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Dim_Date
-- ----------------------------------------------------------------------------
-- The time dimension. Pre-populated with every day of the project range
-- (2021-2024). Does not change at runtime.
-- date_key is YYYYMMDD (e.g. 20230906) — a surrogate key that is also human
-- readable, which helps when debugging.
-- ----------------------------------------------------------------------------
CREATE TABLE Dim_Date (
    date_key      INT          NOT NULL,                     -- format YYYYMMDD
    full_date     DATE         NOT NULL,
    day           TINYINT      NOT NULL,
    month         TINYINT      NOT NULL,
    month_name    VARCHAR(10)  NOT NULL,
    year          SMALLINT     NOT NULL,
    quarter       TINYINT      NOT NULL,
    season        VARCHAR(10)  NOT NULL,                     -- Winter/Spring/Summer/Autumn
    day_of_year   SMALLINT     NOT NULL,
    day_of_week   TINYINT      NOT NULL,                     -- 1=Monday ... 7=Sunday
    is_weekend    TINYINT(1)   NOT NULL DEFAULT 0,
    PRIMARY KEY (date_key),
    UNIQUE KEY full_date (full_date),
    KEY idx_full_date (full_date),
    KEY idx_year_month (year, month)
) ENGINE=InnoDB;


-- ----------------------------------------------------------------------------
-- Dim_Location
-- ----------------------------------------------------------------------------
-- Geographic dimension. Each row is one administrative region of Greece
-- (NUTS-2, e.g. EL61 = Thessaly).
-- The centroids are used for mapping in Power BI and for matching the
-- Open-Meteo weather data.
--
-- risk_profile is filled by clustering.py (K-Means) — the result of grouping
-- the regions into High/Medium/Low. It starts as NULL because clustering
-- runs after the fact tables are loaded.
-- ----------------------------------------------------------------------------
CREATE TABLE Dim_Location (
    location_key  INT             NOT NULL AUTO_INCREMENT,
    country       VARCHAR(50)     NOT NULL DEFAULT 'Greece',
    region_name   VARCHAR(100)    NOT NULL,
    region_code   VARCHAR(10)     NOT NULL,                  -- NUTS-2 code
    centroid_lat  DECIMAL(9,6)    NOT NULL,
    centroid_lon  DECIMAL(9,6)    NOT NULL,
    area_km2      DECIMAL(10,2)   DEFAULT NULL,
    risk_profile  VARCHAR(20)     DEFAULT NULL,              -- K-Means: High/Medium/Low
    geometry_wkt  LONGTEXT        DEFAULT NULL,              -- WKT polygon in WGS84
    PRIMARY KEY (location_key),
    UNIQUE KEY region_code (region_code),
    KEY idx_region_code (region_code),
    KEY idx_region_name (region_name)
) ENGINE=InnoDB;


-- ----------------------------------------------------------------------------
-- Dim_Event
-- ----------------------------------------------------------------------------
-- Named flood events within scope. Allows filtering and grouping by event in
-- the dashboard and the clustering.
-- Dates are documented from EMY, NOA/Meteo, CEMS and the scientific
-- literature (see the description field in 02_dimensions.sql).
-- ----------------------------------------------------------------------------
CREATE TABLE Dim_Event (
    event_key       INT          NOT NULL AUTO_INCREMENT,
    event_id        VARCHAR(50)  NOT NULL,                   -- e.g. STORM_DANIEL_2023
    event_name      VARCHAR(100) NOT NULL,
    start_date      DATE         NOT NULL,
    end_date        DATE         DEFAULT NULL,
    severity_class  VARCHAR(20)  DEFAULT NULL,               -- moderate/major/extreme
    description     TEXT         DEFAULT NULL,
    PRIMARY KEY (event_key),
    UNIQUE KEY event_id (event_id),
    KEY idx_event_dates (start_date, end_date),
    KEY idx_severity (severity_class)
) ENGINE=InnoDB;


-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Fact_Flood
-- ----------------------------------------------------------------------------
-- The main fact table.
--
-- Grain: one current row per (date × region). With SCD Type 2 there can also
--        be historical (is_current=0) versions of the same pair, when EODC
--        revises a GFM product.
--
-- Measures and additivity (important for how they aggregate in BI):
--   • flooded_area_km2    — additive in space, semi-additive in time
--                           (the same area on two days is not summed; these
--                            are snapshots of the same extent)
--   • flood_likelihood    — non-additive (mean probability, use AVG)
--   • flood_ratio         — non-additive (flooded/floodable ratio)
--   • rainfall_mm         — additive
--   • river_discharge_m3s — semi-additive
--   • flooded_pixels / floodable_pixels — raw counts, additive
--
-- SCD Type 2: valid_from, valid_to, is_current. When a row is revised, the
--   old one is closed (valid_to = now, is_current = 0) and a new current row
--   is added.
--
-- Lineage: data_source, source_collection, source_item_id — traceability back
--   to the original STAC item.
--
-- Indexes: (location_key, date_key, is_current) is indexed on purpose,
--   because this is exactly the lookup the ETL performs for every SCD2
--   comparison — without it, every load would do a full scan.
-- ----------------------------------------------------------------------------
CREATE TABLE Fact_Flood (
    flood_key            BIGINT          NOT NULL AUTO_INCREMENT,
    date_key             INT             NOT NULL,
    location_key         INT             NOT NULL,
    event_key            INT             DEFAULT NULL,
    flooded_area_km2     DECIMAL(12,4)   DEFAULT NULL,       -- additive (space), semi-additive (time)
    flood_likelihood     DECIMAL(5,4)    DEFAULT NULL,       -- non-additive (AVG, not SUM)
    flood_ratio          DECIMAL(7,6)    DEFAULT NULL,       -- flooded_pixels / floodable_pixels
    rainfall_mm          DECIMAL(8,2)    DEFAULT NULL,       -- additive (Open-Meteo)
    river_discharge_m3s  DECIMAL(10,2)   DEFAULT NULL,       -- semi-additive (Open-Meteo)
    quality_flag         VARCHAR(20)     DEFAULT NULL,       -- event/high_signal/medium_signal/low_signal
    event_count          INT             NOT NULL DEFAULT 1,
    flooded_pixels       BIGINT          DEFAULT NULL,       -- raw pixel count from the GFM raster
    floodable_pixels     BIGINT          DEFAULT NULL,       -- raw pixel count from the GFM raster
    valid_from           DATETIME        NOT NULL,           -- SCD Type 2
    valid_to             DATETIME        DEFAULT NULL,       -- SCD Type 2
    is_current           TINYINT(1)      NOT NULL DEFAULT 1, -- SCD Type 2
    data_source          VARCHAR(50)     NOT NULL DEFAULT 'GFM',
    source_collection    VARCHAR(50)     DEFAULT 'GFM_ARCHIVE',
    source_item_id       VARCHAR(150)    DEFAULT NULL,       -- STAC item id for traceability
    last_updated         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (flood_key),
    KEY idx_date (date_key),
    KEY idx_location (location_key),
    KEY idx_event (event_key),
    KEY idx_current (is_current),
    KEY idx_source_item (source_item_id),
    KEY idx_grain_current (location_key, date_key, is_current),
    KEY idx_quality_flag (quality_flag),

    CONSTRAINT fk_fact_date     FOREIGN KEY (date_key)     REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_location FOREIGN KEY (location_key) REFERENCES Dim_Location(location_key),
    CONSTRAINT fk_fact_event    FOREIGN KEY (event_key)    REFERENCES Dim_Event(event_key)
) ENGINE=InnoDB;


-- ----------------------------------------------------------------------------
-- Fact_Weather
-- ----------------------------------------------------------------------------
-- Second fact table — the one that turns the schema into a constellation
-- instead of a plain star. Holds daily weather per region.
--
-- Grain: one row per (date × region), EVERY day.
--   A different grain from Fact_Flood, which has rows only on days with a
--   satellite overpass. That is why it is a separate table and not columns
--   in Fact_Flood: otherwise weather would exist only on the few overpass
--   days.
--
-- No SCD2 — historical weather data is not revised the way flood products
--   are. An idempotent upsert is used instead (see weather_etl.py).
--
-- Source: Open-Meteo Historical Weather Archive (centroid-based sampling).
-- ----------------------------------------------------------------------------
CREATE TABLE Fact_Weather (
    weather_key       BIGINT         NOT NULL AUTO_INCREMENT,
    date_key          INT            NOT NULL,
    location_key      INT            NOT NULL,
    precipitation_mm  DECIMAL(8,2)   DEFAULT NULL,
    temp_max_c        DECIMAL(5,2)   DEFAULT NULL,
    temp_min_c        DECIMAL(5,2)   DEFAULT NULL,
    data_source       VARCHAR(50)    NOT NULL DEFAULT 'Open-Meteo',
    last_updated      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (weather_key),
    -- The UNIQUE key is the core of the idempotent upsert: ON DUPLICATE KEY
    -- on this pair guarantees the same (day, region) is never written twice
    -- if weather_etl.py is rerun.
    UNIQUE KEY uq_date_location (date_key, location_key),
    KEY idx_date (date_key),
    KEY idx_location (location_key),

    CONSTRAINT fk_weather_date     FOREIGN KEY (date_key)     REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_weather_location FOREIGN KEY (location_key) REFERENCES Dim_Location(location_key)
) ENGINE=InnoDB;


-- ============================================================================
-- ETL MONITORING TABLE (operational metadata)
-- ============================================================================
-- Records every run of the ETL pipeline: timestamps, watermark before/after,
-- row counts per sync category (NEW / REVISED / UNCHANGED), errors.
-- Not part of the analytical schema — an operational/audit table that lives
-- in the same database for convenience.
-- ============================================================================
CREATE TABLE etl_run_log (
    run_id            INT          NOT NULL AUTO_INCREMENT,
    run_started       DATETIME     NOT NULL,
    run_finished      DATETIME     DEFAULT NULL,
    status            VARCHAR(20)  NOT NULL DEFAULT 'RUNNING', -- RUNNING/SUCCESS/FAILED
    watermark_before  DATE         DEFAULT NULL,
    watermark_after   DATE         DEFAULT NULL,
    items_returned    INT          DEFAULT 0,                  -- STAC items returned by the API
    rows_inserted     INT          DEFAULT 0,                  -- NEW rows
    rows_superseded   INT          DEFAULT 0,                  -- REVISED rows (SCD2)
    rows_unchanged    INT          DEFAULT 0,                  -- UNCHANGED (idempotent skip)
    error_message     TEXT         DEFAULT NULL,
    PRIMARY KEY (run_id),
    KEY idx_run_started (run_started)
) ENGINE=InnoDB;


-- ============================================================================
-- End of 01_schema.sql
-- Next: 02_dimensions.sql (populate Dim_Date, Dim_Location, Dim_Event)
-- ============================================================================
