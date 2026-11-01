-- =============================================================================
--  flood_dw  —  FILE 02 — DIMENSIONS
-- -----------------------------------------------------------------------------
--  Populates the static dimensions:
--    • Dim_Location — 13 NUTS-2 regions (without geometry_wkt; the region
--                     polygons are loaded separately, see README.md)
--    • Dim_Event    — 7 named flood events
--    • Dim_Date     — auto-populated by a stored procedure (1,461 days)
--
--  Runs AFTER 01_schema.sql and BEFORE the fact tables are loaded (the fact
--  tables have foreign keys to these dimensions, so the referenced values
--  must exist first).
-- =============================================================================

USE flood_dw;


-- ----------------------------------------------------------------------------
-- Dim_Location  —  the 13 regions of Greece (NUTS-2)
-- ----------------------------------------------------------------------------
-- Boundary source: Hellenic Statistical Authority (ELSTAT), reprojected to
-- WGS84. The centroids were computed from the polygons. geometry_wkt (the
-- full polygon of each region) is loaded separately because it is very
-- large text.
-- ----------------------------------------------------------------------------
INSERT INTO Dim_Location
    (location_key, country, region_name, region_code, centroid_lat, centroid_lon, area_km2)
VALUES
    ( 1, 'Greece', 'Eastern Macedonia and Thrace', 'EL51', 41.162621, 25.139361, 14177.26),
    ( 2, 'Greece', 'Central Macedonia',            'EL52', 40.734126, 22.979032, 19168.45),
    ( 3, 'Greece', 'Western Macedonia',            'EL53', 40.361598, 21.484260,  9464.64),
    ( 4, 'Greece', 'Epirus',                       'EL54', 39.600554, 20.783919,  9163.08),
    ( 5, 'Greece', 'Thessaly',                     'EL61', 39.532904, 22.216711, 14048.78),
    ( 6, 'Greece', 'North Aegean',                 'EL41', 38.792607, 26.110179,  3847.68),
    ( 7, 'Greece', 'South Aegean',                 'EL42', 36.742249, 26.315321,  5313.80),
    ( 8, 'Greece', 'Central Greece',               'EL64', 38.656261, 22.830117, 15553.03),
    ( 9, 'Greece', 'Western Greece',               'EL63', 38.283516, 21.573441, 11316.91),
    (10, 'Greece', 'Peloponnese',                  'EL65', 37.343173, 22.458301, 15505.53),
    (11, 'Greece', 'Ionian Islands',               'EL62', 38.621548, 20.431723,  2306.84),
    (12, 'Greece', 'Crete',                        'EL43', 35.229252, 24.845832,  8341.88),
    (13, 'Greece', 'Attica',                       'EL30', 37.840158, 23.600814,  3811.63);


-- ----------------------------------------------------------------------------
-- Dim_Event  —  7 named flood events, 2021-2024
-- ----------------------------------------------------------------------------
-- Dates and details compiled from EMY, the National Observatory of Athens /
-- Meteo, Copernicus EMS (CEMS) and the scientific literature on Storm Daniel.
-- ----------------------------------------------------------------------------
INSERT INTO Dim_Event
    (event_key, event_id, event_name, start_date, end_date, severity_class, description)
VALUES
    (1, 'STORM_DANIEL_2023', 'Storm Daniel', '2023-09-04', '2023-09-25', 'extreme',
     'Catastrophic floods in Thessaly (4-8 September 2023). Storm Daniel produced unprecedented precipitation, with over 900 mm recorded in parts of the Pelion area within a few days. The resulting floods across the Thessalian plain caused fatalities and extensive agricultural and infrastructure damage, ranking among the most severe rainfall events in modern Greek records. Sources: Copernicus EMS (CEMS) EMSR692; Falaras et al. 2025 (Remote Sensing 17:1750); Dimitriou et al. 2025 (Water, post-event analysis of the Thessaly flood).'),

    (2, 'STORM_ELIAS_2023', 'Storm Elias', '2023-09-26', '2023-09-29', 'major',
     'Storm Elias (26-29 September 2023). Re-emergence of flooding in Thessaly only weeks after Storm Daniel, while ground was still saturated. Affected the same areas of Pineios River basin and Karla Lake plain. Source: Hellenic National Meteorological Service (EMY).'),

    (3, 'FLOODS_EVROS_2021', 'Evros Floods', '2021-01-15', '2021-02-28', 'major',
     'Severe flooding in NE Greece from the Evros River overflow (January-February 2021), extended to include the Storm Medea cold wave of February 2021 with associated floods in the Alexandroupolis area. Sources: National Observatory of Athens / Meteo; Hellenic National Meteorological Service (EMY).'),

    (4, 'STORM_BORA_2024', 'Storm Bora', '2024-01-15', '2024-02-29', 'moderate',
     'Storm Bora and associated winter storms (15 January - 29 February 2024). Initial Bora event (29 January - 3 February) followed by consecutive winter storms affecting Attica, Eastern Macedonia-Thrace, and other regions. Source: Hellenic National Meteorological Service (EMY) and Greek Ministry of Climate Crisis and Civil Protection.'),

    (5, 'STORM_LEANDROS_2021', 'Storm Leandros', '2021-01-14', '2021-01-18', 'moderate',
     'Storm Leandros (14-18 January 2021). Heavy rainfall, snowfall, and localized floods across much of Greece. Source: Hellenic National Meteorological Service (EMY).'),

    (6, 'NOV_RAINS_2023', 'November 2023 Floods', '2023-11-01', '2023-11-30', 'moderate',
     'Continued heavy precipitation across northern Greece following Storm Daniel. Hellenic National Meteorological Service (EMY) issued orange-level weather alerts on 3-5 November 2023, affecting Central, East and West Macedonia, Epirus, Ionian Islands, and Thessaly.'),

    (8, 'STORM_ERMINIO_2024', 'Storm Erminio', '2024-04-01', '2024-04-10', 'moderate',
     'Storm Erminio (early April 2024). Orange and red-level warnings issued by EMY for Ionian Sea, Western mainland, Peloponnese, Thessaly, Attica, Sporades, Cyclades, and Dodecanese. Heavy precipitation with high flood risk.');


-- ----------------------------------------------------------------------------
-- Dim_Date  —  auto-populate (2021-01-01 → 2024-12-31, 1,461 days)
-- ----------------------------------------------------------------------------
-- A stored procedure with a WHILE loop keeps the script self-contained — no
-- external CSV of dates is needed. It runs once and fills the whole calendar
-- of the project.
-- ----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS populate_dim_date;

DELIMITER //

CREATE PROCEDURE populate_dim_date(IN p_start DATE, IN p_end DATE)
BEGIN
    DECLARE d DATE;
    DECLARE v_month TINYINT;
    DECLARE v_season VARCHAR(10);

    SET d = p_start;

    WHILE d <= p_end DO
        SET v_month = MONTH(d);
        SET v_season = CASE
            WHEN v_month IN (12, 1, 2) THEN 'Winter'
            WHEN v_month IN (3, 4, 5)  THEN 'Spring'
            WHEN v_month IN (6, 7, 8)  THEN 'Summer'
            ELSE                            'Autumn'
        END;

        INSERT INTO Dim_Date (
            date_key, full_date, day, month, month_name, year,
            quarter, season, day_of_year, day_of_week, is_weekend
        ) VALUES (
            CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED),
            d,
            DAY(d),
            v_month,
            DATE_FORMAT(d, '%M'),
            YEAR(d),
            QUARTER(d),
            v_season,
            DAYOFYEAR(d),
            WEEKDAY(d) + 1,                                  -- 1=Monday, 7=Sunday
            CASE WHEN WEEKDAY(d) >= 5 THEN 1 ELSE 0 END
        );

        SET d = DATE_ADD(d, INTERVAL 1 DAY);
    END WHILE;
END //

DELIMITER ;

-- Run for the project range
CALL populate_dim_date('2021-01-01', '2024-12-31');


-- ============================================================================
-- End of 02_dimensions.sql
-- Next: load the region polygons into Dim_Location.geometry_wkt, then run
-- the ETL scripts (see README.md).
-- ============================================================================
