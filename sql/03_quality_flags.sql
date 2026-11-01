-- =============================================================================
--  flood_dw  —  FILE 03 — QUALITY FLAGS
-- -----------------------------------------------------------------------------
--  Classifies every Fact_Flood row by the significance of the observation, so
--  the dashboard can separate real flood signal from background, and so the
--  clustering can count strong observations outside named events:
--
--    event          — the date falls inside a named event (event_key set)
--    high_signal    — outside events, flooded area >= 5 km²
--    medium_signal  — outside events, 1 km² <= flooded area < 5 km²
--    low_signal     — outside events, flooded area < 1 km²
--
--  Runs after the ETL has loaded Fact_Flood, and before clustering.py.
-- =============================================================================

USE flood_dw;

UPDATE Fact_Flood
SET quality_flag = CASE
        WHEN event_key IS NOT NULL      THEN 'event'
        WHEN flooded_area_km2 >= 5      THEN 'high_signal'
        WHEN flooded_area_km2 >= 1      THEN 'medium_signal'
        ELSE                                 'low_signal'
    END
WHERE flood_key > 0;   -- explicit key condition, for MySQL safe-update mode
