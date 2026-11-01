-- =============================================================================
--  flood_dw  —  FILE 04 — SCHEDULING / HOUSEKEEPING
-- -----------------------------------------------------------------------------
--  Sets up a MySQL Event that automatically cleans up etl_run_log.
--
--  The project uses two schedulers:
--    • The main data sync (the ETL) is scheduled with Windows Task Scheduler,
--      because the ETL is a Python pipeline (flood_etl.py) and cannot run
--      inside MySQL.
--    • The MySQL Event below does ONLY housekeeping inside the database
--      (purging old log entries). That is internal database work, so it is
--      implemented as a MySQL Event.
--
--  Runs last, once etl_run_log exists.
-- =============================================================================

USE flood_dw;


-- ----------------------------------------------------------------------------
-- Enable the MySQL event scheduler
-- ----------------------------------------------------------------------------
-- Without this, events are defined but never run. It is a global server
-- setting.
-- ----------------------------------------------------------------------------
SET GLOBAL event_scheduler = ON;


-- ----------------------------------------------------------------------------
-- Event: purge_old_etl_logs
-- ----------------------------------------------------------------------------
-- Deletes etl_run_log entries older than 90 days, so the audit table does not
-- grow without bound. Runs every day at 02:00.
-- ----------------------------------------------------------------------------
DROP EVENT IF EXISTS purge_old_etl_logs;

CREATE EVENT purge_old_etl_logs
ON SCHEDULE EVERY 1 DAY
    STARTS TIMESTAMP(CURRENT_DATE, '02:00:00')
DO
    DELETE FROM etl_run_log
    WHERE run_started < NOW() - INTERVAL 90 DAY;


-- ============================================================================
-- End of 04_scheduling.sql — the database setup is complete.
-- ============================================================================
