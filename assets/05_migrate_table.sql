-- Get Started with Snowflake-Managed Iceberg Tables
-- Script 05: Migrate a table to ICEBERG
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FLEET_DB;
USE SCHEMA RAW;
USE WAREHOUSE FLEET_WH;

SELECT * FROM SENSOR_READINGS_TEMP;

-- ============================================
-- Migrate to Iceberg with a simple CTAS
-- ============================================

CREATE OR REPLACE ICEBERG TABLE SENSOR_READINGS 
    EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
    CATALOG = 'SNOWFLAKE'
AS
SELECT
    *
FROM SENSOR_READINGS_TEMP;

-- ============================================
-- Check table data
-- ============================================

-- Row counts, plus rows present in one table but not the other
SELECT
    (SELECT COUNT(*) FROM FLEET_DB.RAW.SENSOR_READINGS_TEMP) AS temp_rows,
    (SELECT COUNT(*) FROM FLEET_DB.RAW.SENSOR_READINGS)      AS iceberg_rows,
    (SELECT COUNT(*) FROM (SELECT * FROM FLEET_DB.RAW.SENSOR_READINGS_TEMP
                           MINUS
                           SELECT * FROM FLEET_DB.RAW.SENSOR_READINGS))      AS only_in_temp,
    (SELECT COUNT(*) FROM (SELECT * FROM FLEET_DB.RAW.SENSOR_READINGS
                           MINUS
                           SELECT * FROM FLEET_DB.RAW.SENSOR_READINGS_TEMP)) AS only_in_iceberg;

-- ============================================
-- Clean up the standard table
-- ============================================
DROP TABLE SENSOR_READINGS_TEMP;


-- ============================================
-- Time Travel
-- ============================================
DROP TABLE SENSOR_READINGS;
UNDROP TABLE SENSOR_READINGS;
SELECT COUNT(*) FROM FLEET_DB.RAW.SENSOR_READINGS;
