-- OpenAQ Air Quality Data Mart: Production-grade DDL, ETL procedures, tasks, and analytic queries
-- Co-authored with CoCo

-- ============================================================
-- STEP 1: CREATE DATABASE AND SCHEMAS
-- ============================================================
CREATE OR REPLACE DATABASE OPENAQ;

USE DATABASE OPENAQ;

CREATE OR REPLACE SCHEMA RAW
    COMMENT = 'Raw ingestion layer for OpenAQ data';

CREATE OR REPLACE SCHEMA STAGING
    COMMENT = 'Cleaned and validated data';

CREATE OR REPLACE SCHEMA MART
    COMMENT = 'Data mart optimized for analytic queries';

CREATE OR REPLACE SCHEMA DATA_QUALITY
    COMMENT = 'Data quality reporting and rejected records';

CREATE OR REPLACE SCHEMA ETL
    COMMENT = 'Stored procedures and pipeline orchestration';

-- ============================================================
-- STEP 2: CREATE RAW TABLE (TB-SCALE WITH CLUSTERING + MULTI-SOURCE)
-- ============================================================
USE SCHEMA OPENAQ.RAW;

CREATE OR REPLACE TABLE AIR_QUALITY_MEASUREMENTS (
    location_id     INTEGER,
    sensors_id      INTEGER,
    location        VARCHAR(500),
    datetime        TIMESTAMP_TZ,
    lat             FLOAT,
    lon             FLOAT,
    parameter       VARCHAR(50),
    units           VARCHAR(50),
    value           FLOAT,
    source_name     VARCHAR(200) DEFAULT 'openaq_s3',
    source_file     VARCHAR(1000),
    ingested_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('month', datetime), parameter)
COMMENT = 'Raw air quality measurements - clustered for TB-scale query performance';

-- ETL execution log for pipeline observability
CREATE OR REPLACE TABLE OPENAQ.ETL.PIPELINE_LOG (
    run_id          VARCHAR(50),
    step_name       VARCHAR(200),
    status          VARCHAR(20),
    rows_affected   INTEGER,
    error_message   VARCHAR(5000),
    started_at      TIMESTAMP_NTZ,
    completed_at    TIMESTAMP_NTZ
)
COMMENT = 'ETL pipeline execution audit log';

-- ============================================================
-- STEP 3: CREATE FILE FORMATS AND STAGES
-- ============================================================
CREATE OR REPLACE FILE FORMAT OPENAQ.RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NA', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    ENCODING = 'UTF8';

CREATE OR REPLACE FILE FORMAT OPENAQ.RAW.CSV_GZ_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NA', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = 'GZIP'
    ENCODING = 'UTF8';

-- External stage pointing to OpenAQ S3 public bucket (TB-scale historical data)
CREATE OR REPLACE STAGE OPENAQ.RAW.OPENAQ_S3_STAGE
    URL = 's3://openaq-fetches/'
    FILE_FORMAT = OPENAQ.RAW.CSV_GZ_FORMAT;

-- Internal stage for local file uploads
CREATE OR REPLACE STAGE OPENAQ.RAW.INTERNAL_STAGE
    FILE_FORMAT = OPENAQ.RAW.CSV_FORMAT;

-- ============================================================
-- STEP 4: LOAD SAMPLE DATA FROM WORKSPACE CSV
-- ============================================================
COPY FILES INTO @OPENAQ.RAW.INTERNAL_STAGE
FROM 'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live'
FILES=('location-2178-20220503.csv');

COPY INTO OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS (
    location_id, sensors_id, location, datetime, lat, lon, parameter, units, value
)
FROM @OPENAQ.RAW.INTERNAL_STAGE/location-2178-20220503.csv
FILE_FORMAT = (FORMAT_NAME = OPENAQ.RAW.CSV_FORMAT)
ON_ERROR = 'CONTINUE';

-- Verify load
SELECT COUNT(*) AS raw_record_count FROM OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS;

-- ============================================================
-- STEP 5: DATA QUALITY - REJECTED RECORDS TABLE
-- ============================================================
USE SCHEMA OPENAQ.DATA_QUALITY;

CREATE OR REPLACE TABLE REJECTED_RECORDS (
    location_id      INTEGER,
    sensors_id       INTEGER,
    location         VARCHAR(500),
    datetime         TIMESTAMP_TZ,
    lat              FLOAT,
    lon              FLOAT,
    parameter        VARCHAR(50),
    units            VARCHAR(50),
    value            FLOAT,
    rejection_reason VARCHAR(200),
    source_name      VARCHAR(200),
    rejected_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (rejection_reason, parameter)
COMMENT = 'Records excluded from the data mart due to quality issues';

-- ============================================================
-- STEP 6: STAGING - CLEAN DATA TABLE DEFINITION
-- ============================================================
USE SCHEMA OPENAQ.STAGING;

CREATE OR REPLACE TABLE AIR_QUALITY_CLEAN (
    location_id        INTEGER,
    sensors_id         INTEGER,
    location           VARCHAR(500),
    city               VARCHAR(500),
    datetime           TIMESTAMP_TZ,
    measurement_date   DATE,
    measurement_month  DATE,
    measurement_hour   INTEGER,
    lat                FLOAT,
    lon                FLOAT,
    parameter          VARCHAR(50),
    units              VARCHAR(50),
    value              FLOAT,
    interpolated_value FLOAT
)
CLUSTER BY (measurement_date, parameter, city)
COMMENT = 'Cleaned and interpolated air quality data - clustered for TB-scale joins';

-- ============================================================
-- STEP 7: DATA MART - DYNAMIC TABLES (AUTO-REFRESH)
-- ============================================================
USE SCHEMA OPENAQ.MART;

-- 7A: Monthly City Pollution (auto-refreshes from staging)
CREATE OR REPLACE DYNAMIC TABLE MONTHLY_CITY_POLLUTION
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    city,
    location,
    measurement_month,
    parameter,
    units,
    AVG(interpolated_value) AS avg_monthly_value,
    COUNT(*) AS measurement_count,
    MIN(interpolated_value) AS min_value,
    MAX(interpolated_value) AS max_value
FROM OPENAQ.STAGING.AIR_QUALITY_CLEAN
WHERE parameter IN ('co', 'so2')
GROUP BY city, location, measurement_month, parameter, units;

-- 7B: Daily City PM2.5 (auto-refreshes from staging)
CREATE OR REPLACE DYNAMIC TABLE DAILY_CITY_PM25
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    city,
    location,
    measurement_date,
    AVG(interpolated_value) AS avg_daily_pm25,
    COUNT(*) AS measurement_count,
    MIN(interpolated_value) AS min_pm25,
    MAX(interpolated_value) AS max_pm25
FROM OPENAQ.STAGING.AIR_QUALITY_CLEAN
WHERE parameter = 'pm25'
GROUP BY city, location, measurement_date;

-- 7C: Hourly City Pollution (auto-refreshes from staging)
CREATE OR REPLACE DYNAMIC TABLE HOURLY_CITY_POLLUTION
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    city,
    location,
    measurement_date,
    measurement_hour,
    parameter,
    units,
    AVG(interpolated_value) AS avg_hourly_value,
    COUNT(*) AS measurement_count
FROM OPENAQ.STAGING.AIR_QUALITY_CLEAN
WHERE parameter IN ('pm25', 'co', 'so2')
GROUP BY city, location, measurement_date, measurement_hour, parameter, units;

-- 7D: Country Air Quality Index (auto-refreshes from staging)
CREATE OR REPLACE DYNAMIC TABLE COUNTRY_AIR_QUALITY_INDEX
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
WITH hourly_params AS (
    SELECT
        city,
        location,
        measurement_date,
        measurement_hour,
        lat,
        lon,
        MAX(CASE WHEN parameter = 'pm25' THEN interpolated_value END) AS pm25_value,
        MAX(CASE WHEN parameter = 'pm10' THEN interpolated_value END) AS pm10_value,
        MAX(CASE WHEN parameter = 'so2' THEN interpolated_value END) AS so2_value,
        MAX(CASE WHEN parameter = 'co' THEN interpolated_value END) AS co_value
    FROM OPENAQ.STAGING.AIR_QUALITY_CLEAN
    WHERE parameter IN ('pm25', 'pm10', 'so2', 'co')
    GROUP BY city, location, measurement_date, measurement_hour, lat, lon
),
scored AS (
    SELECT
        *,
        COALESCE(pm25_value / 35.0, 0) * 0.4
        + COALESCE(pm10_value / 150.0, 0) * 0.2
        + COALESCE(so2_value / 0.075, 0) * 0.2
        + COALESCE(co_value / 9.0, 0) * 0.2 AS aqi_score
    FROM hourly_params
)
SELECT
    location,
    city,
    lat,
    lon,
    measurement_date,
    measurement_hour,
    pm25_value,
    pm10_value,
    so2_value,
    co_value,
    aqi_score,
    CASE
        WHEN aqi_score >= 1.0 THEN 'High'
        WHEN aqi_score >= 0.5 THEN 'Moderate'
        ELSE 'Low'
    END AS air_quality_level
FROM scored;

-- ============================================================
-- STEP 8: STORED PROCEDURES (PRODUCTION ETL PIPELINE)
-- ============================================================
USE SCHEMA OPENAQ.ETL;

-- 8A: Data Quality Procedure - identifies and logs rejected records
CREATE OR REPLACE PROCEDURE OPENAQ.ETL.SP_RUN_DATA_QUALITY(P_RUN_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET v_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET v_rows INTEGER := 0;

    -- Clear previous quality flags for re-run
    DELETE FROM OPENAQ.DATA_QUALITY.REJECTED_RECORDS
    WHERE rejected_at < DATEADD('day', -30, CURRENT_TIMESTAMP());

    -- Reject NULLs in critical fields
    INSERT INTO OPENAQ.DATA_QUALITY.REJECTED_RECORDS
        (location_id, sensors_id, location, datetime, lat, lon, parameter, units, value, rejection_reason, source_name)
    SELECT location_id, sensors_id, location, datetime, lat, lon, parameter, units, value,
        CASE
            WHEN value IS NULL THEN 'NULL measurement value'
            WHEN datetime IS NULL THEN 'NULL datetime'
            WHEN location IS NULL OR location = '' THEN 'NULL or empty location'
            WHEN parameter IS NULL OR parameter = '' THEN 'NULL or empty parameter'
            WHEN lat IS NULL OR lon IS NULL THEN 'NULL coordinates'
        END,
        source_name
    FROM OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS
    WHERE value IS NULL
       OR datetime IS NULL
       OR location IS NULL OR location = ''
       OR parameter IS NULL OR parameter = ''
       OR lat IS NULL OR lon IS NULL;

    v_rows := SQLROWCOUNT;

    -- Reject negative values
    INSERT INTO OPENAQ.DATA_QUALITY.REJECTED_RECORDS
        (location_id, sensors_id, location, datetime, lat, lon, parameter, units, value, rejection_reason, source_name)
    SELECT location_id, sensors_id, location, datetime, lat, lon, parameter, units, value,
        'Negative measurement value', source_name
    FROM OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS
    WHERE value < 0;

    v_rows := :v_rows + SQLROWCOUNT;

    -- Reject extreme outliers (>5 std dev)
    INSERT INTO OPENAQ.DATA_QUALITY.REJECTED_RECORDS
        (location_id, sensors_id, location, datetime, lat, lon, parameter, units, value, rejection_reason, source_name)
    SELECT r.location_id, r.sensors_id, r.location, r.datetime, r.lat, r.lon, r.parameter, r.units, r.value,
        'Extreme outlier (>5 std dev from mean)', r.source_name
    FROM OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS r
    JOIN (
        SELECT parameter, AVG(value) AS avg_val, STDDEV(value) AS std_val
        FROM OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS
        WHERE value IS NOT NULL AND value >= 0
        GROUP BY parameter
    ) stats ON r.parameter = stats.parameter
    WHERE r.value > (stats.avg_val + 5 * stats.std_val)
      AND stats.std_val > 0;

    v_rows := :v_rows + SQLROWCOUNT;

    -- Log execution
    INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, rows_affected, started_at, completed_at)
    VALUES (:P_RUN_ID, 'DATA_QUALITY', 'SUCCESS', :v_rows, :v_start, CURRENT_TIMESTAMP());

    RETURN 'Data quality complete. Rejected records: ' || :v_rows::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, error_message, started_at, completed_at)
        VALUES (:P_RUN_ID, 'DATA_QUALITY', 'FAILED', SQLERRM, :v_start, CURRENT_TIMESTAMP());
        RETURN 'FAILED: ' || SQLERRM;
END;

-- 8B: Staging Refresh Procedure - rebuilds clean data with interpolation
CREATE OR REPLACE PROCEDURE OPENAQ.ETL.SP_REFRESH_STAGING(P_RUN_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET v_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET v_rows INTEGER := 0;

    CREATE OR REPLACE TABLE OPENAQ.STAGING.AIR_QUALITY_CLEAN AS
    SELECT
        r.location_id,
        r.sensors_id,
        r.location,
        SPLIT_PART(r.location, '-', 1) AS city,
        r.datetime,
        DATE_TRUNC('day', r.datetime)::DATE AS measurement_date,
        DATE_TRUNC('month', r.datetime)::DATE AS measurement_month,
        EXTRACT(HOUR FROM r.datetime)::INTEGER AS measurement_hour,
        r.lat,
        r.lon,
        r.parameter,
        r.units,
        r.value,
        COALESCE(
            r.value,
            (LAG(r.value) OVER (PARTITION BY r.location_id, r.parameter ORDER BY r.datetime)
             + LEAD(r.value) OVER (PARTITION BY r.location_id, r.parameter ORDER BY r.datetime)) / 2
        ) AS interpolated_value
    FROM OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS r
    WHERE r.value IS NOT NULL
      AND r.value >= 0
      AND r.datetime IS NOT NULL
      AND r.location IS NOT NULL AND r.location != ''
      AND r.parameter IS NOT NULL AND r.parameter != ''
      AND r.lat IS NOT NULL AND r.lon IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM OPENAQ.DATA_QUALITY.REJECTED_RECORDS rej
          WHERE rej.location_id = r.location_id
            AND rej.sensors_id = r.sensors_id
            AND rej.datetime = r.datetime
            AND rej.parameter = r.parameter
            AND rej.rejection_reason = 'Extreme outlier (>5 std dev from mean)'
      );

    SELECT COUNT(*) INTO :v_rows FROM OPENAQ.STAGING.AIR_QUALITY_CLEAN;

    -- Re-apply clustering after rebuild
    ALTER TABLE OPENAQ.STAGING.AIR_QUALITY_CLEAN
        CLUSTER BY (measurement_date, parameter, city);

    -- Log execution
    INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, rows_affected, started_at, completed_at)
    VALUES (:P_RUN_ID, 'STAGING_REFRESH', 'SUCCESS', :v_rows, :v_start, CURRENT_TIMESTAMP());

    RETURN 'Staging refresh complete. Clean records: ' || :v_rows::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, error_message, started_at, completed_at)
        VALUES (:P_RUN_ID, 'STAGING_REFRESH', 'FAILED', SQLERRM, :v_start, CURRENT_TIMESTAMP());
        RETURN 'FAILED: ' || SQLERRM;
END;

-- 8C: Incremental Load Procedure - loads new data from S3 by year/month pattern
CREATE OR REPLACE PROCEDURE OPENAQ.ETL.SP_LOAD_FROM_S3(P_RUN_ID VARCHAR, P_YEAR VARCHAR, P_MONTH VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET v_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET v_pattern VARCHAR := '.*locationId=.*/' || P_YEAR || '/' || P_MONTH || '/.*\\.csv\\.gz';

    COPY INTO OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS (
        location_id, sensors_id, location, datetime, lat, lon, parameter, units, value
    )
    FROM @OPENAQ.RAW.OPENAQ_S3_STAGE/records/csv.gz/
    PATTERN = :v_pattern
    FILE_FORMAT = (FORMAT_NAME = OPENAQ.RAW.CSV_GZ_FORMAT)
    ON_ERROR = 'CONTINUE';

    LET v_rows INTEGER := SQLROWCOUNT;

    -- Update source tracking
    UPDATE OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS
    SET source_name = 'openaq_s3_' || :P_YEAR || '_' || :P_MONTH
    WHERE source_name = 'openaq_s3'
      AND source_file IS NULL
      AND ingested_at >= :v_start;

    -- Log execution
    INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, rows_affected, started_at, completed_at)
    VALUES (:P_RUN_ID, 'S3_LOAD_' || :P_YEAR || '_' || :P_MONTH, 'SUCCESS', :v_rows, :v_start, CURRENT_TIMESTAMP());

    RETURN 'S3 load complete for ' || :P_YEAR || '-' || :P_MONTH || '. Rows loaded: ' || :v_rows::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, error_message, started_at, completed_at)
        VALUES (:P_RUN_ID, 'S3_LOAD_' || :P_YEAR || '_' || :P_MONTH, 'FAILED', SQLERRM, :v_start, CURRENT_TIMESTAMP());
        RETURN 'FAILED: ' || SQLERRM;
END;

-- 8D: Master Orchestrator Procedure - runs the full pipeline
CREATE OR REPLACE PROCEDURE OPENAQ.ETL.SP_RUN_FULL_PIPELINE()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET v_run_id VARCHAR := 'RUN_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    LET v_result VARCHAR;

    -- Step 1: Data Quality
    CALL OPENAQ.ETL.SP_RUN_DATA_QUALITY(:v_run_id) INTO :v_result;
    IF (STARTSWITH(:v_result, 'FAILED')) THEN
        RETURN 'Pipeline aborted at DATA_QUALITY: ' || :v_result;
    END IF;

    -- Step 2: Refresh Staging
    CALL OPENAQ.ETL.SP_REFRESH_STAGING(:v_run_id) INTO :v_result;
    IF (STARTSWITH(:v_result, 'FAILED')) THEN
        RETURN 'Pipeline aborted at STAGING_REFRESH: ' || :v_result;
    END IF;

    -- Dynamic tables auto-refresh from staging (no manual mart rebuild needed)

    RETURN 'Pipeline complete. Run ID: ' || :v_run_id;
END;

-- ============================================================
-- STEP 9: TASK-BASED SCHEDULING (AUTOMATED REFRESH)
-- ============================================================

-- Root task: runs the full pipeline every 6 hours
CREATE OR REPLACE TASK OPENAQ.ETL.TASK_FULL_PIPELINE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */6 * * * America/Denver'
    COMMENT = 'Runs full ETL pipeline every 6 hours'
AS
    CALL OPENAQ.ETL.SP_RUN_FULL_PIPELINE();

-- Monitoring task: alerts on pipeline failures (runs every hour)
CREATE OR REPLACE TASK OPENAQ.ETL.TASK_MONITOR_PIPELINE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 * * * * America/Denver'
    COMMENT = 'Monitors pipeline health and logs failures'
AS
    INSERT INTO OPENAQ.ETL.PIPELINE_LOG (run_id, step_name, status, rows_affected, started_at, completed_at)
    SELECT
        'MONITOR_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'),
        'HEALTH_CHECK',
        CASE WHEN COUNT_IF(status = 'FAILED') > 0 THEN 'ALERT' ELSE 'HEALTHY' END,
        COUNT_IF(status = 'FAILED'),
        MIN(started_at),
        CURRENT_TIMESTAMP()
    FROM OPENAQ.ETL.PIPELINE_LOG
    WHERE started_at >= DATEADD('hour', -6, CURRENT_TIMESTAMP());

-- Enable tasks (uncomment to activate in production)
-- ALTER TASK OPENAQ.ETL.TASK_FULL_PIPELINE RESUME;
-- ALTER TASK OPENAQ.ETL.TASK_MONITOR_PIPELINE RESUME;

-- ============================================================
-- STEP 10: RUN THE PIPELINE (INITIAL EXECUTION)
-- ============================================================
CALL OPENAQ.ETL.SP_RUN_FULL_PIPELINE();

-- Verify pipeline execution log
SELECT * FROM OPENAQ.ETL.PIPELINE_LOG ORDER BY started_at DESC;

-- Verify dynamic table refresh status
SELECT name, scheduling_state, last_completed_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name IN ('MONTHLY_CITY_POLLUTION', 'DAILY_CITY_PM25', 'HOURLY_CITY_POLLUTION', 'COUNTRY_AIR_QUALITY_INDEX')
ORDER BY last_completed_time DESC;

-- ============================================================
-- STEP 11: ANALYTIC QUERIES (Business Requirements)
-- ============================================================

-- QUERY 1: For any given month, find all cities with average monthly
--           CO and SO2 levels in the 90th percentile globally
WITH monthly_stats AS (
    SELECT
        city,
        parameter,
        avg_monthly_value,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY avg_monthly_value)
            OVER (PARTITION BY parameter, measurement_month) AS p90_threshold
    FROM OPENAQ.MART.MONTHLY_CITY_POLLUTION
    WHERE measurement_month = '2022-05-01'::DATE
)
SELECT DISTINCT
    city,
    parameter,
    avg_monthly_value,
    p90_threshold
FROM monthly_stats
WHERE avg_monthly_value >= p90_threshold
ORDER BY parameter, avg_monthly_value DESC;

-- QUERY 2: For any given day, find the top 5 cities globally with
--           the highest daily average PM2.5 levels
SELECT
    city,
    location,
    measurement_date,
    avg_daily_pm25,
    measurement_count
FROM OPENAQ.MART.DAILY_CITY_PM25
WHERE measurement_date = '2022-05-03'::DATE
ORDER BY avg_daily_pm25 DESC
LIMIT 5;

-- QUERY 3: For any given hour, find top 10 cities with highest
--           daily average PM2.5, plus mean/median/mode of CO & SO2
WITH top10_pm25_cities AS (
    SELECT city, location, measurement_date, measurement_hour, avg_hourly_value AS pm25_avg
    FROM OPENAQ.MART.HOURLY_CITY_POLLUTION
    WHERE parameter = 'pm25'
      AND measurement_date = '2022-05-03'::DATE
      AND measurement_hour = 8
    ORDER BY pm25_avg DESC
    LIMIT 10
),
co_so2_daily AS (
    SELECT
        c.city,
        c.location,
        s.parameter,
        s.interpolated_value
    FROM top10_pm25_cities c
    JOIN OPENAQ.STAGING.AIR_QUALITY_CLEAN s
        ON c.city = s.city
        AND c.measurement_date = s.measurement_date
    WHERE s.parameter IN ('co', 'so2')
)
SELECT
    t.city,
    t.location,
    t.pm25_avg,
    d.parameter,
    AVG(d.interpolated_value) AS mean_value,
    MEDIAN(d.interpolated_value) AS median_value,
    MODE(d.interpolated_value) AS mode_value
FROM top10_pm25_cities t
LEFT JOIN co_so2_daily d
    ON t.city = d.city AND t.location = d.location
GROUP BY t.city, t.location, t.pm25_avg, d.parameter
ORDER BY t.pm25_avg DESC, d.parameter;

-- QUERY 4: For any given hour, report air quality index per country
--           with 3 levels: High, Moderate, Low
SELECT
    location,
    city,
    lat,
    lon,
    measurement_date,
    measurement_hour,
    pm25_value,
    pm10_value,
    so2_value,
    co_value,
    ROUND(aqi_score, 3) AS aqi_score,
    air_quality_level
FROM OPENAQ.MART.COUNTRY_AIR_QUALITY_INDEX
WHERE measurement_date = '2022-05-03'::DATE
  AND measurement_hour = 8
ORDER BY aqi_score DESC;

-- ============================================================
-- STEP 12: DATA QUALITY REPORTING
-- ============================================================
SELECT
    rejection_reason,
    parameter,
    COUNT(*) AS rejected_count,
    MIN(value) AS min_rejected_value,
    MAX(value) AS max_rejected_value,
    ROUND(AVG(value), 3) AS avg_rejected_value
FROM OPENAQ.DATA_QUALITY.REJECTED_RECORDS
GROUP BY rejection_reason, parameter
ORDER BY rejected_count DESC;

-- ============================================================
-- STEP 13: TB-SCALE HISTORICAL LOAD (2017 full year example)
-- ============================================================
-- Load all 12 months of 2017 data from OpenAQ S3
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '01');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '02');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '03');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '04');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '05');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '06');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '07');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '08');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '09');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '10');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '11');
-- CALL OPENAQ.ETL.SP_LOAD_FROM_S3('HISTORICAL_2017', '2017', '12');
-- After loading, run the full pipeline:
-- CALL OPENAQ.ETL.SP_RUN_FULL_PIPELINE();
