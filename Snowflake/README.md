# OpenAQ Air Quality Data Mart

A fully automated ELT pipeline on Snowflake. AWS Glue extracts raw data from S3 and loads it as-is into Snowflake. All transformations (cleaning, interpolation, aggregation) happen inside Snowflake.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     OpenAQ S3 Bucket                         │
│         s3://openaq-fetches/records/csv.gz/                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  AWS GLUE (Extract + Load ONLY - no transformation)          │
│  • Reads raw files from S3                                   │
│  • Creates/manages table schema dynamically                  │
│  • Writes to Snowflake via Spark/JDBC connector              │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  RAW SCHEMA                                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ AIR_QUALITY_MEASUREMENTS (DDL owned by Glue)        │    │
│  │ Schema is dynamic - columns may change per Glue run │    │
│  └─────────────────────────────────────────────────────┘    │
│                          │                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ STREAM_RAW_NEW_DATA (detects new rows from Glue)    │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │ (auto-trigger)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  ETL SCHEMA (Automated Tasks + Stored Procedures)            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ TASK_PROCESS_NEW_DATA (every 5 min when stream has  │    │
│  │   data) → calls SP_RUN_FULL_PIPELINE                │    │
│  │ TASK_FULL_PIPELINE (cron every 6 hrs - fallback)    │    │
│  │ TASK_MONITOR_PIPELINE (cron every 1 hr - health)    │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼                         ▼
┌──────────────────────┐   ┌──────────────────────────────────┐
│ DATA_QUALITY SCHEMA  │   │ STAGING SCHEMA                    │
│ ┌──────────────────┐ │   │ ┌──────────────────────────────┐ │
│ │ REJECTED_RECORDS │ │   │ │ AIR_QUALITY_CLEAN            │ │
│ │ (nulls, negatives│ │   │ │ Clustered: (date, param,city)│ │
│ │  outliers)       │ │   │ │ + Linear Interpolation       │ │
│ └──────────────────┘ │   │ └──────────────────────────────┘ │
└──────────────────────┘   └──────────────────┬───────────────┘
                                              │
                                              ▼ (auto-refresh)
┌─────────────────────────────────────────────────────────────┐
│  MART SCHEMA (Dynamic Tables - auto-refresh 1hr target lag)  │
│  ┌──────────────────────┐  ┌──────────────────────┐         │
│  │ MONTHLY_CITY_POLLUTION│  │ DAILY_CITY_PM25      │         │
│  └──────────────────────┘  └──────────────────────┘         │
│  ┌──────────────────────┐  ┌──────────────────────────────┐ │
│  │ HOURLY_CITY_POLLUTION│  │ COUNTRY_AIR_QUALITY_INDEX    │ │
│  └──────────────────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## ELT Approach

| Layer | Responsibility | Tool |
|-------|---------------|------|
| **Extract** | Read raw files from S3 | AWS Glue |
| **Load** | Write raw data to Snowflake as-is | AWS Glue (Spark/JDBC) |
| **Transform** | Clean, validate, interpolate, aggregate | Snowflake (procedures + dynamic tables) |

AWS Glue performs **zero transformation** — it only extracts and loads. The raw table schema is dynamically determined by Glue based on the source file structure.

## Automation Summary

| Component | Trigger | Frequency |
|-----------|---------|-----------|
| AWS Glue Job | External (EventBridge/cron) | As scheduled |
| Stream `STREAM_RAW_NEW_DATA` | New rows from Glue | Continuous |
| Task `TASK_PROCESS_NEW_DATA` | Stream has data | Checks every 5 min |
| Task `TASK_FULL_PIPELINE` | Cron schedule | Every 6 hours |
| Task `TASK_MONITOR_PIPELINE` | Cron schedule | Every hour |
| Dynamic Tables (Mart) | Staging table changes | 1-hour target lag |

**No manual execution required.** Once deployed, the pipeline is fully event-driven.

## Pipeline Steps (Deployment Order)

### Step 1: Create Database & Schemas
Creates `OPENAQ` database with 5 schemas: `RAW`, `STAGING`, `MART`, `DATA_QUALITY`, `ETL`.

### Step 2: Raw Table (Glue-Managed) + Pipeline Log
- Table `RAW.AIR_QUALITY_MEASUREMENTS` is created/managed by AWS Glue
- Schema is dynamic — columns may be added or removed between Glue runs
- DO NOT manually create or alter this table
- `ETL.PIPELINE_LOG` table is created here for execution audit tracking

### Step 3: Data Quality Table
`DATA_QUALITY.REJECTED_RECORDS` — stores records that fail quality checks.

### Step 4: Staging Table
`STAGING.AIR_QUALITY_CLEAN` — cleaned data with derived columns (city, date, hour, interpolated values).

### Step 5: Dynamic Table Mart
Four auto-refreshing dynamic tables (1-hour target lag):
1. **MONTHLY_CITY_POLLUTION** — Monthly avg CO & SO2 by city
2. **DAILY_CITY_PM25** — Daily average PM2.5 by city
3. **HOURLY_CITY_POLLUTION** — Hourly PM2.5, CO, SO2 by city
4. **COUNTRY_AIR_QUALITY_INDEX** — AQI score (High/Moderate/Low)

### Step 6: Stored Procedures
| Procedure | Purpose |
|-----------|---------|
| `SP_RUN_DATA_QUALITY(run_id)` | Flags nulls, negatives, outliers |
| `SP_REFRESH_STAGING(run_id)` | Rebuilds clean data with interpolation |
| `SP_RUN_FULL_PIPELINE()` | Orchestrates quality → staging (mart auto-refreshes) |

### Step 7: Stream + Tasks (Automation)
- Stream detects new Glue-loaded rows
- Task triggers full pipeline when new data arrives
- Fallback task runs every 6 hours
- Monitor task checks health every hour
- All tasks are **resumed automatically** (no manual enable needed)

### Step 8: Verify Automation
Uses `SHOW TASKS` and `SHOW DYNAMIC TABLES` to confirm all tasks are running and dynamic tables are active.

### Step 9-10: Analytic Queries & Reporting
Read-only business queries for analysts (not part of automated pipeline).

## Stored Procedure Features

- Error handling with `EXCEPTION WHEN OTHER`
- Execution audit logging to `ETL.PIPELINE_LOG`
- Row count tracking
- Pipeline abort on failure (fail-fast)

## Data Quality Rules

| Rule | Rejection Reason |
|------|-----------------|
| NULL value | `NULL measurement value` |
| NULL datetime | `NULL datetime` |
| NULL/empty location | `NULL or empty location` |
| NULL/empty parameter | `NULL or empty parameter` |
| NULL coordinates | `NULL coordinates` |
| Negative values | `Negative measurement value` |
| >5 std dev from mean | `Extreme outlier (>5 std dev from mean)` |

## Files

| File | Description |
|------|-------------|
| `Untitled.sql` | Complete deployment script — run once to set up the automated pipeline |
| `README.md` | This file |

## Prerequisites

- Snowflake account with `ACCOUNTADMIN` role
- Warehouse: `COMPUTE_WH`
- AWS Glue job configured to write to `OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS` via Snowflake Spark or JDBC connector

## How to Deploy

1. Open `Untitled.sql` in a Snowflake Worksheet
2. Execute all statements top-to-bottom (one-time deployment)
3. Configure AWS Glue to write to `OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS`
4. Pipeline runs automatically from that point — no further manual intervention
