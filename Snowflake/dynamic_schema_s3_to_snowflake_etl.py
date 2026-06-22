/*
 Dynamic Schema Evolution Pipeline: S3 → AWS Glue → Snowflake with Automatic Table Creation and Audit Tracking
High-Level Description

Developed a metadata-driven AWS Glue PySpark ETL pipeline that reads data from the Glue Catalog, automatically handles schema changes (column additions/removals), generates Snowflake DDL dynamically, enriches data with audit columns, and loads the transformed data into Snowflake for scalable analytics.
*/

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit, to_timestamp
from pyspark.sql.types import *
import snowflake.connector

# -------------------------------------------------
# Initialize Glue
# -------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# -------------------------------------------------
# Read Glue Catalog
# -------------------------------------------------
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="openaq_db",
    table_name="raw"
)

df = source_dyf.toDF()

print("Original Columns:")
print(df.columns)

# -------------------------------------------------
# Remove Duplicates
# -------------------------------------------------
df = df.dropDuplicates()

# -------------------------------------------------
# Convert datetime column
# -------------------------------------------------
if "datetime" in [c.lower() for c in df.columns]:
    df = df.withColumn(
        "datetime",
        to_timestamp("datetime")
    )

# -------------------------------------------------
# Audit Columns
# -------------------------------------------------
df = df.withColumn(
    "source_name",
    lit("openaq_s3")
)

df = df.withColumn(
    "source_file",
    lit("location-2178-20220503.csv")
)

df = df.withColumn(
    "ingested_at",
    current_timestamp()
)

print("Final Columns:")
print(df.columns)
print("Column Count:", len(df.columns))

# -------------------------------------------------
# Generate Dynamic Snowflake DDL
# -------------------------------------------------
column_defs = []

for field in df.schema.fields:

    if isinstance(field.dataType, (IntegerType, LongType)):
        sf_type = "INTEGER"

    elif isinstance(field.dataType, (DoubleType, FloatType, DecimalType)):
        sf_type = "FLOAT"

    elif isinstance(field.dataType, TimestampType):
        sf_type = "TIMESTAMP_TZ"

    else:
        sf_type = "VARCHAR(500)"

    column_defs.append(
        f'"{field.name.upper()}" {sf_type}'
    )

ddl = f"""
CREATE OR REPLACE TABLE OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS
(
{','.join(column_defs)}
)
"""

# -------------------------------------------------
# Add Cluster Key
# -------------------------------------------------
cols = [c.lower() for c in df.columns]

if "datetime" in cols and "parameter" in cols:
    ddl += """
CLUSTER BY (DATETIME, PARAMETER)
"""

ddl += """
COMMENT = 'Raw air quality measurements - clustered for TB-scale query performance';
"""

print("Generated DDL:")
print(ddl)

# -------------------------------------------------
# Create Table in Snowflake
# -------------------------------------------------
conn = snowflake.connector.connect(
    account="HNIKFAR-LE41618",
    user="BHASKAR",
    password="Sanikareddy583",
    warehouse="COMPUTE_WH",
    database="OPENAQ",
    schema="RAW",
    role="ACCOUNTADMIN"
)

cur = conn.cursor()

cur.execute("CREATE DATABASE IF NOT EXISTS OPENAQ")
cur.execute("CREATE SCHEMA IF NOT EXISTS OPENAQ.RAW")
cur.execute("DROP TABLE IF EXISTS OPENAQ.RAW.AIR_QUALITY_MEASUREMENTS")
cur.execute(ddl)

cur.close()
conn.close()

# -------------------------------------------------
# Convert to DynamicFrame
# -------------------------------------------------
final_dyf = DynamicFrame.fromDF(
    df,
    glueContext,
    "final_dyf"
)

# -------------------------------------------------
# Snowflake Connection
# -------------------------------------------------
snowflake_options = {
    "sfURL": "HNIKFAR-LE41618.snowflakecomputing.com",
    "sfUser": "BHASKAR",
    "sfPassword": "Sanikareddy583",
    "sfDatabase": "OPENAQ",
    "sfSchema": "RAW",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "dbtable": "AIR_QUALITY_MEASUREMENTS"
}

# -------------------------------------------------
# Load to Snowflake
# -------------------------------------------------
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="snowflake",
    connection_options=snowflake_options
)

print("Data Loaded Successfully Into Snowflake")

job.commit()