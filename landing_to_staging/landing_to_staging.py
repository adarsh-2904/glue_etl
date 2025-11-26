import sys, json, boto3
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp, lit

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
warehouse = "s3://glue-practice-31052025/staging/employee/"
spark = (
    SparkSession.builder.appName("landing_to_staging_iceberg")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

df = spark.read.csv("s3://glue-practice-31052025/landing/delta_dataset.csv", inferSchema=True, header = True)

if "action_type" in df.columns:
    df = df.drop("action_type")
    
print("df count: ",df.count())

s3 = boto3.client("s3")
def read_control(default_ts="1972-01-01T00:00:00Z"):
    try:
        obj = s3.get_object(Bucket="glue-practice-31052025", Key="staging/control/control_f.json")
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        print("last_updated_ts from json: ", payload.get("last_updated_ts", default_ts))
        return payload.get("last_updated_ts", default_ts)
    except s3.exceptions.NoSuchKey:
        return default_ts
        
def write_control(last_ts):
    s3.put_object(
        Bucket="glue-practice-31052025",
        Key="staging/control/control_f.json",
        Body=json.dumps({"last_updated_ts": last_ts}, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

last_updated_ts = read_control()

print("last_updated_ts: ",last_updated_ts)
print("df result:")
# Convert load_ts to proper timestamp
df = df.withColumn("load_ts", to_timestamp(col("load_ts"), "dd-MM-yyyy HH:mm"))
print(df.show())
# Filter incremental records
df_inc = df.filter(
    col("load_ts") > to_timestamp(lit(last_updated_ts), "yyyy-MM-dd'T'HH:mm:ss")
)

print("record count for incremental dataframe: ",df_inc.count())

# --------- Iceberg Upsert to STAGING ----------
df_inc.createOrReplaceTempView("incoming_data")

# If table doesn’t exist, create it
spark.sql(f"""
CREATE TABLE IF NOT EXISTS glue_catalog.targetemployeedb.emp_stg (
    education STRING,
    joiningyear BIGINT,
    city STRING,
    paymenttier BIGINT,
    age BIGINT,
    gender STRING,
    everbenched STRING,
    experienceincurrentdomain BIGINT,
    leaveornot BIGINT,
    nk_id BIGINT,
    load_ts STRING
) USING iceberg
PARTITIONED BY (joiningyear)
""")

# Merge new changes (upsert) into Iceberg table
spark.sql(f"""
MERGE INTO glue_catalog.targetemployeedb.emp_stg t
USING incoming_data s
ON t.nk_id = s.nk_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# --------- Update control table ----------
max_ts_row = df_inc.selectExpr("max(load_ts) as m").collect()[0]
print("max_ts_row: ",max_ts_row)
max_ts = max_ts_row["m"].strftime("%Y-%m-%dT%H:%M:%S")
write_control(max_ts)

print(f"Landing→Staging Iceberg merge complete. Control updated to {max_ts}")
spark.stop()

