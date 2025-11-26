import sys,json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
import boto3
from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp, lit
from pyspark.sql.types import TimestampType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

warehouse = "s3://glue-practice-31052025/curated/employee/"

spark = (
    SparkSession.builder.appName("staging_to_curated_iceberg")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

#read staging table
input_df = spark.read.format("iceberg").load("glue_catalog.targetemployeedb.emp_stg")

s3 = boto3.client("s3")
# read control table
def read_control(default_ts="1972-01-01T00:00:00Z"):
        obj = s3.get_object(Bucket="glue-practice-31052025", Key="curated/control/control_file.json")
        print("obj: ",obj)
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        return payload.get("last_updated_ts", default_ts)
        print("Exception occured")
        return default_ts

#write to control table        
def write_control(last_ts):
    s3.put_object(
        Bucket="glue-practice-31052025",
        Key="curated/control/control_file.json",
        Body=json.dumps({"last_updated_ts": last_ts}, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

last_updated_ts = read_control()
print("curated: last_updated_ts: ", last_updated_ts)

src = (
    spark.table("glue_catalog.targetemployeedb.emp_stg")
         .withColumn("load_ts", col("load_ts").cast("timestamp"))
         .filter(col("load_ts") > to_timestamp(lit(last_updated_ts)))
)

print("Incremental Iceberg rows:", src.count())
# -------------------- TARGET ICEBERG TABLE --------------------
iceberg_table = "glue_catalog.curatedemployeedb.emp_scd2"

src.createOrReplaceTempView("incoming")
# Create Iceberg SCD2 table if missing
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {iceberg_table} (
    employee_id BIGINT,
    education STRING,
    joiningyear BIGINT,
    city STRING,
    paymenttier BIGINT,
    age BIGINT,
    gender STRING,
    everbenched STRING,
    experienceincurrentdomain BIGINT,
    leaveornot BIGINT,
    load_ts TIMESTAMP,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN
) USING iceberg
PARTITIONED BY (joiningyear)
""")

# -------------------- INITIALIZE TABLE (if empty) --------------------
existing_count = spark.sql(f"SELECT COUNT(*) AS c FROM {iceberg_table}").collect()[0]["c"]

print("target existing count: ", existing_count)
if existing_count == 0:
    print("Initializing Iceberg SCD2 table...")

    init_df = (
    src.withColumn("employee_id", col("nk_id"))
       .withColumn("effective_start_date", current_timestamp())
       .withColumn("effective_end_date", lit(None).cast("timestamp"))
       .withColumn("is_current", lit(True))
       .select(
           "employee_id",
           "education",
           "joiningyear",
           "city",
           "paymenttier",
           "age",
           "gender",
           "everbenched",
           "experienceincurrentdomain",
           "leaveornot",
           "load_ts",
           "effective_start_date",
           "effective_end_date",
           "is_current"
       )
    )
    print("init_df:")
    print(init_df.show())
    init_df.writeTo(iceberg_table).append()
    print("Initialized table with:", init_df.count())
    # Continue processing next incremental batch normally.
    
    # -------------------- STEP 1: EXPIRE CURRENT ROWS --------------------
spark.sql(f"""
MERGE INTO {iceberg_table} t
USING (
    SELECT nk_id AS employee_id, load_ts AS new_ts
    FROM (
        SELECT nk_id, load_ts,
        ROW_NUMBER() OVER (PARTITION BY nk_id ORDER BY load_ts DESC) AS rn
        FROM incoming
    ) WHERE rn = 1
) s
ON t.employee_id = s.employee_id AND t.is_current = true
WHEN MATCHED AND s.new_ts > t.load_ts
THEN UPDATE SET
    t.effective_end_date = current_timestamp(),
    t.is_current = false
""")

print("Expired SCD2 rows successfully.")

# -------------------- STEP 2: INSERT NEW “CURRENT” ROW VERSIONS --------------------
to_insert = spark.sql(f"""
SELECT 
    s.nk_id AS employee_id,
    s.education,
    s.joiningyear,
    s.city,
    s.paymenttier,
    s.age,
    s.gender,
    s.everbenched,
    s.experienceincurrentdomain,
    s.leaveornot,
    s.load_ts,
    current_timestamp() AS effective_start_date,
    NULL AS effective_end_date,
    true AS is_current
FROM incoming s
LEFT JOIN (
    SELECT employee_id, load_ts AS t_load_ts
    FROM {iceberg_table} WHERE is_current = true
) t
ON s.nk_id = t.employee_id
WHERE t.employee_id IS NULL
   OR s.load_ts > t.t_load_ts
""")

to_insert.writeTo(iceberg_table).append()
print("Inserted rows:", to_insert.count())

# -------------------- STEP 3: UPDATE CONTROL TABLE --------------------
max_ts_row = src.selectExpr("max(load_ts) as m").collect()[0]
print("max_ts_row: ",max_ts_row)
max_ts = max_ts_row["m"].strftime("%Y-%m-%dT%H:%M:%S")
write_control(max_ts)

print("SCD2 processing completed.")
