from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession
spark = SparkSession.builder \
    .appName("Databricks Production Code") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema for data
schema = T.StructType([
    T.StructField("id", T.BigIntType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("phone", T.StringType(), True),
    T.StructField("address", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("updated_at", T.TimestampType(), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True),
    T.StructField("metadata", T.MapType(T.StringType(), T.StringType()), True)
])

# Load data into DataFrame
data_path = "/mnt/data/input_data"
df = spark.read.schema(schema).json(data_path)

# Handle NULL values
df = df.fillna({'name': 'Unknown', 'email': 'unknown@example.com'})

# Implement window function for analytics
window_spec = F.window("created_at", "1 day")
df = df.withColumn("daily_count", F.count("id").over(window_spec))

# Delta Lake table path
delta_table_path = "/mnt/delta/test_data"

# Write data to Delta Lake
df.write.format("delta").mode("overwrite").save(delta_table_path)

# Optimize Delta Lake table
delta_table = DeltaTable.forPath(spark, delta_table_path)
delta_table.optimize().executeZOrderBy("created_at")

# Vacuum Delta Lake table
delta_table.vacuum(168)  # Retain 7 days of history

# Error handling and logging
try:
    # Example transformation
    transformed_df = df.withColumn("name_upper", F.upper("name"))
    transformed_df.show()
except Exception as e:
    logger.error("Error during transformation: %s", e)

# Data quality checks
invalid_data_df = df.filter(F.col("email").contains("invalid"))
if invalid_data_df.count() > 0:
    logger.warning("Found invalid email addresses")

# Stop SparkSession
spark.stop()



-- Create Delta Lake table with schema evolution
CREATE TABLE IF NOT EXISTS purgo_playground.test_data (
  id BIGINT,
  name STRING,
  email STRING,
  phone STRING,
  address STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  tags ARRAY<STRING>,
  metadata MAP<STRING, STRING>
) USING DELTA
PARTITIONED BY (created_at)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge new data into Delta Lake table
MERGE INTO purgo_playground.test_data AS target
USING new_data AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- Optimize table for performance
OPTIMIZE purgo_playground.test_data
ZORDER BY (created_at);

-- Vacuum old data
VACUUM purgo_playground.test_data RETAIN 168 HOURS;

-- Validate data quality
SELECT * FROM purgo_playground.test_data
WHERE email LIKE '%invalid%';
