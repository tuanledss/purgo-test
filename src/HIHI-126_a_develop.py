from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Delta Lake Example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema
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

# Load data
data_path = "/mnt/delta/test_data"
if DeltaTable.isDeltaTable(spark, data_path):
    delta_table = DeltaTable.forPath(spark, data_path)
else:
    df = spark.createDataFrame([], schema)
    df.write.format("delta").save(data_path)
    delta_table = DeltaTable.forPath(spark, data_path)

# Function to merge data
def merge_data(new_data):
    try:
        delta_table.alias("old").merge(
            new_data.alias("new"),
            "old.id = new.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("Data merged successfully.")
    except Exception as e:
        logger.error(f"Error merging data: {e}")

# Example data
new_data = spark.createDataFrame([
    (1, "John Doe", "john.doe@example.com", "123-456-7890", "123 Main St", F.current_timestamp(), F.current_timestamp(), ["tag1", "tag2"], {"key1": "value1"}),
    (2, "Jane Doe", "jane.doe@example.com", "987-654-3210", "456 Elm St", F.current_timestamp(), F.current_timestamp(), ["tag3", "tag4"], {"key2": "value2"})
], schema)

# Merge new data
merge_data(new_data)

# Optimize and vacuum
spark.sql(f"OPTIMIZE delta.`{data_path}` ZORDER BY (id)")
spark.sql(f"VACUUM delta.`{data_path}` RETAIN 0 HOURS")

# Read data with time travel
version_0_df = spark.read.format("delta").option("versionAsOf", 0).load(data_path)
version_0_df.show()

# Stop Spark session
spark.stop()



-- Create Delta table
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
PARTITIONED BY (created_at);

-- Insert data
MERGE INTO purgo_playground.test_data AS target
USING (
  SELECT 1 AS id, 'John Doe' AS name, 'john.doe@example.com' AS email, '123-456-7890' AS phone, '123 Main St' AS address, current_timestamp() AS created_at, current_timestamp() AS updated_at, ARRAY('tag1', 'tag2') AS tags, MAP('key1', 'value1') AS metadata
  UNION ALL
  SELECT 2, 'Jane Doe', 'jane.doe@example.com', '987-654-3210', '456 Elm St', current_timestamp(), current_timestamp(), ARRAY('tag3', 'tag4'), MAP('key2', 'value2')
) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- Optimize and vacuum
OPTIMIZE purgo_playground.test_data ZORDER BY (id);
VACUUM purgo_playground.test_data RETAIN 0 HOURS;

-- Time travel query
SELECT * FROM purgo_playground.test_data TIMESTAMP AS OF '2024-03-21T00:00:00.000+0000';
