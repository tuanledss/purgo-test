from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType
from datetime import datetime
import json
from Crypto.Cipher import AES
import base64

# Set up Spark session
spark = SparkSession.builder.appName("TestEncryptPIIData").getOrCreate()

# Define the schema for customer_360_raw_clone table
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True)
])

# Sample valid, edge, and error case data
data = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "ACME Corp", "Developer", "123 Elm St", "Metropolis", "NY", "10101", "USA", "Tech", "Alice Smith", "2023-01-01", "2023-02-01", "p1,p2,p3", ""),
    (2, None, None, None, "ACME Corp", "Developer", "123 Elm St", "Metropolis", "NY", "10101", "USA", "Tech", "Alice Smith", "2023-01-01", "2023-02-01", "p1,p2,p3", ""),
    (3, "Jürgen Müller", "jürgen.müller@example.com", "+491234567890", "Müller GmbH", "Engineer", "Bäckerstraße 1", "Berlin", "BE", "10115", "DEU", "Mechanical", "Max Mustermann", "2023-03-01", "2023-03-02", "p4,p5", "This is a note with emoji 🚀")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Define encryption key and initialization vector
key = b'Sixteen byte key'
iv = b'Sixteen byte IV__'

# Encryption utility function using AES encryption
def encrypt(text):
    if text is None:
        return text
    cipher = AES.new(key, AES.MODE_CFB, iv)
    ct_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(ct_bytes).decode('utf-8')

# UDF to encrypt columns
encrypt_udf = udf(encrypt, StringType())

# Encrypt the specified PII columns
encrypted_df = df.withColumn("encrypted_name", encrypt_udf(col("name")))\
                 .withColumn("encrypted_email", encrypt_udf(col("email")))\
                 .withColumn("encrypted_phone", encrypt_udf(col("phone")))\
                 .withColumn("encrypted_zip", encrypt_udf(col("zip")))

# Drop original columns and rename encrypted columns
final_df = encrypted_df.drop("name", "email", "phone", "zip")\
    .withColumnRenamed("encrypted_name", "name")\
    .withColumnRenamed("encrypted_email", "email")\
    .withColumnRenamed("encrypted_phone", "phone")\
    .withColumnRenamed("encrypted_zip", "zip")

# Assert schema matches expected encrypted version
expected_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True)
])

# Validate Schema
assert final_df.schema == expected_schema, "Schema does not match expected schema after encryption"

# Validate Null handling
encrypted_null_values = final_df.filter((col("name").isNull()) | (col("email").isNull()) | (col("phone").isNull()) | (col("zip").isNull())).count()
assert encrypted_null_values == 1, "Null handling is incorrect for encrypted columns"

# Test Delta Lake output operations
final_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Validate table existence
tables = spark.sql("SHOW TABLES IN purgo_playground").collect()
assert any("customer_360_raw_clone" in table.tableName for table in tables), "Table customer_360_raw_clone was not created"

# Validate performance: Basic count check to verify data load
row_count = spark.sql("SELECT COUNT(*) FROM purgo_playground.customer_360_raw_clone").collect()[0][0]
assert row_count == 3, f"Expected 3 rows in customer_360_raw_clone table, but found {row_count}"

# Save encryption key to JSON file
encryption_key = {
    "key": base64.b64encode(key).decode('utf-8'),
    "iv": base64.b64encode(iv).decode('utf-8')
}
current_datetime = datetime.utcnow().strftime("%Y%m%d%H%M%S")
json_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Save the key, handling potential I/O errors
try:
    dbutils.fs.put(json_path, json.dumps(encryption_key), overwrite=True)
except Exception as e:
    raise AssertionError(f"Failed to save JSON file at specified location: {e}")

# Cleanup operations, dropping the table to reset state
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

spark.stop()
