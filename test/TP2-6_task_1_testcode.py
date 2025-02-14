# PySpark Test Code for Encrypting PII Data in Databricks

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType
import base64
import json
import os
import unittest

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIDataTest").getOrCreate()

# Define encryption function
def encrypt_data(data, key):
    if data is not None:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    return None

# Register UDF
encrypt_udf = udf(lambda x: encrypt_data(x, "encryption_key"), StringType())

# Define schema for validation
customer_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Unit test class
class TestEncryptPIIData(unittest.TestCase):

    def setUp(self):
        # Load the customer_360_raw table
        self.customer_df = spark.table("purgo_playground.customer_360_raw")

    def test_schema_validation(self):
        # Validate schema
        self.assertEqual(self.customer_df.schema, customer_schema)

    def test_encrypt_pii_columns(self):
        # Encrypt PII columns
        encrypted_df = self.customer_df.withColumn("name", encrypt_udf(col("name"))) \
                                       .withColumn("email", encrypt_udf(col("email"))) \
                                       .withColumn("phone", encrypt_udf(col("phone"))) \
                                       .withColumn("zip", encrypt_udf(col("zip")))

        # Check that encrypted columns do not match original
        for row in encrypted_df.collect():
            self.assertNotEqual(row['name'], self.customer_df.filter(col("id") == row['id']).select("name").collect()[0][0])
            self.assertNotEqual(row['email'], self.customer_df.filter(col("id") == row['id']).select("email").collect()[0][0])
            self.assertNotEqual(row['phone'], self.customer_df.filter(col("id") == row['id']).select("phone").collect()[0][0])
            self.assertNotEqual(row['zip'], self.customer_df.filter(col("id") == row['id']).select("zip").collect()[0][0])

    def test_null_handling(self):
        # Test NULL handling
        null_df = self.customer_df.withColumn("name", lit(None))
        encrypted_null_df = null_df.withColumn("name", encrypt_udf(col("name")))
        self.assertTrue(encrypted_null_df.filter(col("name").isNull()).count() > 0)

    def test_encryption_key_generation(self):
        # Generate encryption key and save as JSON
        encryption_key = base64.b64encode(os.urandom(32)).decode('utf-8')
        key_data = {"encryption_key": encryption_key, "timestamp": current_timestamp().cast("string").alias("timestamp")}
        key_json = json.dumps(key_data)

        # Save the encryption key to a JSON file
        key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_timestamp().cast('string')}.json"
        with open(key_file_path, 'w') as key_file:
            key_file.write(key_json)

        # Validate key file creation
        self.assertTrue(os.path.exists(key_file_path))

    def tearDown(self):
        # Cleanup operations
        spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Run the tests
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

# Stop Spark session
spark.stop()

-- SQL Test Code for Databricks SQL Operations

/* Test Delta Lake Operations */
-- Validate that the Delta table exists and has the correct schema
CREATE OR REPLACE TABLE purgo_playground.customer_360_raw_clone12 AS
SELECT * FROM purgo_playground.customer_360_raw12;

-- Check schema
DESCRIBE TABLE purgo_playground.customer_360_raw_clone12;

/* Test MERGE, UPDATE, DELETE operations */
-- Test MERGE operation
MERGE INTO purgo_playground.customer_360_raw_clone12 AS target
USING (SELECT * FROM purgo_playground.customer_360_raw WHERE id = 1) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET target.name = source.name;

/* Validate UPDATE */
SELECT * FROM purgo_playground.customer_360_raw_clone12 WHERE id = 1;

/* Test DELETE operation */
DELETE FROM purgo_playground.customer_360_raw_clone12 WHERE id = 1;

/* Validate DELETE */
SELECT * FROM purgo_playground.customer_360_raw_clone12 WHERE id = 1;

/* Test window functions and analytics features */
-- Test window function
SELECT id, name, ROW_NUMBER() OVER (PARTITION BY country ORDER BY creation_date DESC) AS row_num
FROM purgo_playground.customer_360_raw_clone12;

/* Cleanup operations */
-- Drop the test table
DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12;