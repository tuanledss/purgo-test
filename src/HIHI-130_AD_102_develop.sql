-- SQL Unit Test Structure for Databricks SQL Operations
-- Requirements: Ensure SQL syntax, assertions, and queries use Databricks SQL standards
-- Test Delta Lake Operations with SQL

/* Section: Validate Table Existence and Proper Schema Setup */
-- Check existence of raw data table
SELECT COUNT(*) AS table_exists 
FROM information_schema.tables 
WHERE table_name = 'customer_360_raw';

-- Create a replica table (customer_360_raw_clone) as part of setup
CREATE TABLE IF NOT EXISTS purgo_playground.customer_360_raw_clone AS
SELECT * FROM purgo_playground.customer_360_raw;

/* Section: Validate Schema Integrity */
-- Assert schema match between original and replica tables
SELECT 
    a.column_name as orig_col,
    b.column_name as clone_col
FROM information_schema.columns a
JOIN information_schema.columns b 
    ON a.column_name = b.column_name
WHERE a.table_name = 'customer_360_raw' 
  AND b.table_name = 'customer_360_raw_clone';

/* Section: Validate Delta Operations and SQL Assertions */
-- Test MERGE Operation
MERGE INTO purgo_playground.customer_360_raw_clone AS target
USING purgo_playground.customer_updates AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.phone = source.new_phone
WHEN NOT MATCHED THEN INSERT (id, name, email, phone, zip) VALUES (source.id, source.name, source.email, source.new_phone, source.new_zip);

/* Validate Update Operation */
UPDATE purgo_playground.customer_360_raw_clone
SET phone = '000-000-0000' 
WHERE id = 1;

/* Validate Delete Operation */
DELETE FROM purgo_playground.customer_360_raw_clone 
WHERE is_churn = 1;



# PySpark Unit Tests for PII Encryption and Batch Processing
# Requirements: Use PySpark testing approach for transformations and integration tests 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, udf
import base64
from Crypto.Cipher import AES
import unittest

# Initialize Spark session
spark = SparkSession.builder.appName("PII Encryption Unit Tests").getOrCreate()

# Define a simple schema for testing
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True)
])

# Sample test data
test_data = [
    (1, "Alice", "alice@example.com", "1234567890", "12345", "Tech Corp", "Engineer"),
    (2, "Bob", "bob@example.com", "0987654321", "54321", "Biz Inc", "Manager"),
    (3, None, None, None, None, "Null Inc", "Officer")
]

# Create DataFrame from test data
test_df = spark.createDataFrame(test_data, schema)

# Encryption setup
key = b'Sixteen byte key'
iv = b'Sixteen byte IV__'

# Encryption function
def encrypt(value):
    if value is None:
        return None
    cipher = AES.new(key, AES.MODE_CFB, iv)
    encrypted_bytes = cipher.encrypt(value.encode('utf-8'))
    return base64.b64encode(encrypted_bytes).decode('utf-8')

# Register UDF
encrypt_udf = udf(encrypt, StringType())

# Transform DataFrame
encrypted_df = test_df.withColumn("encrypted_name", encrypt_udf(col("name")))\
                      .withColumn("encrypted_email", encrypt_udf(col("email")))\
                      .withColumn("encrypted_phone", encrypt_udf(col("phone")))\
                      .withColumn("encrypted_zip", encrypt_udf(col("zip")))

# Define test case
class TestPIIEncryption(unittest.TestCase):
    def setUp(self):
        self.df = encrypted_df

    def test_encryption(self):
        # Validate non-null transform
        self.assertIsNotNone(self.df.filter(col("name").isNotNull()).select("encrypted_name").collect()[0][0])
        # Validate null handling
        self.assertIsNone(self.df.filter(col("name").isNull()).select("encrypted_name").collect()[0][0])

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

spark.stop()
