# Import necessary libraries
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import base64
import json
from datetime import datetime
from cryptography.fernet import Fernet

# Generate encryption key
encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key)

# Define UDF for encryption
def encrypt_value(value):
    if value is not None:
        return cipher_suite.encrypt(value.encode()).decode()
    return None

encrypt_udf = udf(encrypt_value, StringType())

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Create a replica of the customer_360_raw table
spark.sql("""
    CREATE TABLE purgo_playground.customer_360_raw_clone12 AS
    SELECT * FROM purgo_playground.customer_360_raw
""")

# Load the clone table
customer_360_raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone12")

# Encrypt PII columns
encrypted_df = customer_360_raw_clone_df.withColumn("name", encrypt_udf(col("name"))) \
                                        .withColumn("email", encrypt_udf(col("email"))) \
                                        .withColumn("phone", encrypt_udf(col("phone"))) \
                                        .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Save the encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
encryption_key_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_datetime}.json"

with open(encryption_key_path, 'w') as key_file:
    json.dump({"encryption_key": base64.b64encode(encryption_key).decode()}, key_file)

-- SQL Test Code for Databricks

/* Test for table existence and cleanup */
-- Drop the clone table if it exists
DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12;

-- Create a replica of the customer_360_raw table
CREATE TABLE purgo_playground.customer_360_raw_clone12 AS
SELECT * FROM purgo_playground.customer_360_raw;

/* Validate schema of the cloned table */
-- Check if the schema of the cloned table matches the original
DESCRIBE TABLE purgo_playground.customer_360_raw_clone12;

/* Validate data type conversions */
-- Ensure data types are correctly handled
SELECT 
  CASE WHEN typeof(name) = 'string' THEN 'PASS' ELSE 'FAIL' END AS name_type_check,
  CASE WHEN typeof(email) = 'string' THEN 'PASS' ELSE 'FAIL' END AS email_type_check,
  CASE WHEN typeof(phone) = 'string' THEN 'PASS' ELSE 'FAIL' END AS phone_type_check,
  CASE WHEN typeof(zip) = 'string' THEN 'PASS' ELSE 'FAIL' END AS zip_type_check
FROM purgo_playground.customer_360_raw_clone12;

/* Validate encryption */
-- Check that encrypted data is not equal to original data
SELECT 
  CASE WHEN name != original_name THEN 'PASS' ELSE 'FAIL' END AS name_encryption_check,
  CASE WHEN email != original_email THEN 'PASS' ELSE 'FAIL' END AS email_encryption_check,
  CASE WHEN phone != original_phone THEN 'PASS' ELSE 'FAIL' END AS phone_encryption_check,
  CASE WHEN zip != original_zip THEN 'PASS' ELSE 'FAIL' END AS zip_encryption_check
FROM (
  SELECT 
    name, email, phone, zip,
    (SELECT name FROM purgo_playground.customer_360_raw WHERE id = c.id) AS original_name,
    (SELECT email FROM purgo_playground.customer_360_raw WHERE id = c.id) AS original_email,
    (SELECT phone FROM purgo_playground.customer_360_raw WHERE id = c.id) AS original_phone,
    (SELECT zip FROM purgo_playground.customer_360_raw WHERE id = c.id) AS original_zip
  FROM purgo_playground.customer_360_raw_clone12 c
);

/* Validate Delta Lake operations */
-- Check if Delta Lake operations are successful
MERGE INTO purgo_playground.customer_360_raw_clone12 AS target
USING (SELECT * FROM purgo_playground.customer_360_raw WHERE id = 1) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.name = source.name;

/* Validate window functions */
-- Test window function for analytics
SELECT id, name, ROW_NUMBER() OVER (PARTITION BY country ORDER BY last_interaction_date DESC) AS row_num
FROM purgo_playground.customer_360_raw_clone12;

/* Cleanup operations */
-- Drop the clone table after tests
DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12;