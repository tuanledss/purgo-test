# PySpark script for encrypting PII data in Databricks

from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import json
from datetime import datetime

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of the customer_360_raw table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS
SELECT * FROM purgo_playground.customer_360_raw
""")

# Load data from the cloned table
df = spark.table("purgo_playground.customer_360_raw_clone")

# Generate encryption key
key = Fernet.generate_key()
fernet = Fernet(key)

# Function to encrypt a column
def encrypt_column(df, column_name):
    return df.withColumn(column_name, col(column_name).cast("string").rdd.map(lambda x: fernet.encrypt(x.encode()).decode()).toDF())

# Encrypt specified PII columns
for column in ["name", "email", "phone", "zip"]:
    df = encrypt_column(df, column)

# Save encrypted data back to the table
df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key as a JSON file
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
dbutils.fs.put(key_file_path, json.dumps({"encryption_key": key.decode()}), overwrite=True)
-- SQL script for testing and validation in Databricks

-- Validate schema of customer_360_raw_clone
DESCRIBE TABLE purgo_playground.customer_360_raw_clone;

-- Validate data types of PII columns
SELECT 
  CASE WHEN data_type = 'string' THEN 'PASS' ELSE 'FAIL' END AS name_type_check,
  CASE WHEN data_type = 'string' THEN 'PASS' ELSE 'FAIL' END AS email_type_check,
  CASE WHEN data_type = 'string' THEN 'PASS' ELSE 'FAIL' END AS phone_type_check,
  CASE WHEN data_type = 'string' THEN 'PASS' ELSE 'FAIL' END AS zip_type_check
FROM information_schema.columns
WHERE table_name = 'customer_360_raw_clone' AND column_name IN ('name', 'email', 'phone', 'zip');

-- Test encryption by checking that encrypted values are not equal to original values
SELECT 
  CASE WHEN name != 'John Doe' THEN 'PASS' ELSE 'FAIL' END AS name_encryption_check,
  CASE WHEN email != 'john.doe@example.com' THEN 'PASS' ELSE 'FAIL' END AS email_encryption_check,
  CASE WHEN phone != '1234567890' THEN 'PASS' ELSE 'FAIL' END AS phone_encryption_check,
  CASE WHEN zip != '10001' THEN 'PASS' ELSE 'FAIL' END AS zip_encryption_check
FROM purgo_playground.customer_360_raw_clone
WHERE id = 1;

-- Cleanup: Drop the test table after validation
DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone;