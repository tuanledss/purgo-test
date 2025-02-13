# Ensure necessary libraries are installed
# %pip install pycryptodome

# PySpark Test Script
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType
from datetime import datetime
import base64
from Crypto.Cipher import AES
import pytest
import json

# Set up Spark session
spark = SparkSession.builder.appName("Test Encryption Transformations").getOrCreate()

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

# Sample data for testing
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

# Encryption utility function using AES
def encrypt(text):
    if text is None:
        return text
    cipher = AES.new(key, AES.MODE_CFB, iv)
    ct_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(ct_bytes).decode('utf-8')

# UDF to handle encryption
encrypt_udf = udf(encrypt, StringType())

# Transform DataFrame with encrypted fields
encrypted_df = df.withColumn("encrypted_name", encrypt_udf(col("name")))\
                 .withColumn("encrypted_email", encrypt_udf(col("email")))\
                 .withColumn("encrypted_phone", encrypt_udf(col("phone")))\
                 .withColumn("encrypted_zip", encrypt_udf(col("zip")))

final_df = encrypted_df.drop("name", "email", "phone", "zip")\
    .withColumnRenamed("encrypted_name", "name")\
    .withColumnRenamed("encrypted_email", "email")\
    .withColumnRenamed("encrypted_phone", "phone")\
    .withColumnRenamed("encrypted_zip", "zip")

# Test Utility Function
def test_encrypt_function():
    assert encrypt("Test") != "Test"
    assert encrypt(None) == None

# Test Schema
def test_schema():
    # Expected Schema
    field_names = [field.name for field in final_df.schema.fields]
    assert "name" in field_names
    assert "email" in field_names
    assert "phone" in field_names
    assert "zip" in field_names

# Save encryption key to JSON for integration test
encryption_key = {
    "key": base64.b64encode(key).decode('utf-8'),
    "iv": base64.b64encode(iv).decode('utf-8')
}

current_datetime = datetime.utcnow().strftime("%Y%m%d%H%M%S")
json_path = f"/path/to/encryption_key_{current_datetime}.json"

# Write JSON File
def save_json_key():
    with open(json_path, 'w') as json_file:
        json.dump(encryption_key, json_file)

# Test Save JSON
def test_json_save():
    try:
        save_json_key()
        assert True
    except Exception as e:
        assert False, f"Failed to save JSON: {str(e)}"

# Cleanup
# Note: This assumes the existence of the purgo_playground namespace and customer_360_raw_clone table is intended to be dropped.
spark.stop()
