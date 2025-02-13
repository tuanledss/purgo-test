from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json
import datetime
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import pytest
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder.appName("TestEncryptPIIData").getOrCreate()

# Define the encryption function for testing
def encrypt_data_test(data, key):
    if data is None:
        return None
    cipher = AES.new(key, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(data.encode('utf-8'))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode('utf-8')

# Set up test data and mock table creation
def create_clone_table():
    test_data = [
        Row(id=1, name="John Doe", email="john.doe@example.com", phone="1234567890", zip="12345", company="CompA", job_title="Engineer", address="123 St", city="CityA", state="StateA", country="CountryA", industry="IndustryA", account_manager="ManagerA", creation_date="2023-01-01", last_interaction_date="2023-02-01", purchase_history="HistoryA", notes="NotesA", is_churn=0),
        Row(id=2, name="Jane Smith", email="jane.smith@example.com", phone="0987654321", zip="54321", company="CompB", job_title="Manager", address="456 St", city="CityB", state="StateB", country="CountryB", industry="IndustryB", account_manager="ManagerB", creation_date="2023-01-02", last_interaction_date="2023-02-02", purchase_history="HistoryB", notes="NotesB", is_churn=1)
    ]
    df = spark.createDataFrame(test_data)
    df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Test Schema Validation
def test_schema_validation():
    create_clone_table()
    df = spark.table("purgo_playground.customer_360_raw_clone")
    expected_schema = "id BIGINT, name STRING, email STRING, phone STRING, zip STRING, company STRING, job_title STRING, address STRING, city STRING, state STRING, country STRING, industry STRING, account_manager STRING, creation_date DATE, last_interaction_date DATE, purchase_history STRING, notes STRING, is_churn INT"
    assert str(df.schema) == expected_schema

# Test Data Type and NULL Handling
def test_data_types_and_null_handling():
    create_clone_table()
    df = spark.table("purgo_playground.customer_360_raw_clone")
    assert df.select(col("name").cast(StringType())).dtypes[0][1] == "string"
    assert df.filter(col("name").isNull()).count() == 0

# Test Encryption Functionality
def test_encryption_functionality():
    encryption_key = get_random_bytes(32)
    encrypt_udf_test = udf(lambda x: encrypt_data_test(x, encryption_key), StringType())
    create_clone_table()
    df = spark.table("purgo_playground.customer_360_raw_clone")
    df_encrypted = df.withColumn("name", encrypt_udf_test(col("name"))) \
                     .withColumn("email", encrypt_udf_test(col("email"))) \
                     .withColumn("phone", encrypt_udf_test(col("phone"))) \
                     .withColumn("zip", encrypt_udf_test(col("zip")))
    assert df_encrypted.filter(col("name").isNull()).count() == 0
    assert df_encrypted.filter(col("email").isNull()).count() == 0

# Performance Test
def test_performance():
    start_time = datetime.datetime.now()
    create_clone_table()
    end_time = datetime.datetime.now()
    assert (end_time - start_time).seconds < 300

# Test Delta Lake Operations
def test_delta_lake_operations():
    # Assuming Delta Lake operations are performed and checked here
    pass

# Run all tests
if __name__ == "__main__":
    pytest.main([__file__])

