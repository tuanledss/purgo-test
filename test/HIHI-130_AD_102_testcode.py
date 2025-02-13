from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, expr
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType, NullType
from datetime import datetime
import json
from Crypto.Cipher import AES
import base64

# Set up Spark session
spark = SparkSession.builder.appName("Encrypt PII Data Test Code").getOrCreate()

# Define schema for testing the customer_360_raw_clone
schema_test = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True)
])

# Sample data for unit tests
data_test = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "10101"),
    (2, None, None, None, None),
    (3, "Jürgen Müller", "jürgen.müller@example.com", "+49 1234 567890", "10115")
]

# Create test DataFrame
df_test = spark.createDataFrame(data_test, schema=schema_test)

# Define encryption key and initialization vector
key = b'Sixteen byte key'
iv = b'Sixteen byte IV__'

# Define encryption utility function
def encrypt(text):
    if text is None:
        return None
    cipher = AES.new(key, AES.MODE_CFB, iv)
    ct_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(ct_bytes).decode('utf-8')

# Register UDF for encrypting data
encrypt_udf = udf(encrypt, StringType())

# Sample test execution
# Encryption test for PII columns
encrypted_df_test = df_test.withColumn("encrypted_name", encrypt_udf(col("name")))\
    .withColumn("encrypted_email", encrypt_udf(col("email")))\
    .withColumn("encrypted_phone", encrypt_udf(col("phone")))\
    .withColumn("encrypted_zip", encrypt_udf(col("zip")))

# Checking column data types post encryption
assert encrypted_df_test.schema["encrypted_name"].dataType == StringType()
assert encrypted_df_test.schema["encrypted_email"].dataType == StringType()
assert encrypted_df_test.schema["encrypted_phone"].dataType == StringType()
assert encrypted_df_test.schema["encrypted_zip"].dataType == StringType()

# Validate NULL handling (if None remains None after encryption)
null_encryption_test = df_test.filter(col("id") == 2).select("encrypted_name", "encrypted_email",
                                                               "encrypted_phone", "encrypted_zip")\
    .collect()

for row in null_encryption_test:
    assert row["encrypted_name"] is None
    assert row["encrypted_email"] is None
    assert row["encrypted_phone"] is None
    assert row["encrypted_zip"] is None

# Validate data integrity of non-PII columns
assert df_test.select("id").collect() == encrypted_df_test.select("id").collect()

# Save encryption key to JSON output
encryption_key = {
    "key": base64.b64encode(key).decode('utf-8'),
    "iv": base64.b64encode(iv).decode('utf-8')
}
current_datetime = datetime.utcnow().strftime("%Y%m%d%H%M%S")
json_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_test_{current_datetime}.json"

# Function to simulate JSON file write for testing
def simulate_json_write(path, data):
    # Simulating write in comments to review file path and data
    print(f"Writing JSON to path: {path}")
    print(json.dumps(data, indent=4))

simulate_json_write(json_path, encryption_key)

# Stream test, create streaming DataFrame simulation for tests
streaming_data_test = [
    (1, "Alice", "alice@example.com", "9876543210", "60606"),
    (2, None, "none@domain.com", None, None),
    (3, "Robert Doe-Smith", "rob.doe-smith@example.com", "+44 1234 567890", "20220")
]
streaming_df_test = spark.createDataFrame(streaming_data_test, schema=schema_test)

# Define mock streaming dataframe
mock_streaming_df = streaming_df_test.withColumn("encrypted_name", encrypt_udf(col("name")))\
    .withColumn("encrypted_email", encrypt_udf(col("email")))\
    .withColumn("encrypted_phone", encrypt_udf(col("phone")))\
    .withColumn("encrypted_zip", encrypt_udf(col("zip")))

# Simulate stream processing with assert statements
assert mock_streaming_df.filter(col("id") == 3).select("encrypted_name").head()[0] is not None

# Delta Lake operations test for NULL handling and encryption columns existence
# This section requires mocking Delta Lake specific writing and reading confirmation
# Write data to delta
mock_streaming_df.write.format("delta").mode("overwrite").save("/tmp/delta/mock_customer_360")

# Check if written data is correct
delta_test_read = spark.read.format("delta").load("/tmp/delta/mock_customer_360").filter("id = 1")

# Run assertions to validate the delta table
assert delta_test_read.filter(col("id") == 1).select("encrypted_name").head()[0] is not None

# End of PySpark test case simulation
spark.stop()
