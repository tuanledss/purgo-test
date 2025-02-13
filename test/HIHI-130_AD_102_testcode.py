# Import necessary libraries and setup configurations
# Assuming AES encryption with PyCrypto for demonstration
# Install required library
# pip install pycryptodome

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType
from datetime import datetime
from Crypto.Cipher import AES
import base64
import json

# Initialize Spark session for testing
spark = SparkSession.builder.appName("Test PII Encryption").getOrCreate()

# Define a test schema matching customer_360_raw
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

# Sample test data including edge and error cases
test_data = [
    (1, "Alice Smith", "alice@example.com", "5551234567", "Tech Corp", "Manager", "123 Main St", "Springfield", "IL", "62701", "USA", "IT", "John Doe", "2022-05-20", "2023-09-01", "item1,item2", ""),
    (2, None, None, None, "Empty Corp", "CEO", "456 Elm St", "Bubbleton", "CA", "90210", "USA", "Finance", "Jane Doe", "2022-04-01", "2023-08-30", "item3", ""),
    (3, "Ümit Özdağ", "üm@example.de", "+49123456789", "Müller GmbH", "Engineer", "Franzstrasse", "Berlin", "BE", "10115", "DE", "Mechanical", "Hans Zimmer", "2022-06-15", "2023-10-10", "item4,item5", "Note")
]

# Create DataFrame
df = spark.createDataFrame(test_data, schema)

# Define encryption key and initialization vector
key = b'Sixteen byte key'
iv = b'Sixteen byte IV__'

# Encryption function using AES
def encrypt_value(value):
    if value is None:
        return None
    cipher = AES.new(key, AES.MODE_CFB, iv)
    ciphertext = cipher.encrypt(value.encode('utf-8'))
    return base64.b64encode(ciphertext).decode('utf-8')

# Register UDF
encrypt_udf = udf(encrypt_value, StringType())

# Apply encryption to PII columns
encrypted_df = df.withColumn("name_enc", encrypt_udf(col("name")))\
                 .withColumn("email_enc", encrypt_udf(col("email")))\
                 .withColumn("phone_enc", encrypt_udf(col("phone")))\
                 .withColumn("zip_enc", encrypt_udf(col("zip")))

# Drop original PII columns and rename encrypted columns
final_df = encrypted_df.drop("name", "email", "phone", "zip")\
                       .withColumnRenamed("name_enc", "name")\
                       .withColumnRenamed("email_enc", "email")\
                       .withColumnRenamed("phone_enc", "phone")\
                       .withColumnRenamed("zip_enc", "zip")

# Schema validation test
assert final_df.schema == df.schema, "Schema mismatch after encryption"

# Save encrypted data to Delta table
final_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key to JSON file
encryption_key_data = {
    "key": base64.b64encode(key).decode('utf-8'),
    "iv": base64.b64encode(iv).decode('utf-8')
}
timestamp_str = datetime.utcnow().strftime("%Y%m%d%H%M%S")
json_filepath = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{timestamp_str}.json"

# Write encryption key to the specified JSON path
with open(json_filepath, 'w') as json_file:
    json.dump(encryption_key_data, json_file)

# Clean up Spark session
spark.stop()
