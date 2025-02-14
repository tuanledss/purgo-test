from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp
from pyspark.sql.types import StringType
import json
import base64
import os

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIData").getOrCreate()

# Define encryption function
def encrypt_data(data, key):
    if data is not None:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    return None

# Register UDF
encrypt_udf = udf(lambda x: encrypt_data(x, "encryption_key"), StringType())

# Load the customer_360_raw table
customer_df = spark.table("purgo_playground.customer_360_raw")

# Encrypt PII columns
encrypted_df = customer_df.withColumn("name", encrypt_udf(col("name"))) \
                          .withColumn("email", encrypt_udf(col("email"))) \
                          .withColumn("phone", encrypt_udf(col("phone"))) \
                          .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data to the clone table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Generate encryption key and save as JSON
encryption_key = base64.b64encode(os.urandom(32)).decode('utf-8')
key_data = {"encryption_key": encryption_key, "timestamp": current_timestamp().cast("string").alias("timestamp")}
key_json = json.dumps(key_data)

# Save the encryption key to a JSON file
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_timestamp().cast('string')}.json"
with open(key_file_path, 'w') as key_file:
    key_file.write(key_json)

# Log the encryption process
print(f"Encryption process completed. Key saved at: {key_file_path}")

# Stop Spark session
spark.stop()