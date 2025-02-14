from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
import base64
import json
from datetime import datetime
from cryptography.fernet import Fernet

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIData").getOrCreate()

# Generate encryption key
encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key)

# Define UDF for encryption
def encrypt_value(value):
    if value is not None:
        return cipher_suite.encrypt(value.encode()).decode()
    return None

encrypt_udf = udf(encrypt_value, StringType())

# Load the customer_360_raw table
customer_360_raw_df = spark.table("purgo_playground.customer_360_raw")

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Create a replica of the customer_360_raw table
customer_360_raw_df.write.saveAsTable("purgo_playground.customer_360_raw_clone12")

# Load the clone table
customer_360_raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone12")

# Encrypt PII columns
encrypted_df = customer_360_raw_clone_df.withColumn("name", encrypt_udf(col("name"))) \
                                        .withColumn("email", encrypt_udf(col("email"))) \
                                        .withColumn("phone", encrypt_udf(col("phone"))) \
                                        .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Save the encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
encryption_key_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_datetime}.json"

with open(encryption_key_path, 'w') as key_file:
    json.dump({"encryption_key": base64.b64encode(encryption_key).decode()}, key_file)

# Stop the Spark session
spark.stop()