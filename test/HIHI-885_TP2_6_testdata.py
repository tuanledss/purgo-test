from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
import json
import datetime
import os
from cryptography.fernet import Fernet

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIData").getOrCreate()

# Define encryption function
def encrypt_data(data, key):
    fernet = Fernet(key)
    return fernet.encrypt(data.encode()).decode()

# Generate encryption key
encryption_key = Fernet.generate_key()
fernet = Fernet(encryption_key)

# Save encryption key to JSON file
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_datetime}.json"
with open(key_file_path, 'w') as key_file:
    json.dump({"encryption_key": encryption_key.decode()}, key_file)

# Register UDF for encryption
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Create a replica of the original table
spark.sql("""
    CREATE TABLE purgo_playground.customer_360_raw_clone12 AS
    SELECT * FROM purgo_playground.customer_360_raw12
""")

# Load data from the clone table
df = spark.table("purgo_playground.customer_360_raw_clone12")

# Encrypt PII columns
encrypted_df = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Output the path of the encryption key file
print(f"Encryption key saved to: {key_file_path}")

