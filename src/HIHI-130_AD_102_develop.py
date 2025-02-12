# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json
import datetime
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIData").getOrCreate()

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of the original table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS
SELECT * FROM purgo_playground.customer_360_raw
""")

# Generate a random AES-256 encryption key
encryption_key = get_random_bytes(32)  # 256 bits

# Save the encryption key as a JSON file
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"
with open(key_file_path, 'w') as key_file:
    json.dump({"encryption_key": base64.b64encode(encryption_key).decode('utf-8')}, key_file)

# Define encryption function
def encrypt_data(data, key):
    if data is None:
        return None
    cipher = AES.new(key, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(data.encode('utf-8'))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode('utf-8')

# Register UDF for encryption
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Load the clone table
df_clone = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
df_encrypted = df_clone.withColumn("name", encrypt_udf(col("name"))) \
                       .withColumn("email", encrypt_udf(col("email"))) \
                       .withColumn("phone", encrypt_udf(col("phone"))) \
                       .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
df_encrypted.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.customer_360_raw_clone")

# Cleanup: Drop the clone table after the process
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
