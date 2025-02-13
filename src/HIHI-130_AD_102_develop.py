# Required Libraries
from pyspark.sql.functions import col, udf, current_timestamp, lit
from pyspark.sql.types import StringType, BinaryType
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import json

# Define Encryption Utility
def encrypt(value, key):
    if value is None:
        return None
    cipher = AES.new(key, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(value.encode("utf-8"))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode("utf-8")

# Generate Encryption Key
encryption_key = get_random_bytes(32)  # AES-256 requires a 32-byte key
encryption_key_encoded = base64.b64encode(encryption_key).decode("utf-8")

# Save Encryption Key as JSON File
encryption_key_filename = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_timestamp().cast('string').substr(1, 19).replace(':', '').replace(' ', '_')}.json"
with open(encryption_key_filename, "w") as outfile:
    json.dump({"encryption_key": encryption_key_encoded}, outfile)

# Create UDF for Encryption
encrypt_udf = udf(lambda value: encrypt(value, encryption_key), StringType())

# Drop the existing clone table if exists and replicate customer_360_raw
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
spark.sql("""
    CREATE TABLE purgo_playground.customer_360_raw_clone
    AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Read the data from the clone table
df_clone = spark.table("purgo_playground.customer_360_raw_clone")

# Apply encryption to PII columns
df_encrypted = df_clone \
    .withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Write encrypted data back to the Delta Lake
df_encrypted.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Optimize the Delta Table and Apply Z-Ordering
spark.sql("OPTIMIZE purgo_playground.customer_360_raw_clone ZORDER BY (id)")

# Vacuum the Delta Table
spark.sql("VACUUM purgo_playground.customer_360_raw_clone RETAIN 0 HOURS")

# Log Encryption Completion
print("Encryption process completed successfully.")

