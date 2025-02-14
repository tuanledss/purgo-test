from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import datetime
import json
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIData").getOrCreate()

# Define encryption function
def encrypt_data(data, key):
    if data is None:
        return None
    cipher = AES.new(key, AES.MODE_EAX)
    nonce = cipher.nonce
    ciphertext, tag = cipher.encrypt_and_digest(data.encode('utf-8'))
    return base64.b64encode(nonce + ciphertext).decode('utf-8')

# Generate encryption key
encryption_key = get_random_bytes(32)  # AES-256 key
encryption_key_b64 = base64.b64encode(encryption_key).decode('utf-8')

# Save encryption key to JSON
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_datetime}.json"
with open(key_file_path, 'w') as key_file:
    json.dump({"encryption_key": encryption_key_b64}, key_file)

# Register UDF for encryption
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Create a replica of the original table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone12
AS SELECT * FROM purgo_playground.customer_360_raw12
""")

# Load the data from the replica table
df = spark.table("purgo_playground.customer_360_raw_clone12")

# Encrypt PII columns
df_encrypted = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Save encrypted data back to the table
df_encrypted.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Optimize and vacuum the table for performance
spark.sql("OPTIMIZE purgo_playground.customer_360_raw_clone12 ZORDER BY (id)")
spark.sql("VACUUM purgo_playground.customer_360_raw_clone12 RETAIN 0 HOURS")

