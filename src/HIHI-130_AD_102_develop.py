from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json
import datetime
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import base64

# Initialize Spark session
spark = SparkSession.builder.appName("Encrypt PII Data").getOrCreate()

# Drop the cloned table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of the source table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone 
AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Define the encryption function
def encrypt_data(data, key):
    if data is None:
        return None
    cipher = AES.new(key, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(data.encode('utf-8'))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode('utf-8')

# Generate encryption key
encryption_key = get_random_bytes(32)

# Save the encryption key as a JSON file
key_dict = {"key": base64.b64encode(encryption_key).decode('utf-8')}
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"
dbutils.fs.put(key_file_path, json.dumps(key_dict))

# Create a UDF for encrypting data
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Load data from the clone table and encrypt PII columns
df = spark.table("purgo_playground.customer_360_raw_clone")
df_encrypted = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Write encrypted data back to Delta table
df_encrypted.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("purgo_playground.customer_360_raw_clone")

# Optimize the table and apply Z-ordering
spark.sql("""
OPTIMIZE purgo_playground.customer_360_raw_clone
ZORDER BY (id)
""")

# Vacuum old data files after data is no longer needed
spark.sql("VACUUM purgo_playground.customer_360_raw_clone RETAIN 168 HOURS")

