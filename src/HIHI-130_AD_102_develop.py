from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType
from datetime import datetime
from Crypto.Cipher import AES
import base64
import json
import os

# Encryption utility function using AES
def encrypt_pii(text):
    if text is None:
        return None
    cipher = AES.new(b'Sixteen byte key', AES.MODE_CFB, b'Sixteen byte IV__')
    enc_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(enc_bytes).decode('utf-8')

# UDF for column encryption
encrypt_udf = F.udf(encrypt_pii, StringType())

# Drop the table if exists and create a clone from customer_360_raw
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
spark.sql("CREATE TABLE purgo_playground.customer_360_raw_clone AS SELECT * FROM purgo_playground.customer_360_raw")

# Read data from replica table
df_clone = spark.table("purgo_playground.customer_360_raw_clone")

# Apply encryption to specified columns
df_encrypted = df_clone.withColumn("name", encrypt_udf(col("name")))\
                       .withColumn("email", encrypt_udf(col("email")))\
                       .withColumn("phone", encrypt_udf(col("phone")))\
                       .withColumn("zip", encrypt_udf(col("zip")))

# Write back to Delta Lake
df_encrypted.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate encryption key metadata
encryption_metadata = {
    "key": base64.b64encode(b'Sixteen byte key').decode('utf-8'),
    "iv": base64.b64encode(b'Sixteen byte IV__').decode('utf-8'),
    "generated_at": datetime.utcnow().isoformat()
}

# Define JSON file path and save the encryption key
current_time = datetime.now().strftime("%Y%m%d%H%M%S")
json_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_time}.json"

# Ensure the directory exists
os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

# Write encryption key metadata to JSON file
with open(json_file_path, 'w') as json_file:
    json.dump(encryption_metadata, json_file)

