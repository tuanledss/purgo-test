# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime
from Crypto.Cipher import AES
import base64
import json

# Encryption utility function using AES
def encrypt_pii(text):
    if text is None:
        return None
    cipher = AES.new(b'Sixteen byte key', AES.MODE_CFB, b'Sixteen byte IV__')
    enc_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(enc_bytes).decode('utf-8')

# UDF for column encryption
encrypt_udf = F.udf(encrypt_pii, StringType())

# Drop and recreate the clone table
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
spark.sql("CREATE TABLE purgo_playground.customer_360_raw_clone AS SELECT * FROM purgo_playground.customer_360_raw")

# Load customer_360_raw_clone table
raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone")

# Apply encryption to PII columns
encrypted_df = raw_clone_df.withColumn("name", encrypt_udf(F.col("name")))\
                           .withColumn("email", encrypt_udf(F.col("email")))\
                           .withColumn("phone", encrypt_udf(F.col("phone")))\
                           .withColumn("zip", encrypt_udf(F.col("zip")))

# Write encrypted data back to clone table using Delta format
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Saving the encryption key and IV to a JSON file
encryption_metadata = {
    "key": base64.b64encode(b'Sixteen byte key').decode('utf-8'),
    "iv": base64.b64encode(b'Sixteen byte IV__').decode('utf-8')
}
current_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
json_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_time}.json"

# Save key to a JSON file
dbutils.fs.put(json_file_path, json.dumps(encryption_metadata))

# Add table optimization steps
spark.sql("OPTIMIZE purgo_playground.customer_360_raw_clone")

# Vacuum old files to save storage
spark.sql("VACUUM purgo_playground.customer_360_raw_clone RETAIN 168 HOURS")
