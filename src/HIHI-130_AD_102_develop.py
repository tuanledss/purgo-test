# Import necessary libraries for PySpark and cryptographic functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from datetime import datetime
import base64
from Crypto.Cipher import AES

# Setup Spark session
spark = SparkSession.builder.appName("Encrypt PII Data Test").getOrCreate()

# Define UDF for encryption
def encrypt(text):
    if text is None:
        return text
    cipher = AES.new(key, AES.MODE_CFB, iv)
    ct_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(ct_bytes).decode('utf-8')

encrypt_udf = udf(encrypt, StringType())

# Sample data for testing
test_data = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "10101"),
    (2, None, None, None, None),
    (3, "Jürgen Müller", "jürgen.müller@example.com", "+491234567890", "10115")
]

# Define the schema
schema = ["id", "name", "email", "phone", "zip"]

# Create DataFrame from sample data
df = spark.createDataFrame(test_data, schema)

# Define keys for encryption
key = b'Sixteen byte key'
iv = b'Sixteen byte IV__'

# Encrypt specified PII columns
encrypted_df = (df
    .withColumn("encrypted_name", encrypt_udf(col("name")))
    .withColumn("encrypted_email", encrypt_udf(col("email")))
    .withColumn("encrypted_phone", encrypt_udf(col("phone")))
    .withColumn("encrypted_zip", encrypt_udf(col("zip"))))

# Drop original columns and rename encrypted columns to original names
final_df = (encrypted_df
    .drop("name", "email", "phone", "zip")
    .withColumnRenamed("encrypted_name", "name")
    .withColumnRenamed("encrypted_email", "email")
    .withColumnRenamed("encrypted_phone", "phone")
    .withColumnRenamed("encrypted_zip", "zip"))

# Write the encrypted DataFrame to a Delta table
final_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
encryption_key = {"key": base64.b64encode(key).decode('utf-8'), "iv": base64.b64encode(iv).decode('utf-8')}
json_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Save encryption key information
with open(json_path, 'w') as json_file:
    json.dump(encryption_key, json_file)

# Stop Spark session
spark.stop()
