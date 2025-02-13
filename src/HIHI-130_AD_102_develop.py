from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, base64, from_json
from pyspark.sql.types import StringType, StructType, StructField, MapType
from pyspark.sql.streaming import DataStreamWriter
from pyspark.testing.sqlutils import assertDataFrameEqual
import json
import datetime
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Initialize Spark session
spark = SparkSession.builder.appName("DatabricksTest").getOrCreate()

# Function to create a random AES-256 encryption key
def generate_key():
    return get_random_bytes(32)  # 256 bits

# Function to encrypt data
def encrypt_data(data, key):
    if data is None:
        return None
    cipher = AES.new(key, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(data.encode('utf-8'))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode('utf-8')

# Setup: Drop and create table
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS
SELECT *, 0 AS is_churn FROM purgo_playground.customer_360_raw
""")

# Encrypt function as UDF
encryption_key = generate_key()
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Load DataFrame
df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
df_encrypted = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Write encrypted data back
df_encrypted.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"
with open(key_file_path, 'w') as key_file:
    json.dump({"encryption_key": base64.b64encode(encryption_key).decode('utf-8')}, key_file)

# Test data validation
def validate_data_encryption():
    orig_df = spark.table("purgo_playground.customer_360_raw")
    encrypted_df = spark.table("purgo_playground.customer_360_raw_clone")

    # Ensure schema matches
    assert orig_df.schema == encrypted_df.schema, "Schema mismatch!"

    # Test for NULL handling
    encrypted_nulls = encrypted_df.filter(col("name").isNull() | col("email").isNull() | col("phone").isNull() | col("zip").isNull()).count()
    orig_nulls = orig_df.filter(col("name").isNull() | col("email").isNull() | col("phone").isNull() | col("zip").isNull()).count()
    assert encrypted_nulls == orig_nulls, "NULL handling failed!"

    # Test data integrity by decrypting and matching
    for row in orig_df.collect():
        enc_row = encrypted_df.filter(col("id") == row.id).collect()[0]
        decrypted_name = decrypt_data(enc_row.name, encryption_key)
        decrypted_email = decrypt_data(enc_row.email, encryption_key)
        decrypted_phone = decrypt_data(enc_row.phone, encryption_key)
        decrypted_zip = decrypt_data(enc_row.zip, encryption_key)
        assert row.name == decrypted_name and row.email == decrypted_email and \
               row.phone == decrypted_phone and row.zip == decrypted_zip, "Data integrity failed!"

# Define decrypt_data function for validation
def decrypt_data(encrypted_data, key):
    if encrypted_data is None:
        return None
    enc_data_bytes = base64.b64decode(encrypted_data)
    nonce, tag, ciphertext = enc_data_bytes[:16], enc_data_bytes[16:32], enc_data_bytes[32:]
    cipher = AES.new(key, AES.MODE_EAX, nonce=nonce)
    return cipher.decrypt_and_verify(ciphertext, tag).decode('utf-8')

# Validate the encryption process
validate_data_encryption()

print("All tests passed!")

