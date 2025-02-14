from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, StructType, StructField, LongType, DoubleType, ArrayType, MapType, TimestampType
import datetime
import json
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define encryption function
def encrypt_data(data, key):
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

# Define schema for test data
schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

# Create test data
data = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "12345", "2024-03-21T00:00:00.000+0000", 100.50, ["tag1", "tag2"], {"key1": "value1"}),
    (2, "Jane Smith", "jane.smith@example.com", "0987654321", "54321", "2024-03-21T00:00:00.000+0000", 200.75, ["tag3"], {"key2": "value2"}),
    (3, None, None, None, None, "2024-03-21T00:00:00.000+0000", None, [], {}),
    (4, "Special Chars !@#$%^&*()", "special@example.com", "1112223333", "00000", "2024-03-21T00:00:00.000+0000", 300.00, ["tag4"], {"key3": "value3"}),
    (5, "Multi-byte 文字", "multibyte@example.com", "4445556666", "99999", "2024-03-21T00:00:00.000+0000", 400.25, ["tag5"], {"key4": "value4"}),
    # Add more records as needed for edge cases, error cases, etc.
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Encrypt PII columns
df_encrypted = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Show encrypted data
df_encrypted.show(truncate=False)

# Save encrypted data to a new table
df_encrypted.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

