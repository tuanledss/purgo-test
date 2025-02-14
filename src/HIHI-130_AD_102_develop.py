from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, expr
from pyspark.sql.types import StringType, StructType, StructField, LongType, DoubleType, ArrayType, MapType, TimestampType
import datetime
import json
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import unittest

# Initialize Spark session
spark = SparkSession.builder.appName("TestEncryption").getOrCreate()

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
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Encrypt PII columns
df_encrypted = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Save encrypted data to a new table
df_encrypted.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Unit Test Class
class TestEncryption(unittest.TestCase):

    def test_schema_validation(self):
        expected_schema = StructType([
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
        self.assertEqual(df_encrypted.schema, expected_schema)

    def test_data_type_conversion(self):
        self.assertTrue(all(isinstance(row['name'], str) for row in df_encrypted.collect()))
        self.assertTrue(all(isinstance(row['email'], str) for row in df_encrypted.collect()))
        self.assertTrue(all(isinstance(row['phone'], str) for row in df_encrypted.collect()))
        self.assertTrue(all(isinstance(row['zip'], str) for row in df_encrypted.collect()))

    def test_null_handling(self):
        null_row = df_encrypted.filter(col("id") == 3).collect()[0]
        self.assertIsNone(null_row['name'])
        self.assertIsNone(null_row['email'])
        self.assertIsNone(null_row['phone'])
        self.assertIsNone(null_row['zip'])

    def test_encryption(self):
        encrypted_row = df_encrypted.filter(col("id") == 1).collect()[0]
        self.assertNotEqual(encrypted_row['name'], "John Doe")
        self.assertNotEqual(encrypted_row['email'], "john.doe@example.com")
        self.assertNotEqual(encrypted_row['phone'], "1234567890")
        self.assertNotEqual(encrypted_row['zip'], "12345")

    def test_encryption_key_saved(self):
        with open(key_file_path, 'r') as key_file:
            key_data = json.load(key_file)
        self.assertIn("encryption_key", key_data)

    def test_performance(self):
        import time
        start_time = time.time()
        df_encrypted.collect()
        end_time = time.time()
        self.assertLess(end_time - start_time, 300)  # 5 minutes

# Run the tests
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

