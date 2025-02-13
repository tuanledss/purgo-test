from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import col, lit, udf, from_json, struct, merge, array, map_concat, expr, explode, when
from pyspark.sql.streaming import DataStreamWriter
import json
import datetime
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import unittest


# Initialize Spark session
spark = SparkSession.builder.appName("DatabricksSQLTest").getOrCreate()

# Recreate "customer_360_raw_clone" if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS
SELECT * FROM purgo_playground.customer_360_raw
""")

# Define schema for encrypted DataFrame
schema = T.StructType([
    T.StructField("id", T.LongType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("phone", T.StringType(), True),
    T.StructField("company", T.StringType(), True),
    T.StructField("job_title", T.StringType(), True),
    T.StructField("address", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
    T.StructField("zip", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("industry", T.StringType(), True),
    T.StructField("account_manager", T.StringType(), True),
    T.StructField("creation_date", T.DateType(), True),
    T.StructField("last_interaction_date", T.DateType(), True),
    T.StructField("purchase_history", T.StringType(), True),
    T.StructField("notes", T.StringType(), True),
    T.StructField("is_churn", T.IntegerType(), True)
])

# Generate a random AES-256 encryption key
encryption_key = get_random_bytes(32)  # 256 bits

# Save the encryption key as a JSON file
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{{current_datetime}}.json"
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
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), T.StringType())

# Load the clone table
df_clone = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
df_encrypted = df_clone.withColumn("name", encrypt_udf(col("name"))) \
                       .withColumn("email", encrypt_udf(col("email"))) \
                       .withColumn("phone", encrypt_udf(col("phone"))) \
                       .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
df_encrypted.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.customer_360_raw_clone")

# Define a test class for PySpark
class TestDataEncryption(unittest.TestCase):

    def setUp(self):
        # Set up any configuration before each test
        self.spark = spark
        self.encryption_key = encryption_key

    def test_schema(self):
        # Test schema validation
        expected_schema = schema
        actual_schema = df_encrypted.schema
        self.assertEqual(expected_schema, actual_schema, "Schema does not match!")

    def test_data_type_conversion(self):
        # Test data type conversion on encrypted columns
        df_test = df_encrypted.select("name", "email", "phone", "zip")
        self.assertTrue(all(field.dataType == T.StringType() for field in df_test.schema.fields),
                        "Data types of encrypted columns should be STRING!")

    def test_null_handling(self):
        # Test NULL handling in encrypted columns
        df_test = df_clone.select("name", "email", "phone", "zip")
        null_counts = df_test.where(col("name").isNull() | col("email").isNull() | col("phone").isNull() | col("zip").isNull()).count()
        self.assertEqual(null_counts, 0, "NULL values found in encrypted PII columns!")

    def test_encryption_validity(self):
        # Test for encryption validity by checking if encryptions are non-deterministic
        duplicate_records = df_encrypted.groupBy("name", "email", "phone", "zip").count().filter("count > 1").count()
        self.assertEqual(duplicate_records, 0, "Encryption seems to be deterministic; found duplicate encrypted records!")

    def test_delta_operations(self):
        # Example Delta Lake operation validation (read and check the table exists)
        try:
            delta_df = self.spark.read.format("delta").table("purgo_playground.customer_360_raw_clone")
            self.assertIsNotNone(delta_df, "Delta table not found or cannot be read!")
        except Exception as e:
            self.fail(f"Delta table read operation failed: {str(e)}")

    def tearDown(self):
        # Clean up actions after each test
        # Optionally drop the table if needed
        self.spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Run the tests
if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

