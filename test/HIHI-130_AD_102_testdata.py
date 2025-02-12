from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
import json
import datetime
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Initialize Spark session
spark = SparkSession.builder.appName("EncryptPIIData").getOrCreate()

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of the original table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS
SELECT * FROM purgo_playground.customer_360_raw
""")

# Generate a random AES-256 encryption key
encryption_key = get_random_bytes(32)  # 256 bits

# Save the encryption key as a JSON file
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"
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
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Load the clone table
df_clone = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
df_encrypted = df_clone.withColumn("name", encrypt_udf(col("name"))) \
                       .withColumn("email", encrypt_udf(col("email"))) \
                       .withColumn("phone", encrypt_udf(col("phone"))) \
                       .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
df_encrypted.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Test data generation for various scenarios
test_data = [
    # Happy path test data
    (1, "John Doe", "john.doe@example.com", "1234567890", "Company A", "Engineer", "123 Main St", "CityA", "StateA", "12345", "CountryA", "IndustryA", "ManagerA", "2023-01-01", "2023-02-01", "HistoryA", "NotesA", 0),
    # Edge cases
    (2, "Jane Smith", "jane.smith@example.com", "0987654321", "Company B", "Manager", "456 Elm St", "CityB", "StateB", "54321", "CountryB", "IndustryB", "ManagerB", "2023-01-02", "2023-02-02", "HistoryB", "NotesB", 1),
    # Error cases
    (3, None, "invalid_email", "0000000000", "Company C", "Director", "789 Oak St", "CityC", "StateC", "00000", "CountryC", "IndustryC", "ManagerC", "2023-01-03", "2023-02-03", "HistoryC", "NotesC", 0),
    # NULL handling scenarios
    (4, None, None, None, "Company D", "Analyst", "101 Pine St", "CityD", "StateD", None, "CountryD", "IndustryD", "ManagerD", "2023-01-04", "2023-02-04", "HistoryD", "NotesD", 1),
    # Special characters and multi-byte characters
    (5, "José Álvarez", "jose.álvarez@example.com", "+1-800-555-0199", "Company E", "Consultant", "202 Birch St", "CityE", "StateE", "67890", "CountryE", "IndustryE", "ManagerE", "2023-01-05", "2023-02-05", "HistoryE", "NotesE", 0)
]

# Create DataFrame with test data
schema = df_clone.schema
test_df = spark.createDataFrame(test_data, schema)

# Show test data
test_df.show(truncate=False)