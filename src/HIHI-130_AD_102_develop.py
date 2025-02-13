from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from datetime import datetime
import json
from Crypto.Cipher import AES
import base64

# Setup Spark session
spark = SparkSession.builder.appName("Test Encrypt PII Data in Databricks").getOrCreate()

# Test Libraries Installation
# Install necessary packages if not available
# Note: This might require restarting the cluster or running once outside the notebook
# %pip install pycrypto

# Define test schema for customer_360_raw_clone table
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True)
])

# Test Data: Sample valid, edge, and error case data
data = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "10101"),
    (2, None, None, None, "10101"),
    (3, "Jürgen Müller", "jürgen.müller@example.de", "+491234567890", "10115")
]

# Create DataFrame with test data
df = spark.createDataFrame(data, schema)

# Testing encryption logic
# Define encryption key and initialization vector
key = b'Sixteen byte key'
iv = b'Sixteen byte IV__'

# Encryption function
def encrypt(text):
    if text is None:
        return None
    cipher = AES.new(key, AES.MODE_CFB, iv)
    encrypted_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(encrypted_bytes).decode('utf-8')

# Register UDF for encryption
encrypt_udf = udf(encrypt, StringType())

# Test encryption of PII columns
test_encrypted_df = df.withColumn("name_encrypted", encrypt_udf(col("name")))\
                      .withColumn("email_encrypted", encrypt_udf(col("email")))\
                      .withColumn("phone_encrypted", encrypt_udf(col("phone")))\
                      .withColumn("zip_encrypted", encrypt_udf(col("zip")))

# Schema validation test: Ensure the schema is as expected after encryption
assert test_encrypted_df.schema == StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("name_encrypted", StringType(), True),
    StructField("email_encrypted", StringType(), True),
    StructField("phone_encrypted", StringType(), True),
    StructField("zip_encrypted", StringType(), True)
]), "Schema does not match expected schema"

# Unit test: Check data type conversions and NULL handling
test_result = test_encrypted_df.filter(col("id") == 2).select("name_encrypted").collect()
assert test_result[0].name_encrypted is None, "NULL value encryption failed"

# Delta Lake operation test: Write encrypted test DataFrame to Delta table
test_table_name = "purgo_playground.test_customer_360_raw_clone"
test_encrypted_df.write.format("delta").mode("overwrite").saveAsTable(test_table_name)

# Data quality validation: Ensure encrypted values exist and are different from original
test_values = test_encrypted_df.filter(col("id") == 1).select("name", "name_encrypted").collect()
assert test_values[0].name != test_values[0].name_encrypted, "Encryption test failed for value change"

# Cleanup: Drop test table
spark.sql(f"DROP TABLE IF EXISTS {test_table_name}")

# Test saving encryption key to JSON
test_encryption_key = {
    "key": base64.b64encode(key).decode('utf-8'),
    "iv": base64.b64encode(iv).decode('utf-8')
}
current_datetime = datetime.utcnow().strftime("%Y%m%d%H%M%S")
test_json_path = "/Volumes/agilisium_playground/purgo_playground/de_dq/test_encryption_key_"+current_datetime+".json"

# Save test key
with open(test_json_path, 'w') as json_file:
    json.dump(test_encryption_key, json_file)

# Assert JSON file creation
assert os.path.exists(test_json_path), "JSON key file was not created"

spark.stop()
