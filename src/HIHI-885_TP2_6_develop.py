from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json
import datetime
from cryptography.fernet import Fernet

# Define encryption function
def encrypt_data(data, key):
    if data is None:
        return None
    fernet = Fernet(key)
    return fernet.encrypt(data.encode()).decode()

# Generate encryption key
encryption_key = Fernet.generate_key()

# Save encryption key to JSON file
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_datetime}.json"
with open(key_file_path, 'w') as key_file:
    json.dump({"encryption_key": encryption_key.decode()}, key_file)

# Register UDF for encryption
encrypt_udf = udf(lambda x: encrypt_data(x, encryption_key), StringType())

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Create a replica of the original table
spark.sql("""
    CREATE TABLE purgo_playground.customer_360_raw_clone12 AS
    SELECT * FROM purgo_playground.customer_360_raw12
""")

# Load data from the clone table
df = spark.table("purgo_playground.customer_360_raw_clone12")

# Encrypt PII columns
encrypted_df = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Save the encrypted data back to the clone table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone12")

# Output the path of the encryption key file
print(f"Encryption key saved to: {key_file_path}")

# Test: Validate schema of the encrypted table
expected_schema = ["id", "name", "email", "phone", "company", "job_title", "address", "city", "state", "country", "industry", "account_manager", "creation_date", "last_interaction_date", "purchase_history", "notes", "zip"]
actual_schema = [field.name for field in encrypted_df.schema.fields]
assert expected_schema == actual_schema, "Schema validation failed"

# Test: Validate data type conversions
assert encrypted_df.schema["name"].dataType == StringType(), "Data type conversion failed for 'name'"
assert encrypted_df.schema["email"].dataType == StringType(), "Data type conversion failed for 'email'"
assert encrypted_df.schema["phone"].dataType == StringType(), "Data type conversion failed for 'phone'"
assert encrypted_df.schema["zip"].dataType == StringType(), "Data type conversion failed for 'zip'"

# Test: Validate NULL handling
null_test_df = df.withColumn("name", lit(None).cast(StringType()))
encrypted_null_test_df = null_test_df.withColumn("name", encrypt_udf(col("name")))
assert encrypted_null_test_df.filter(col("name").isNull()).count() == null_test_df.filter(col("name").isNull()).count(), "NULL handling test failed"

# Test: Cleanup operations
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")
dbutils.fs.rm(key_file_path)
