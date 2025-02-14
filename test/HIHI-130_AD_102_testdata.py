from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType
import json
from datetime import datetime
from cryptography.fernet import Fernet

# Create Spark session
spark = SparkSession.builder \
    .appName("Databricks PII Encryption") \
    .getOrCreate()

# Drop the clone table if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone12")

# Create a replica of the original table
spark.sql("CREATE TABLE purgo_playground.customer_360_raw_clone12 AS SELECT * FROM purgo_playground.customer_360_raw")

# Key generation and encryption logic
encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

def encrypt(data):
    if data is None:
        return None
    return cipher.encrypt(data.encode()).decode()

encrypt_udf = udf(encrypt, StringType())

# Encrypt specified columns
columns_to_encrypt = ["name", "email", "phone", "zip"]

df = spark.table("purgo_playground.customer_360_raw_clone12")
for column in columns_to_encrypt:
    df = df.withColumn(column, encrypt_udf(col(column)))

# Write back the encrypted data
df.write.mode('overwrite').saveAsTable("purgo_playground.customer_360_raw_clone12")

# Save the encryption key
encryption_key_data = {"key": encryption_key.decode()}
current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
encryption_key_file = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{current_datetime}.json"

with open(encryption_key_file, 'w') as f:
    json.dump(encryption_key_data, f)

# Generate comprehensive test data for the replica table
test_data = [
    (1, "Alice", "alice@example.com", "+123456789", "12345", "ACME Corp", "Engineer", "123 St", "CityX", "StateX", "CountryX", "Tech", "John Doe", "2024-01-01", "2024-01-15", "Purchase1", "Note1", "12345", 0),
    # Happy path, valid scenario
    (2, "Bob", "bob@example.com", "+987654321", "54321", "Globex", "Manager", "456 Av", "CityY", "StateY", "CountryY", "Finance", "Jane Smith", "2024-02-01", "2024-02-20", "Purchase2", "Note2", "54321", 1),
    # Edge case, long name
    (3, "Carolyn Elizabeth Johnson-Smith", "carol@example.com", "+198765432", "67890", "Initech", "Researcher", "789 Blvd", "CityZ", "StateZ", "CountryZ", "Health", "Richard Roe", "2024-03-01", "2024-03-30", "Purchase3", "Note3", "67890", 0),
    # Error case, invalid phone number
    (4, "Dave", "dave@example.com", "INVALID_PHONE", "98765", "Hooli", "Director", "101 Rd", "CityW", "StateW", "CountryW", "Education", "Mary Major", "2024-04-01", "2024-04-25", "Purchase4", "Note4", "98765", 1),
    # NULL handling, missing email
    (5, "Eve", None, "+1122334455", "11223", "Massive Dynamic", "Intern", "202 Ln", "CityV", "StateV", "CountryV", "Retail", "Ellen Ripley", "2024-05-01", "2024-05-15", "Purchase5", "Note5", "11223", 0),
    # Special characters and multi-byte characters in name
    (6, "Chírø Nishikáðó", "hiro@example.com", "+5566778899", "99887", "Wonka Industries", "Consultant", "303 St", "CityU", "StateU", "CountryU", "Travel", "Will Riker", "2024-06-01", "2024-06-10", "Purchase6", "Note6", "99887", 1)
]

test_df = spark.createDataFrame(test_data, schema=df.schema)
test_df.write.mode('append').saveAsTable("purgo_playground.customer_360_raw_clone12")
