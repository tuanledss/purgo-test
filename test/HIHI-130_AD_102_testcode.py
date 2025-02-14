# Databricks PySpark script to encrypt PII columns and save the encryption key

# Library installation (if necessary)
# %pip install simple-crypt

import os
import json
from datetime import datetime

from simplecrypt import encrypt, decrypt  # Replace with your chosen encryption library
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

# Configuration (replace with your actual values)
# Encryption Algorithm: AES-256 (using simple-crypt as an example)
# Key Management System: Azure Key Vault (example usage shown below - adapt as needed)
# Source Table: purgo_playground.customer_360_raw_clone
# Destination Table: purgo_playground.customer_360_raw_clone (overwriting for this example)
# JSON Location: /Volumes/agilisium_playground/purgo_playground/de_dq12
PII_COLUMNS = ["name", "email", "phone", "zip"]
KEY_VAULT_NAME = "your-keyvault-name" # Replace with actual Key Vault Name if using Azure Key Vault
KEY_NAME = "your-key-name"  # Replace with actual key name if using Azure Key Vault
SECRET_NAME = "your_secret_name"


# Key generation (using simple-crypt, replace with your key management strategy)
#  Preferably use a secure key management system like Azure Key Vault
encryption_key = os.urandom(32) # Generate a 32-byte key for AES-256


try:
    #  If using Azure Key Vault for key management
    #  Retrieve the Key Vault secrets
    #
    #  This is a placeholder - Replace with your actual Key Vault integration
    #  using Azure Key Vault libraries or appropriate secrets management tools
    #
    # credentials = dbutils.secrets.get(scope = "purgo-secret-scope", key = "purgo-vault-secret")
    # print(credentials)

    #  ... (code to securely retrieve encryption key from Azure Key Vault) ...

    # Encryption function
    def encrypt_data(data, key):
        return encrypt(key, data).decode('latin1') # Simple-crypt returns bytes, decode to string

    # Decryption function (for testing)
    def decrypt_data(encrypted_data, key):
        encrypted_bytes = encrypted_data.encode('latin1')  # Simple-crypt returns bytes, decode to string
        return decrypt(key, encrypted_bytes).decode()



    spark = SparkSession.builder.appName("EncryptPII").getOrCreate()
    # Load data from source table
    source_table_name = "purgo_playground.customer_360_raw" # Source Table Name
    df = spark.table(source_table_name)

    # Drop the clone table if exists
    spark.sql(f"DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
    # Encrypt PII columns
    for column in PII_COLUMNS:
        df = df.withColumn(column, col(column).cast(StringType()))  # Cast to string if necessary
        df = df.withColumn(column, lit(encrypt_data(df[column].getItem(0), encryption_key)) if df[column].getItem(0) is not None else lit(None)) # Use a udf here


    # Save the encrypted data to the destination table (overwrite mode)
    destination_table_name = "purgo_playground.customer_360_raw_clone" # Destination Table Name
    df.write.format("delta").mode("overwrite").saveAsTable(destination_table_name)


    # Save the encryption key as a JSON file (REPLACE WITH SECURE KEY MANAGEMENT)
    key_file_path = "/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".json"
    # IMPORTANT: In a production environment, DO NOT store the key directly as plaintext.
    # Use a secure key management system like Azure Key Vault.
    key_data = {"encryption_key": encryption_key.decode('latin1')} # Store key as string in JSON

    with open(key_file_path, "w") as f:
        json.dump(key_data, f)

    # Test decryption (optional, for verification)
    decrypted_df = df
    for column in PII_COLUMNS:
        decrypted_df = decrypted_df.withColumn(column, lit(decrypt_data(df[column].getItem(0), encryption_key)) if df[column].getItem(0) is not None else lit(None))

    decrypted_df.show()


# Unit test example
    test_value = data_happy_path[0][1]  # 'John Doe' from the test data
    encrypted_value = encrypt_data(test_value, encryption_key)
    decrypted_value = decrypt_data(encrypted_value, encryption_key)
    assert decrypted_value == test_value, f"Decryption failed: Expected {test_value}, got {decrypted_value}"


except Exception as e:
    print(f"An error occurred: {e}")


finally:
    spark.stop()


