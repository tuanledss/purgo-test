# Databricks PySpark script to encrypt PII columns and save the encryption key

# Library installation (if necessary)
# %pip install simple-crypt cryptography

import os
import json
from datetime import datetime

from cryptography.fernet import Fernet # Use Fernet for symmetric encryption
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Configuration (replace with your actual values)
# Encryption Algorithm: Fernet (symmetric encryption)
# Key Management System: Azure Key Vault (example usage shown below - adapt as needed)
# Source Table: purgo_playground.customer_360_raw_clone
# Destination Table: purgo_playground.customer_360_raw_clone
# JSON Location: /Volumes/agilisium_playground/purgo_playground/de_dq12
PII_COLUMNS = ["name", "email", "phone", "zip"]
KEY_VAULT_NAME = "your-keyvault-name" # Replace with actual Key Vault Name if using Azure Key Vault
KEY_NAME = "your-key-name" # Replace with actual key name if using Azure Key Vault


# Key generation (using Fernet, demonstrating secure key handling with Azure Key Vault as an example)
try:
    # Azure Key Vault integration (replace with your actual Key Vault implementation)

    # 1. Retrieve Key from Azure Key Vault (if exists) or generate a new one
    try:
        # Try to get the key from Key Vault
        #  This is a placeholder, replace with your Key Vault code
        #  Use Azure Key Vault libraries for secure access
        # key = dbutils.secrets.get(scope="purgo-secret-scope", key="purgo-vault-secret")
        # f = Fernet(key) # Initialize Fernet with retrieved key
        raise Exception("Key not found") # Simulate key not found for testing key generation


    except Exception as e:
        if "Key not found" in str(e):  # or another appropriate check for key absence
            print("Generating a new encryption key and storing it in Azure Key Vault...")
            # Generate a new key
            key = Fernet.generate_key()
            f = Fernet(key) # Initialize Fernet
            # Store the key in Azure Key Vault
            # Use Azure Key Vault libraries here to save 'key' securely
            # Example (replace with your Key Vault logic):
            # dbutils.secrets.set(scope = "purgo-secret-scope", key = "purgo-vault-secret", value = key.decode()) # Store as string
        else:
            raise  # Re-raise other exceptions


    # Encryption function (using Fernet)
    def encrypt_data(data, f):
        if data is not None: # Handle Nulls
            encrypted_data = f.encrypt(data.encode())
            return encrypted_data.decode() # Decode to string for Delta Lake
        return None


    spark = SparkSession.builder.appName("EncryptPII").getOrCreate()
    # Load data from source table
    source_table_name = "purgo_playground.customer_360_raw_clone"
    df = spark.table(source_table_name)



    # Encrypt PII columns
    for column in PII_COLUMNS:
        df = df.withColumn(column, F.col(column).cast(StringType())) # Cast to string
        udf_encrypt = F.udf(lambda x: encrypt_data(x, f), StringType()) # Create UDF
        df = df.withColumn(column, udf_encrypt(F.col(column)))


    # Save the encrypted data to the destination table (overwrite mode)
    destination_table_name = "purgo_playground.customer_360_raw_clone"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destination_table_name)


    # Save the encryption key as a JSON file (REPLACE WITH SECURE KEY MANAGEMENT)
    key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq12/encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    # CAUTION: In a real-world scenario, never store encryption keys as cleartext like this.
    # key_data = {"encryption_key": key.decode()} # Decode bytes to string for JSON serialization.

    # with open(key_file_path, "w") as f_key:
    #    json.dump(key_data, f_key)



except Exception as e:
    print(f"An error occurred: {e}")


