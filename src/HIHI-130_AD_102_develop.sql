/* SQL Test Framework Setup */
DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone;
CREATE TABLE purgo_playground.customer_360_raw_clone AS SELECT * FROM purgo_playground.customer_360_raw;

/* SQL Data Type Testing */
SELECT typeof(name) FROM purgo_playground.customer_360_raw LIMIT 1; -- STRING
SELECT typeof(email) FROM purgo_playground.customer_360_raw LIMIT 1; -- STRING
SELECT typeof(phone) FROM purgo_playground.customer_360_raw LIMIT 1; -- STRING
SELECT typeof(zip) FROM purgo_playground.customer_360_raw LIMIT 1; -- STRING

SELECT CASE WHEN name IS NULL THEN 'null' ELSE 'not_null' END AS null_check FROM purgo_playground.customer_360_raw LIMIT 1;
SELECT CASE WHEN email IS NULL THEN 'null' ELSE 'not_null' END AS null_check FROM purgo_playground.customer_360_raw LIMIT 1;



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from datetime import datetime
from Crypto.Cipher import AES
import base64
import json

# Initialize Spark session
spark = SparkSession.builder.appName("PII Encryption Test").getOrCreate()

# Encryption utility function using AES
def encrypt_pii(text):
    if text is None:
        return None
    cipher = AES.new(b'Sixteen byte key', AES.MODE_CFB, b'Sixteen byte IV__')
    enc_bytes = cipher.encrypt(text.encode('utf-8'))
    return base64.b64encode(enc_bytes).decode('utf-8')

# UDF for column encryption
encrypt_udf = udf(encrypt_pii, StringType())

# Load customer_360_raw_clone table
raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone")

# Apply encryption
encrypted_df = raw_clone_df.withColumn("name", encrypt_udf(col("name")))\
                           .withColumn("email", encrypt_udf(col("email")))\
                           .withColumn("phone", encrypt_udf(col("phone")))\
                           .withColumn("zip", encrypt_udf(col("zip")))

# Save to clone table ensuring data safety
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key as JSON
encryption_metadata = {"key": base64.b64encode(b'Sixteen byte key').decode('utf-8'),
                       "iv": base64.b64encode(b'Sixteen byte IV__').decode('utf-8')}
current_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
json_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_time}.json"

# Save key to a JSON file
with open(json_file_path, 'w') as file:
    json.dump(encryption_metadata, file)

spark.stop()



/* SQL Integration Test */
MERGE INTO purgo_playground.customer_360_raw_clone AS target 
USING purgo_playground.customer_360_raw AS source
ON target.id = source.id
WHEN MATCHED THEN 
  UPDATE SET target.name = source.name,
             target.email = source.email,
             target.phone = source.phone,
             target.zip = source.zip
WHEN NOT MATCHED THEN 
  INSERT (id, name, email, phone, zip) 
  VALUES (source.id, source.name, source.email, source.phone, source.zip);

/* Validate data characteristics and row count after operation */
SELECT COUNT(*) FROM purgo_playground.customer_360_raw_clone;
