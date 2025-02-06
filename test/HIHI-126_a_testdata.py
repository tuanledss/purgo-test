from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Initialize Spark Session
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for test data
schema = T.StructType([
    T.StructField("id", T.BigIntType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("phone", T.StringType(), True),
    T.StructField("address", T.StructType([
        T.StructField("street", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("zip", T.StringType(), True)
    ]), True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("updated_at", T.TimestampType(), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True),
    T.StructField("properties", T.MapType(T.StringType(), T.StringType()), True)
])

# Generate happy path test data
happy_path_data = [
    (1, "John Doe", "john.doe@example.com", "123-456-7890", 
     {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     ["tag1", "tag2"], {"key1": "value1", "key2": "value2"}),
    (2, "Jane Doe", "jane.doe@example.com", "987-654-3210", 
     {"street": "456 Elm St", "city": "Othertown", "state": "NY", "zip": "67890"}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     ["tag3", "tag4"], {"key3": "value3", "key4": "value4"}),
    # Add more happy path test data as needed
]

# Generate edge case test data
edge_case_data = [
    (3, "", "", "", 
     {"street": "", "city": "", "state": "", "zip": ""}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     [], {}),
    (4, None, None, None, 
     {"street": None, "city": None, "state": None, "zip": None}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     None, None),
    # Add more edge case test data as needed
]

# Generate error case test data
error_case_data = [
    (5, "Invalid", "Invalid", "Invalid", 
     {"street": "Invalid", "city": "Invalid", "state": "Invalid", "zip": "Invalid"}, 
     "Invalid", "Invalid", 
     ["Invalid"], {"Invalid": "Invalid"}),
    # Add more error case test data as needed
]

# Generate NULL handling test data
null_handling_data = [
    (6, None, "john.doe@example.com", "123-456-7890", 
     {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     ["tag1", "tag2"], {"key1": "value1", "key2": "value2"}),
    (7, "Jane Doe", None, "987-654-3210", 
     {"street": "456 Elm St", "city": "Othertown", "state": "NY", "zip": "67890"}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     ["tag3", "tag4"], {"key3": "value3", "key4": "value4"}),
    # Add more NULL handling test data as needed
]

# Generate special characters and multi-byte characters test data
special_chars_data = [
    (8, "John_Doe", "john.doe@example.com", "123-456-7890", 
     {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     ["tag1", "tag2"], {"key1": "value1", "key2": "value2"}),
    (9, "Jane_Doe", "jane.doe@example.com", "987-654-3210", 
     {"street": "456 Elm St", "city": "Othertown", "state": "NY", "zip": "67890"}, 
     "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 
     ["tag3", "tag4"], {"key3": "value3", "key4": "value4"}),
    # Add more special characters and multi-byte characters test data as needed
]

# Combine all test data
test_data = happy_path_data + edge_case_data + error_case_data + null_handling_data + special_chars_data

# Create DataFrame from test data
df = spark.createDataFrame(test_data, schema)

# Write DataFrame to Databricks table
df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.test_data")


Note: This code generates test data for a hypothetical table `test_data` in the `purgo_playground` schema. You will need to modify the schema and table name to match your actual use case. Additionally, you may need to add more test data to cover all the scenarios and edge cases.