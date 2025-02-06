from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DecimalType, ArrayType, MapType

# Create SparkSession
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for test data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("balance", DecimalType(10, 2), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("hobbies", ArrayType(StringType()), True),
    StructField("settings", MapType(StringType(), StringType()), True)
])

# Generate happy path test data
happy_path_data = [
    ("1", "John Doe", "john.doe@example.com", "123-456-7890", "123 Main St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 1000.00, 0.05, ["reading", "hiking"], {"theme": "light"}),
    ("2", "Jane Doe", "jane.doe@example.com", "987-654-3210", "456 Elm St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 500.00, 0.03, ["cooking", "traveling"], {"theme": "dark"}),
    ("3", "Bob Smith", "bob.smith@example.com", "555-123-4567", "789 Oak St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 2000.00, 0.07, ["gaming", "watching movies"], {"theme": "light"})
]

# Generate edge cases test data
edge_cases_data = [
    ("4", "Alice Johnson", "alice.johnson@example.com", "111-111-1111", "123 Main St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 0.00, 0.00, [], {}),
    ("5", "Mike Brown", "mike.brown@example.com", "999-999-9999", "456 Elm St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 10000.00, 1.00, ["reading", "hiking", "cooking", "traveling", "gaming", "watching movies"], {"theme": "light", "font_size": "large"}),
    ("6", "Emma Davis", "emma.davis@example.com", "777-777-7777", "789 Oak St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", -1000.00, -0.05, ["invalid hobby"], {"invalid setting": "invalid value"})
]

# Generate error cases test data
error_cases_data = [
    ("7", "Invalid Name", "invalid.email", "123-456-7890", "123 Main St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 1000.00, 0.05, ["reading", "hiking"], {"theme": "light"}),
    ("8", "John Doe", "john.doe@example.com", "invalid phone", "456 Elm St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 500.00, 0.03, ["cooking", "traveling"], {"theme": "dark"}),
    ("9", "Jane Doe", "jane.doe@example.com", "987-654-3210", "invalid address", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 2000.00, 0.07, ["gaming", "watching movies"], {"theme": "light"})
]

# Generate NULL handling scenarios test data
null_handling_data = [
    ("10", None, "john.doe@example.com", "123-456-7890", "123 Main St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 1000.00, 0.05, ["reading", "hiking"], {"theme": "light"}),
    ("11", "John Doe", None, "987-654-3210", "456 Elm St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 500.00, 0.03, ["cooking", "traveling"], {"theme": "dark"}),
    ("12", "Jane Doe", "jane.doe@example.com", None, "789 Oak St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 2000.00, 0.07, ["gaming", "watching movies"], {"theme": "light"})
]

# Generate special characters and multi-byte characters test data
special_chars_data = [
    ("13", "John_Doe", "john.doe@example.com", "123-456-7890", "123 Main St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 1000.00, 0.05, ["reading", "hiking"], {"theme": "light"}),
    ("14", "Jane Doe", "jane.doe@example.com", "987-654-3210", "456 Elm St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 500.00, 0.03, ["cooking", "traveling"], {"theme": "dark"}),
    ("15", "", "bob.smith@example.com", "555-123-4567", "789 Oak St", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", 2000.00, 0.07, ["gaming", "watching movies"], {"theme": "light"})
]

# Create DataFrames for each test data category
happy_path_df = spark.createDataFrame(happy_path_data, schema)
edge_cases_df = spark.createDataFrame(edge_cases_data, schema)
error_cases_df = spark.createDataFrame(error_cases_data, schema)
null_handling_df = spark.createDataFrame(null_handling_data, schema)
special_chars_df = spark.createDataFrame(special_chars_data, schema)

# Write DataFrames to Parquet files
happy_path_df.write.parquet("happy_path_test_data")
edge_cases_df.write.parquet("edge_cases_test_data")
error_cases_df.write.parquet("error_cases_test_data")
null_handling_df.write.parquet("null_handling_test_data")
special_chars_df.write.parquet("special_chars_test_data")



-- Create table for test data
CREATE TABLE test_data (
  id STRING,
  name STRING,
  email STRING,
  phone STRING,
  address STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  balance DECIMAL(10, 2),
  interest_rate DOUBLE,
  hobbies ARRAY<STRING>,
  settings MAP<STRING, STRING>
);

-- Insert happy path test data
INSERT INTO test_data VALUES
  ('1', 'John Doe', 'john.doe@example.com', '123-456-7890', '123 Main St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 1000.00, 0.05, ARRAY['reading', 'hiking'], MAP['theme': 'light']),
  ('2', 'Jane Doe', 'jane.doe@example.com', '987-654-3210', '456 Elm St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 500.00, 0.03, ARRAY['cooking', 'traveling'], MAP['theme': 'dark']),
  ('3', 'Bob Smith', 'bob.smith@example.com', '555-123-4567', '789 Oak St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 2000.00, 0.07, ARRAY['gaming', 'watching movies'], MAP['theme': 'light']);

-- Insert edge cases test data
INSERT INTO test_data VALUES
  ('4', 'Alice Johnson', 'alice.johnson@example.com', '111-111-1111', '123 Main St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 0.00, 0.00, ARRAY[], MAP[]),
  ('5', 'Mike Brown', 'mike.brown@example.com', '999-999-9999', '456 Elm St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 10000.00, 1.00, ARRAY['reading', 'hiking', 'cooking', 'traveling', 'gaming', 'watching movies'], MAP['theme': 'light', 'font_size': 'large']),
  ('6', 'Emma Davis', 'emma.davis@example.com', '777-777-7777', '789 Oak St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', -1000.00, -0.05, ARRAY['invalid hobby'], MAP['invalid setting': 'invalid value']);

-- Insert error cases test data
INSERT INTO test_data VALUES
  ('7', 'Invalid Name', 'invalid.email', '123-456-7890', '123 Main St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 1000.00, 0.05, ARRAY['reading', 'hiking'], MAP['theme': 'light']),
  ('8', 'John Doe', 'john.doe@example.com', 'invalid phone', '456 Elm St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 500.00, 0.03, ARRAY['cooking', 'traveling'], MAP['theme': 'dark']),
  ('9', 'Jane Doe', 'jane.doe@example.com', '987-654-3210', 'invalid address', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 2000.00, 0.07, ARRAY['gaming', 'watching movies'], MAP['theme': 'light']);

-- Insert NULL handling scenarios test data
INSERT INTO test_data VALUES
  ('10', NULL, 'john.doe@example.com', '123-456-7890', '123 Main St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 1000.00, 0.05, ARRAY['reading', 'hiking'], MAP['theme': 'light']),
  ('11', 'John Doe', NULL, '987-654-3210', '456 Elm St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 500.00, 0.03, ARRAY['cooking', 'traveling'], MAP['theme': 'dark']),
  ('12', 'Jane Doe', 'jane.doe@example.com', NULL, '789 Oak St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 2000.00, 0.07, ARRAY['gaming', 'watching movies'], MAP['theme': 'light']);

-- Insert special characters and multi-byte characters test data
INSERT INTO test_data VALUES
  ('13', 'John_Doe', 'john.doe@example.com', '123-456-7890', '123 Main St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 1000.00, 0.05, ARRAY['reading', 'hiking'], MAP['theme': 'light']),
  ('14', 'Jane Doe', 'jane.doe@example.com', '987-654-3210', '456 Elm St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 500.00, 0.03, ARRAY['cooking', 'traveling'], MAP['theme': 'dark']),
  ('15', '', 'bob.smith@example.com', '555-123-4567', '789 Oak St', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 2000.00, 0.07, ARRAY['gaming', 'watching movies'], MAP['theme': 'light']);
