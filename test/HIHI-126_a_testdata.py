from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DecimalType, ArrayType, MapType

# Create SparkSession
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for test data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("double_value", DoubleType(), True),
    StructField("decimal_value", DecimalType(10, 2), True),
    StructField("array_value", ArrayType(StringType()), True),
    StructField("map_value", MapType(StringType(), StringType()), True)
])

# Generate happy path test data
happy_path_data = [
    ("1", "John Doe", "2024-03-21T00:00:00.000+0000", 10.5, 10.50, ["apple", "banana"], {"key": "value"}),
    ("2", "Jane Doe", "2024-03-22T00:00:00.000+0000", 20.5, 20.50, ["orange", "grape"], {"key2": "value2"}),
    ("3", "Bob Smith", "2024-03-23T00:00:00.000+0000", 30.5, 30.50, ["pear", "peach"], {"key3": "value3"}),
    ("4", "Alice Johnson", "2024-03-24T00:00:00.000+0000", 40.5, 40.50, ["plum", "cherry"], {"key4": "value4"}),
    ("5", "Mike Brown", "2024-03-25T00:00:00.000+0000", 50.5, 50.50, ["watermelon", "strawberry"], {"key5": "value5"}),
]

# Generate edge cases test data
edge_cases_data = [
    ("6", "", "2024-03-26T00:00:00.000+0000", 0.0, 0.00, [], {}),
    ("7", None, "2024-03-27T00:00:00.000+0000", None, None, None, None),
    ("8", "Very long name that exceeds the maximum length", "2024-03-28T00:00:00.000+0000", 100.5, 100.50, ["very", "long", "array"], {"very": "long", "map": "value"}),
    ("9", "Name with special characters !@#$%^&*()", "2024-03-29T00:00:00.000+0000", -10.5, -10.50, ["special", "characters"], {"special": "characters", "map": "value"}),
    ("10", "Name with multi-byte characters ", "2024-03-30T00:00:00.000+0000", 10.5, 10.50, ["multi-byte", "characters"], {"multi-byte": "characters", "map": "value"}),
]

# Generate error cases test data
error_cases_data = [
    ("11", "Invalid timestamp", "Invalid timestamp", 10.5, 10.50, ["apple", "banana"], {"key": "value"}),
    ("12", "Invalid double value", "2024-03-21T00:00:00.000+0000", "Invalid double value", 10.50, ["orange", "grape"], {"key2": "value2"}),
    ("13", "Invalid decimal value", "2024-03-22T00:00:00.000+0000", 20.5, "Invalid decimal value", ["pear", "peach"], {"key3": "value3"}),
    ("14", "Invalid array value", "2024-03-23T00:00:00.000+0000", 30.5, 30.50, "Invalid array value", {"key4": "value4"}),
    ("15", "Invalid map value", "2024-03-24T00:00:00.000+0000", 40.5, 40.50, ["plum", "cherry"], "Invalid map value"),
]

# Create DataFrames for each test data category
happy_path_df = spark.createDataFrame(happy_path_data, schema)
edge_cases_df = spark.createDataFrame(edge_cases_data, schema)
error_cases_df = spark.createDataFrame(error_cases_data, schema)

# Write DataFrames to Databricks tables
happy_path_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.happy_path_test_data")
edge_cases_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.edge_cases_test_data")
error_cases_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.error_cases_test_data")



-- Create tables for each test data category
CREATE TABLE purgo_playground.happy_path_test_data (
  id STRING,
  name STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.edge_cases_test_data (
  id STRING,
  name STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.error_cases_test_data (
  id STRING,
  name STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

-- Insert happy path test data
INSERT INTO purgo_playground.happy_path_test_data
VALUES
  ('1', 'John Doe', '2024-03-21T00:00:00.000+0000', 10.5, 10.50, ARRAY['apple', 'banana'], MAP('key', 'value')),
  ('2', 'Jane Doe', '2024-03-22T00:00:00.000+0000', 20.5, 20.50, ARRAY['orange', 'grape'], MAP('key2', 'value2')),
  ('3', 'Bob Smith', '2024-03-23T00:00:00.000+0000', 30.5, 30.50, ARRAY['pear', 'peach'], MAP('key3', 'value3')),
  ('4', 'Alice Johnson', '2024-03-24T00:00:00.000+0000', 40.5, 40.50, ARRAY['plum', 'cherry'], MAP('key4', 'value4')),
  ('5', 'Mike Brown', '2024-03-25T00:00:00.000+0000', 50.5, 50.50, ARRAY['watermelon', 'strawberry'], MAP('key5', 'value5'));

-- Insert edge cases test data
INSERT INTO purgo_playground.edge_cases_test_data
VALUES
  ('6', '', '2024-03-26T00:00:00.000+0000', 0.0, 0.00, ARRAY[], MAP()),
  ('7', NULL, '2024-03-27T00:00:00.000+0000', NULL, NULL, NULL, NULL),
  ('8', 'Very long name that exceeds the maximum length', '2024-03-28T00:00:00.000+0000', 100.5, 100.50, ARRAY['very', 'long', 'array'], MAP('very', 'long', 'map', 'value')),
  ('9', 'Name with special characters !@#$%^&*()', '2024-03-29T00:00:00.000+0000', -10.5, -10.50, ARRAY['special', 'characters'], MAP('special', 'characters', 'map', 'value')),
  ('10', 'Name with multi-byte characters ', '2024-03-30T00:00:00.000+0000', 10.5, 10.50, ARRAY['multi-byte', 'characters'], MAP('multi-byte', 'characters', 'map', 'value'));

-- Insert error cases test data
INSERT INTO purgo_playground.error_cases_test_data
VALUES
  ('11', 'Invalid timestamp', 'Invalid timestamp', 10.5, 10.50, ARRAY['apple', 'banana'], MAP('key', 'value')),
  ('12', 'Invalid double value', '2024-03-21T00:00:00.000+0000', 'Invalid double value', 10.50, ARRAY['orange', 'grape'], MAP('key2', 'value2')),
  ('13', 'Invalid decimal value', '2024-03-22T00:00:00.000+0000', 20.5, 'Invalid decimal value', ARRAY['pear', 'peach'], MAP('key3', 'value3')),
  ('14', 'Invalid array value', '2024-03-23T00:00:00.000+0000', 30.5, 30.50, 'Invalid array value', MAP('key4', 'value4')),
  ('15', 'Invalid map value', '2024-03-24T00:00:00.000+0000', 40.5, 40.50, ARRAY['plum', 'cherry'], 'Invalid map value');
