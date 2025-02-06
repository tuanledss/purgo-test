from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DecimalType, ArrayType, MapType

# Initialize Spark Session
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for test data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("double_value", DoubleType(), True),
    StructField("decimal_value", DecimalType(10, 2), True),
    StructField("array_value", ArrayType(StringType()), True),
    StructField("map_value", MapType(StringType(), StringType()), True)
])

# Generate happy path test data
happy_path_data = [
    ("1", "2024-03-21T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("2", "2024-03-22T00:00:00.000+0000", 20.5, 20.50, ["value3", "value4"], {"key3": "value3", "key4": "value4"}),
    ("3", "2024-03-23T00:00:00.000+0000", 30.5, 30.50, ["value5", "value6"], {"key5": "value5", "key6": "value6"}),
    ("4", "2024-03-24T00:00:00.000+0000", 40.5, 40.50, ["value7", "value8"], {"key7": "value7", "key8": "value8"}),
    ("5", "2024-03-25T00:00:00.000+0000", 50.5, 50.50, ["value9", "value10"], {"key9": "value9", "key10": "value10"}),
]

# Generate edge cases test data
edge_cases_data = [
    ("6", "2024-03-26T00:00:00.000+0000", 0.0, 0.00, [], {}),
    ("7", "2024-03-27T00:00:00.000+0000", 100.0, 100.00, ["value1"], {"key1": "value1"}),
    ("8", "2024-03-28T00:00:00.000+0000", -10.5, -10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("9", "2024-03-29T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2", "value3"], {"key1": "value1", "key2": "value2", "key3": "value3"}),
    ("10", "2024-03-30T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2", "value3", "value4"], {"key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"}),
]

# Generate error cases test data
error_cases_data = [
    ("11", "invalid_timestamp", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("12", "2024-03-31T00:00:00.000+0000", "invalid_double", 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("13", "2024-03-31T00:00:00.000+0000", 10.5, "invalid_decimal", ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("14", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, "invalid_array", {"key1": "value1", "key2": "value2"}),
    ("15", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], "invalid_map"),
]

# Generate NULL handling scenarios test data
null_handling_data = [
    ("16", None, 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("17", "2024-03-31T00:00:00.000+0000", None, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("18", "2024-03-31T00:00:00.000+0000", 10.5, None, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("19", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, None, {"key1": "value1", "key2": "value2"}),
    ("20", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], None),
]

# Generate special characters and multi-byte characters test data
special_chars_data = [
    ("21", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("22", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("23", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("24", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
    ("25", "2024-03-31T00:00:00.000+0000", 10.5, 10.50, ["value1", "value2"], {"key1": "value1", "key2": "value2"}),
]

# Create DataFrames for each test data category
happy_path_df = spark.createDataFrame(happy_path_data, schema)
edge_cases_df = spark.createDataFrame(edge_cases_data, schema)
error_cases_df = spark.createDataFrame(error_cases_data, schema)
null_handling_df = spark.createDataFrame(null_handling_data, schema)
special_chars_df = spark.createDataFrame(special_chars_data, schema)

# Write DataFrames to Databricks tables
happy_path_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.happy_path_test_data")
edge_cases_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.edge_cases_test_data")
error_cases_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.error_cases_test_data")
null_handling_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.null_handling_test_data")
special_chars_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.special_chars_test_data")



-- Create tables for each test data category
CREATE TABLE purgo_playground.happy_path_test_data (
  id STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.edge_cases_test_data (
  id STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.error_cases_test_data (
  id STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.null_handling_test_data (
  id STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.special_chars_test_data (
  id STRING,
  timestamp TIMESTAMP,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 2),
  array_value ARRAY<STRING>,
  map_value MAP<STRING, STRING>
);

-- Insert data into tables
INSERT INTO purgo_playground.happy_path_test_data
VALUES
  ('1', '2024-03-21T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('2', '2024-03-22T00:00:00.000+0000', 20.5, 20.50, ['value3', 'value4'], {'key3': 'value3', 'key4': 'value4'}),
  ('3', '2024-03-23T00:00:00.000+0000', 30.5, 30.50, ['value5', 'value6'], {'key5': 'value5', 'key6': 'value6'}),
  ('4', '2024-03-24T00:00:00.000+0000', 40.5, 40.50, ['value7', 'value8'], {'key7': 'value7', 'key8': 'value8'}),
  ('5', '2024-03-25T00:00:00.000+0000', 50.5, 50.50, ['value9', 'value10'], {'key9': 'value9', 'key10': 'value10'});

INSERT INTO purgo_playground.edge_cases_test_data
VALUES
  ('6', '2024-03-26T00:00:00.000+0000', 0.0, 0.00, [], {}),
  ('7', '2024-03-27T00:00:00.000+0000', 100.0, 100.00, ['value1'], {'key1': 'value1'}),
  ('8', '2024-03-28T00:00:00.000+0000', -10.5, -10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('9', '2024-03-29T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2', 'value3'], {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}),
  ('10', '2024-03-30T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2', 'value3', 'value4'], {'key1': 'value1', 'key2': 'value2', 'key3': 'value3', 'key4': 'value4'});

INSERT INTO purgo_playground.error_cases_test_data
VALUES
  ('11', 'invalid_timestamp', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('12', '2024-03-31T00:00:00.000+0000', 'invalid_double', 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('13', '2024-03-31T00:00:00.000+0000', 10.5, 'invalid_decimal', ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('14', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, 'invalid_array', {'key1': 'value1', 'key2': 'value2'}),
  ('15', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], 'invalid_map');

INSERT INTO purgo_playground.null_handling_test_data
VALUES
  ('16', NULL, 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('17', '2024-03-31T00:00:00.000+0000', NULL, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('18', '2024-03-31T00:00:00.000+0000', 10.5, NULL, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('19', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, NULL, {'key1': 'value1', 'key2': 'value2'}),
  ('20', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], NULL);

INSERT INTO purgo_playground.special_chars_test_data
VALUES
  ('21', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('22', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('23', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('24', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'}),
  ('25', '2024-03-31T00:00:00.000+0000', 10.5, 10.50, ['value1', 'value2'], {'key1': 'value1', 'key2': 'value2'});
