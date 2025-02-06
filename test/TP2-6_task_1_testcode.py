from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DecimalType, ArrayType, MapType

# Initialize Spark Session
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for test data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("quantity", DoubleType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

# Generate happy path test data
happy_path_data = [
    ("1", "Product A", "2024-03-21T00:00:00.000+0000", 10.99, 5.0, ["tag1", "tag2"], {"color": "red", "size": "large"}),
    ("2", "Product B", "2024-03-22T00:00:00.000+0000", 9.99, 3.0, ["tag2", "tag3"], {"color": "blue", "size": "medium"}),
    ("3", "Product C", "2024-03-23T00:00:00.000+0000", 12.99, 2.0, ["tag1", "tag3"], {"color": "green", "size": "small"}),
    ("4", "Product D", "2024-03-24T00:00:00.000+0000", 8.99, 4.0, ["tag2", "tag4"], {"color": "yellow", "size": "large"}),
    ("5", "Product E", "2024-03-25T00:00:00.000+0000", 11.99, 1.0, ["tag1", "tag4"], {"color": "purple", "size": "medium"}),
]

# Generate edge cases test data
edge_cases_data = [
    ("6", "Product F", "2024-03-26T00:00:00.000+0000", 0.00, 0.0, [], {}),
    ("7", "Product G", "2024-03-27T00:00:00.000+0000", 100.00, 10.0, ["tag1", "tag2", "tag3", "tag4", "tag5"], {"color": "red", "size": "large", "material": "wood"}),
    ("8", "Product H", "2024-03-28T00:00:00.000+0000", -10.00, -5.0, ["tag1", "tag2"], {"color": "blue", "size": "medium"}),
    ("9", "Product I", "2024-03-29T00:00:00.000+0000", 10.00, 5.0, ["tag1", "tag2"], {"color": "green", "size": "small"}),
    ("10", "Product J", "2024-03-30T00:00:00.000+0000", 0.01, 0.01, ["tag1", "tag2"], {"color": "yellow", "size": "large"}),
]

# Generate error cases test data
error_cases_data = [
    ("11", None, "2024-03-31T00:00:00.000+0000", 10.99, 5.0, ["tag1", "tag2"], {"color": "red", "size": "large"}),
    ("12", "Product L", None, 9.99, 3.0, ["tag2", "tag3"], {"color": "blue", "size": "medium"}),
    ("13", "Product M", "2024-04-01T00:00:00.000+0000", None, 2.0, ["tag1", "tag3"], {"color": "green", "size": "small"}),
    ("14", "Product N", "2024-04-02T00:00:00.000+0000", 8.99, None, ["tag2", "tag4"], {"color": "yellow", "size": "large"}),
    ("15", "Product O", "2024-04-03T00:00:00.000+0000", 11.99, 1.0, None, {"color": "purple", "size": "medium"}),
]

# Generate NULL handling scenarios test data
null_handling_data = [
    (None, "Product P", "2024-04-04T00:00:00.000+0000", 10.99, 5.0, ["tag1", "tag2"], {"color": "red", "size": "large"}),
    ("17", None, "2024-04-05T00:00:00.000+0000", 9.99, 3.0, ["tag2", "tag3"], {"color": "blue", "size": "medium"}),
    ("18", "Product R", None, 12.99, 2.0, ["tag1", "tag3"], {"color": "green", "size": "small"}),
    ("19", "Product S", "2024-04-07T00:00:00.000+0000", None, 4.0, ["tag2", "tag4"], {"color": "yellow", "size": "large"}),
    ("20", "Product T", "2024-04-08T00:00:00.000+0000", 8.99, None, ["tag1", "tag4"], {"color": "purple", "size": "medium"}),
]

# Generate special characters and multi-byte characters test data
special_chars_data = [
    ("21", "Product U", "2024-04-09T00:00:00.000+0000", 10.99, 5.0, ["tag1", "tag2"], {"color": "red", "size": "large"}),
    ("22", "Product V", "2024-04-10T00:00:00.000+0000", 9.99, 3.0, ["tag2", "tag3"], {"color": "blue", "size": "medium"}),
    ("23", "Product W", "2024-04-11T00:00:00.000+0000", 12.99, 2.0, ["tag1", "tag3"], {"color": "green", "size": "small"}),
    ("24", "Product X", "2024-04-12T00:00:00.000+0000", 8.99, 4.0, ["tag2", "tag4"], {"color": "yellow", "size": "large"}),
    ("25", "Product Y", "2024-04-13T00:00:00.000+0000", 11.99, 1.0, ["tag1", "tag4"], {"color": "purple", "size": "medium"}),
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
  name STRING,
  created_at TIMESTAMP,
  price DECIMAL(10, 2),
  quantity DOUBLE,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.edge_cases_test_data (
  id STRING,
  name STRING,
  created_at TIMESTAMP,
  price DECIMAL(10, 2),
  quantity DOUBLE,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.error_cases_test_data (
  id STRING,
  name STRING,
  created_at TIMESTAMP,
  price DECIMAL(10, 2),
  quantity DOUBLE,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.null_handling_test_data (
  id STRING,
  name STRING,
  created_at TIMESTAMP,
  price DECIMAL(10, 2),
  quantity DOUBLE,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>
);

CREATE TABLE purgo_playground.special_chars_test_data (
  id STRING,
  name STRING,
  created_at TIMESTAMP,
  price DECIMAL(10, 2),
  quantity DOUBLE,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>
);

-- Insert Data into Tables with Correct Syntax for Arrays and Maps
INSERT INTO purgo_playground.happy_path_test_data
VALUES
  ('1', 'Product A', '2024-03-21T00:00:00.000+0000', 10.99, 5.0, ARRAY('tag1', 'tag2'), MAP('color', 'red', 'size', 'large')),
  ('2', 'Product B', '2024-03-22T00:00:00.000+0000', 9.99, 3.0, ARRAY('tag2', 'tag3'), MAP('color', 'blue', 'size', 'medium')),
  ('3', 'Product C', '2024-03-23T00:00:00.000+0000', 12.99, 2.0, ARRAY('tag1', 'tag3'), MAP('color', 'green', 'size', 'small')),
  ('4', 'Product D', '2024-03-24T00:00:00.000+0000', 8.99, 4.0, ARRAY('tag2', 'tag4'), MAP('color', 'yellow', 'size', 'large')),
  ('5', 'Product E', '2024-03-25T00:00:00.000+0000', 11.99, 1.0, ARRAY('tag1', 'tag4'), MAP('color', 'purple', 'size', 'medium'));

INSERT INTO purgo_playground.edge_cases_test_data
VALUES
  ('6', 'Product F', '2024-03-26T00:00:00.000+0000', 0.00, 0.0, ARRAY(), MAP()),
  ('7', 'Product G', '2024-03-27T00:00:00.000+0000', 100.00, 10.0, ARRAY('tag1', 'tag2', 'tag3', 'tag4', 'tag5'), MAP('color', 'red', 'size', 'large', 'material', 'wood')),
  ('8', 'Product H', '2024-03-28T00:00:00.000+0000', -10.00, -5.0, ARRAY('tag1', 'tag2'), MAP('color', 'blue', 'size', 'medium')),
  ('9', 'Product I', '2024-03-29T00:00:00.000+0000', 10.00, 5.0, ARRAY('tag1', 'tag2'), MAP('color', 'green', 'size', 'small')),
  ('10', 'Product J', '2024-03-30T00:00:00.000+0000', 0.01, 0.01, ARRAY('tag1', 'tag2'), MAP('color', 'yellow', 'size', 'large'));

INSERT INTO purgo_playground.error_cases_test_data
VALUES
  ('11', NULL, '2024-03-31T00:00:00.000+0000', 10.99, 5.0, ARRAY('tag1', 'tag2'), MAP('color', 'red', 'size', 'large')),
  ('12', 'Product L', NULL, 9.99, 3.0, ARRAY('tag2', 'tag3'), MAP('color', 'blue', 'size', 'medium')),
  ('13', 'Product M', '2024-04-01T00:00:00.000+0000', NULL, 2.0, ARRAY('tag1', 'tag3'), MAP('color', 'green', 'size', 'small')),
  ('14', 'Product N', '2024-04-02T00:00:00.000+0000', 8.99, NULL, ARRAY('tag2', 'tag4'), MAP('color', 'yellow', 'size', 'large')),
  ('15', 'Product O', '2024-04-03T00:00:00.000+0000', 11.99, 1.0, NULL, MAP('color', 'purple', 'size', 'medium'));

INSERT INTO purgo_playground.null_handling_test_data
VALUES
  (NULL, 'Product P', '2024-04-04T00:00:00.000+0000', 10.99, 5.0, ARRAY('tag1', 'tag2'), MAP('color', 'red', 'size', 'large')),
  ('17', NULL, '2024-04-05T00:00:00.000+0000', 9.99, 3.0, ARRAY('tag2', 'tag3'), MAP('color', 'blue', 'size', 'medium')),
  ('18', 'Product R', NULL, 12.99, 2.0, ARRAY('tag1', 'tag3'), MAP('color', 'green', 'size', 'small')),
  ('19', 'Product S', '2024-04-07T00:00:00.000+0000', NULL, 4.0, ARRAY('tag2', 'tag4'), MAP('color', 'yellow', 'size', 'large')),
  ('20', 'Product T', '2024-04-08T00:00:00.000+0000', 8.99, NULL, ARRAY('tag1', 'tag4'), MAP('color', 'purple', 'size', 'medium'));

INSERT INTO purgo_playground.special_chars_test_data
VALUES
  ('21', 'Product U', '2024-04-09T00:00:00.000+0000', 10.99, 5.0, ARRAY('tag1', 'tag2'), MAP('color', 'red', 'size', 'large')),
  ('22', 'Product V', '2024-04-10T00:00:00.000+0000', 9.99, 3.0, ARRAY('tag2', 'tag3'), MAP('color', 'blue', 'size', 'medium')),
  ('23', 'Product W', '2024-04-11T00:00:00.000+0000', 12.99, 2.0, ARRAY('tag1', 'tag3'), MAP('color', 'green', 'size', 'small')),
  ('24', 'Product X', '2024-04-12T00:00:00.000+0000', 8.99, 4.0, ARRAY('tag2', 'tag4'), MAP('color', 'yellow', 'size', 'large')),
  ('25', 'Product Y', '2024-04-13T00:00:00.000+0000', 11.99, 1.0, ARRAY('tag1', 'tag4'), MAP('color', 'purple', 'size', 'medium'));
