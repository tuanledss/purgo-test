# PySpark Tests
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("Databricks Test").getOrCreate()

# Define DataFrame schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True)
])

# Sample data for testing
data = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "10101"),
    (2, None, None, None, None),  # Handling NULLs
    (3, "Jane Doe", "jane.doe@example.com", "0987654321", "20205")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Test schema validation
expected_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True)
])

assert df.schema == expected_schema, "Schema does not match expected schema"

# Test data quality - Check for NULLs
null_counts = df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns])
null_counts.show()

# Assert if there are expected NULLs (customize as needed)
null_name_count = null_counts.collect()[0]['name']
assert null_name_count == 1, "Unexpected NULL count in 'name' column"

# Test cases for Delta Lake MERGE, UPDATE, DELETE operations
# Setup two tables as DataFrame to simulate operations
source_df = spark.createDataFrame([(4, "Mary Jane", "mary.jane@example.com", "5647382910", "30303")], schema)

# Simulate an operation as Delta Lake doesn't natively execute in Spark DataFrame API
df = df.union(source_df)

# Simulate UPDATE operation
updated_df = df.withColumn("email", F.when(df.name == "John Doe", "new.john.doe@example.com").otherwise(df.email))

# Check UPDATE
assert updated_df.filter(updated_df.name == "John Doe").select("email").collect()[0][0] == "new.john.doe@example.com"

# Simulate DELETE by filtering
filtered_df = updated_df.filter(updated_df.name != "Mary Jane")

# Check DELETE
assert filtered_df.filter(filtered_df.name == "Mary Jane").count() == 0

spark.stop()
