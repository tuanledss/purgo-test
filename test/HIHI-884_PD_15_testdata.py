# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DecimalType, ArrayType, MapType
from pyspark.sql.functions import col, expr

# Create Spark session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define schema for d_product table
schema = StructType([
    StructField("prod_id", IntegerType(), False),
    StructField("item_nbr", StringType(), True),
    StructField("sellable_qty", IntegerType(), True),
    StructField("prod_exp_dt", StringType(), True)
])

# Generates happy path test data (valid scenarios)
happy_data = [
    (1, "A123", 50, "20240321"),
    (2, "B456", 10, "20240322"),
    (3, "C789", 25, "20240323"),
    (4, "D012", 5, "20240324"),
    (5, "E345", 100, "20240325")
]

# Edge cases (boundary conditions)
edge_data = [
    (6, "F678", 0, "20241231"),     # Boundary for sellable_qty
    (7, "G901", 2147483647, "20240101") # Max INT value
]

# Error cases for invalid scenarios
error_data = [
    (8, None, 10, "20240321"),          # NULL item_nbr
    (9, "I123", None, "20240322"),      # NULL sellable_qty
    (10, "J456", 20, "2024-03-21"),     # Invalid date format
    (11, "K789", 30, "March 21, 2024"), # Invalid date format
    (12, "L012", 40, "21032024")        # Incorrect date format
]

# NULL handling scenarios
null_data = [
    (13, None, None, None)   # All fields null
]

# Special characters and multi-byte characters
special_data = [
    (14, "想67😃", 50, "20240330"),  # Multibyte character in item_nbr
    (15, "M890!@#", 20, "20240331") # Special characters in item_nbr
]

# Combine all data into a single list
test_data = happy_data + edge_data + error_data + null_data + special_data

# Create DataFrame
df_d_product = spark.createDataFrame(test_data, schema)

# Show the resulting DataFrame
df_d_product.show(truncate=False)

# Data quality checks

# Check for NULL item_nbr
df_null_item_nbr = df_d_product.filter(col("item_nbr").isNull())
df_null_item_nbr.show(5)
print(f"Count of records with NULL 'item_nbr': {df_null_item_nbr.count()}")

# Check for NULL sellable_qty
df_null_sellable_qty = df_d_product.filter(col("sellable_qty").isNull())
df_null_sellable_qty.show(5)
print(f"Count of records with NULL 'sellable_qty': {df_null_sellable_qty.count()}")

# Check for incorrect prod_exp_dt format (not YYYYMMDD)
df_invalid_date_format = df_d_product.filter(~expr("prod_exp_dt LIKE '_______'"))
df_invalid_date_format.show(5)
print(f"Count of records with incorrect 'prod_exp_dt' format: {df_invalid_date_format.count()}")

