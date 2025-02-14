# Python/PySpark test code for data quality checks using Spark framework

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality Checks") \
    .getOrCreate()

# Define schema for d_product table
schema = StructType([
    StructField("prod_id", IntegerType(), nullable=False),
    StructField("item_nbr", StringType(), nullable=True),
    StructField("sellable_qty", IntegerType(), nullable=True),
    StructField("prod_exp_dt", StringType(), nullable=True)
])

# Sample data creation according to the schema
data = [
    (1, "A123", 50, "20220101"),
    (2, None, 30, "20220101"),  # Null item_nbr
    (3, "B456", None, "20220103"),  # Null sellable_qty
    (4, "C789", 70, "2022010"),  # Incorrect date format
    (5, "D012", 60, "2022031"),  # Incorrect date format
]

# Create DataFrame
df_d_product = spark.createDataFrame(data, schema)

# Show the DataFrame
df_d_product.show(truncate=False)

# Check for NULL item_nbr
null_item_nbr_count = df_d_product.filter(col("item_nbr").isNull()).count()
null_item_nbr_sample = df_d_product.filter(col("item_nbr").isNull()).limit(5)
print(f"Count of records with NULL item_nbr: {null_item_nbr_count}")
null_item_nbr_sample.show()

# Check for NULL sellable_qty
null_sellable_qty_count = df_d_product.filter(col("sellable_qty").isNull()).count()
null_sellable_qty_sample = df_d_product.filter(col("sellable_qty").isNull()).limit(5)
print(f"Count of records with NULL sellable_qty: {null_sellable_qty_count}")
null_sellable_qty_sample.show()

# Check for incorrect prod_exp_dt format
correct_format_expr = "^[0-9]{8}$"
invalid_date_count = df_d_product.filter(~col("prod_exp_dt").rlike(correct_format_expr)).count()
invalid_date_sample = df_d_product.filter(~col("prod_exp_dt").rlike(correct_format_expr)).limit(5)
print(f"Count of records with incorrect prod_exp_dt format: {invalid_date_count}")
invalid_date_sample.show()

# Clean up resources
spark.stop()
