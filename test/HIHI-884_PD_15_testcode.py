# PySpark Data Quality Tests for d_product DataFrame

# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, regexp_extract

# Create Spark session
spark = SparkSession.builder \
    .appName("Databricks Test Code") \
    .getOrCreate()

# Define schema for d_product table
schema = StructType([
    StructField("prod_id", IntegerType(), False),
    StructField("item_nbr", StringType(), True),
    StructField("sellable_qty", IntegerType(), True),
    StructField("prod_exp_dt", StringType(), True)
])

# Sample data to simulate the table
test_data = [
    (1, "A123", 50, "20240321"),
    (2, None, 10, "20240322"),        # NULL item_nbr
    (3, "C789", None, "20240323"),    # NULL sellable_qty
    (4, "D012", 5, "2024-03-24"),     # Incorrect date format
    (5, "E345", 100, "21032024")      # Incorrect date format
]

# Create DataFrame
df_d_product = spark.createDataFrame(test_data, schema)

# Data quality checks

# Check for NULL item_nbr
df_null_item_nbr = df_d_product.filter(col("item_nbr").isNull())
df_null_item_nbr.show(5)
assert df_null_item_nbr.count() > 0, "Expected null item_nbr records not found."

# Check for NULL sellable_qty
df_null_sellable_qty = df_d_product.filter(col("sellable_qty").isNull())
df_null_sellable_qty.show(5)
assert df_null_sellable_qty.count() > 0, "Expected null sellable_qty records not found."

# Check for incorrect prod_exp_dt format (not YYYYMMDD)
df_invalid_date_format = df_d_product.filter(~regexp_extract(col("prod_exp_dt"), '^\\d{8}$', 0) == "")
df_invalid_date_format.show(5)
assert df_invalid_date_format.count() > 0, "Expected invalid prod_exp_dt format records not found."



-- SQL Data Quality Checks for the d_product table in the purgo_playground catalog

/* Section: Verify item_nbr is not null */
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/* Section: Verify sellable_qty is not null */
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* Section: Validate prod_exp_dt format */
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT REGEXP_LIKE(prod_exp_dt, '^\\d{8}$');

SELECT *
FROM purgo_playground.d_product
WHERE NOT REGEXP_LIKE(prod_exp_dt, '^\\d{8}$')
LIMIT 5;
