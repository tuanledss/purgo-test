-- SQL Test Cases for Data Quality on d_product Table

/*
  Data Quality Test: Check for NULLs in item_nbr
*/
-- Get count of records where item_nbr is NULL
SELECT COUNT(*) AS count_null_item_nbr
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display sample records where item_nbr is NULL
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/*
  Data Quality Test: Check for NULLs in sellable_qty
*/
-- Get count of records where sellable_qty is NULL
SELECT COUNT(*) AS count_null_sellable_qty
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display sample records where sellable_qty is NULL
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/*
  Data Quality Test: Validate prod_exp_dt Format
*/
-- Get count of records where prod_exp_dt does not match YYYYMMDD format
SELECT COUNT(*) AS count_invalid_prod_exp_dt
FROM purgo_playground.d_product
WHERE NOT (prod_exp_dt RLIKE '^[0-9]{8}$');

-- Display sample records where prod_exp_dt does not match YYYYMMDD format
SELECT *
FROM purgo_playground.d_product
WHERE NOT (prod_exp_dt RLIKE '^[0-9]{8}$')
LIMIT 5;



# PySpark Test Cases for Data Quality on d_product Table

# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("Data Quality Tests on d_product") \
    .getOrCreate()

# Define schema for the d_product table
schema = StructType([
    StructField("prod_id", IntegerType(), False),
    StructField("item_nbr", StringType(), True),
    StructField("sellable_qty", IntegerType(), True),
    StructField("prod_exp_dt", StringType(), True)
])

# Sample DataFrame representing d_product table
data = [
    (1, "A123", 50, "20230301"),
    (2, None, 25, "20230302"),  # Null item_nbr
    (3, "B456", None, "20230303"),  # Null sellable_qty
    (4, "C789", 40, "2023-03-04"),  # Incorrect date format
    (5, "D012", 60, "04032023"),  # Incorrect date format
]

df_d_product = spark.createDataFrame(data, schema)

# Check for NULL item_nbr
df_null_item_nbr = df_d_product.filter(col("item_nbr").isNull())
assert df_null_item_nbr.count() == 1, "Data quality error: Null values found in item_nbr"
df_null_item_nbr.show()

# Check for NULL sellable_qty
df_null_sellable_qty = df_d_product.filter(col("sellable_qty").isNull())
assert df_null_sellable_qty.count() == 1, "Data quality error: Null values found in sellable_qty"
df_null_sellable_qty.show()

# Check for invalid date format in prod_exp_dt
df_invalid_date_format = df_d_product.filter(~col("prod_exp_dt").rlike("^[0-9]{8}$"))
assert df_invalid_date_format.count() == 2, "Data quality error: Invalid prod_exp_dt format found"
df_invalid_date_format.show()
