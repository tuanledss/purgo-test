-- SQL tests for d_product table data quality checks

/* 
   Test Suite: Data Quality Checks for d_product Table
   Description: This suite includes tests to ensure that data quality checks are performed on the d_product table to prevent
   moving erroneous data to production.
*/

-- Scenario: Verify item_nbr is not null
SELECT COUNT(*) AS null_item_nbr_count 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

-- Scenario: Verify sellable_qty is not null
SELECT COUNT(*) AS null_sellable_qty_count 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

-- Scenario: Validate prod_exp_dt format
SELECT COUNT(*) AS invalid_prod_exp_dt_count 
FROM purgo_playground.d_product 
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

SELECT * 
FROM purgo_playground.d_product 
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$' 
LIMIT 5;



# PySpark test code for data quality checks on d_product table

# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Create Spark session
spark = SparkSession.builder \
    .appName("Data Quality Checks - d_product Table") \
    .getOrCreate()

# Load d_product DataFrame (assuming Table is already available in Databricks environment)
df_d_product = spark.table("purgo_playground.d_product")

# Check for NULL item_nbr
df_null_item_nbr = df_d_product.filter(col("item_nbr").isNull())
df_null_item_nbr.show(5, truncate=False)
assert df_null_item_nbr.count() == 0, "There are records with NULL 'item_nbr'"

# Check for NULL sellable_qty
df_null_sellable_qty = df_d_product.filter(col("sellable_qty").isNull())
df_null_sellable_qty.show(5, truncate=False)
assert df_null_sellable_qty.count() == 0, "There are records with NULL 'sellable_qty'"

# Check for incorrect prod_exp_dt format (not 'YYYYMMDD')
df_invalid_date_format = df_d_product.filter(~expr("prod_exp_dt RLIKE '^[0-9]{8}$'"))
df_invalid_date_format.show(5, truncate=False)
assert df_invalid_date_format.count() == 0, "There are records with incorrect 'prod_exp_dt' format"
