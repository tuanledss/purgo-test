/* 
  Data Quality Check for d_product Table
  This script checks for null values in 'item_nbr' and 'sellable_qty' columns,
  and validates the 'prod_exp_dt' column for correct date format (yyyymmdd).
  The script outputs counts and sample records for each check.
*/

/* Check for NULL values in 'item_nbr' */
-- Count records where 'item_nbr' is NULL
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records where 'item_nbr' is NULL
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/* Check for NULL values in 'sellable_qty' */
-- Count records where 'sellable_qty' is NULL
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records where 'sellable_qty' is NULL
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* Validate 'prod_exp_dt' format */
-- Count records where 'prod_exp_dt' is not in yyyymmdd format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records where 'prod_exp_dt' is not in yyyymmdd format
SELECT *
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;



# PySpark Test Code for Data Quality Checks

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality Checks") \
    .getOrCreate()

# Define schema for d_product table
schema = StructType([
    StructField("prod_id", StringType(), True),
    StructField("item_nbr", StringType(), True),
    StructField("sellable_qty", DoubleType(), True),
    StructField("prod_exp_dt", StringType(), True)
])

# Load data into DataFrame
d_product_df = spark.read.format("delta").schema(schema).load("/path/to/d_product")

# Check for NULL values in 'item_nbr'
null_item_nbr_count = d_product_df.filter(col("item_nbr").isNull()).count()
assert null_item_nbr_count == 0, f"Found {null_item_nbr_count} records with NULL 'item_nbr'"

# Display 5 sample records where 'item_nbr' is NULL
d_product_df.filter(col("item_nbr").isNull()).show(5)

# Check for NULL values in 'sellable_qty'
null_sellable_qty_count = d_product_df.filter(col("sellable_qty").isNull()).count()
assert null_sellable_qty_count == 0, f"Found {null_sellable_qty_count} records with NULL 'sellable_qty'"

# Display 5 sample records where 'sellable_qty' is NULL
d_product_df.filter(col("sellable_qty").isNull()).show(5)

# Validate 'prod_exp_dt' format
invalid_prod_exp_dt_count = d_product_df.filter(~col("prod_exp_dt").rlike("^[0-9]{8}$")).count()
assert invalid_prod_exp_dt_count == 0, f"Found {invalid_prod_exp_dt_count} records with invalid 'prod_exp_dt' format"

# Display 5 sample records where 'prod_exp_dt' is not yyyymmdd format
d_product_df.filter(~col("prod_exp_dt").rlike("^[0-9]{8}$")).show(5)

# Stop Spark session
spark.stop()
