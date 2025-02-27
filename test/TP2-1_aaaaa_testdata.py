from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DecimalType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Test Data Generation") \
    .getOrCreate()

# Define schema for inventory and sales datasets
inv_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("inv_loc", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("net_qty", DoubleType(), True),
    StructField("expired_qt", DecimalType(38, 0), True),
    StructField("item_nbr", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("um_rate", DoubleType(), True),
    StructField("plant_loc_cd", StringType(), True),
    StructField("inv_stock_reference", StringType(), True),
    StructField("stock_type", StringType(), True),
    StructField("qty_on_hand", DoubleType(), True),
    StructField("qty_shipped", DoubleType(), True),
    StructField("flag_active", StringType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True)
])

sales_schema = StructType([
    StructField("ref_txn_id", StringType(), True),
    StructField("item_nbr", StringType(), True),
    StructField("qty_sold", DoubleType(), True),
    StructField("order_qty", DoubleType(), True),
    StructField("flag_cancel", StringType(), True),
    StructField("cancel_qty", DoubleType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True)
])

# Create test data for f_inv_movmnt
inv_data = [
    ("txn001", "loc1", 100.0, 90.0, 10, "item1", 50.5, 1.0, "PLANT1", "REF1", "STOCK", 100.0, 5.0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("txn002", "loc2", 200.0, 190.0, 0, "item2", 60.0, 1.1, "PLANT2", "REF2", "STOCK", 150.0, 10.0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Edge case: NULL values
    (None, "loc3", 0.0, -5.0, -1, "item3", None, 0.0, "PLANT3", None, "STOCK", 0.0, 0.0, "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Error case: invalid negative financial quantity
    ("txn004", "loc4", -20.0, -15.0, 5, "item4", 70.5, 1.2, "PLANT4", "REF4", "STOCK", 200.0, 15.0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Special chars in item_nbr
    ("txn005", "loc5", 300.0, 290.0, 10, "item_special_√", 80.5, 1.3, "PLANT5", "REF5", "STOCK", 250.0, 20.0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000")
]

# Create test data for f_sales
sales_data = [
    ("ref_txn001", "item1", 30.0, 40.0, "N", 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("ref_txn002", "item2", 50.0, 60.0, "N", 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Edge case: zero qty_sold with non-zero order_qty
    ("ref_txn003", "item3", 0.0, 50.0, "N", 10.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Error case: negative qty_sold
    ("ref_txn004", "item4", -10.0, 40.0, "N", 5.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Special chars in item_nbr
    ("ref_txn005", "item_special_√", 70.0, 80.0, "Y", 20.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000")
]

# Create DataFrames
inv_df = spark.createDataFrame(inv_data, schema=inv_schema)
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Show DataFrame
inv_df.show()
sales_df.show()

