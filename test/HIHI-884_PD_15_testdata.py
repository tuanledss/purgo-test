from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, TimestampType, IntegerType, LongType

# Create SparkSession
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for d_product table
d_product_schema = StructType([
    StructField("prod_id", StringType(), True),
    StructField("item_nbr", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("prod_exp_dt", DecimalType(38, 0), True),
    StructField("cost_per_pkg", DoubleType(), True),
    StructField("plant_add", StringType(), True),
    StructField("plant_loc_cd", StringType(), True),
    StructField("prod_line", StringType(), True),
    StructField("stock_type", StringType(), True),
    StructField("pre_prod_days", DoubleType(), True),
    StructField("sellable_qty", DoubleType(), True),
    StructField("prod_ordr_tracker_nbr", StringType(), True),
    StructField("max_order_qty", StringType(), True),
    StructField("flag_active", StringType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True),
    StructField("src_sys_cd", StringType(), True),
    StructField("hfm_entity", StringType(), True)
])

# Generate test data for d_product table
d_product_data = [
    ("prod1", "item1", 10.0, 20220101, 5.0, "plant1", "loc1", "line1", "stock1", 10.0, 100.0, "tracker1", "max1", "Y", "2022-01-01T00:00:00.000+0000", "2022-01-01T00:00:00.000+0000", "src1", "hfm1"),
    ("prod2", None, 20.0, 20220201, 10.0, "plant2", "loc2", "line2", "stock2", 20.0, 200.0, "tracker2", "max2", "N", "2022-02-01T00:00:00.000+0000", "2022-02-01T00:00:00.000+0000", "src2", "hfm2"),
    ("prod3", "item3", None, 20220301, 15.0, "plant3", "loc3", "line3", "stock3", 30.0, 300.0, "tracker3", "max3", "Y", "2022-03-01T00:00:00.000+0000", "2022-03-01T00:00:00.000+0000", "src3", "hfm3"),
    # Add more test data as needed
]

# Create DataFrame for d_product table
d_product_df = spark.createDataFrame(d_product_data, schema=d_product_schema)

# Display test data
d_product_df.show()

# Define schema for d_product_revenue table
d_product_revenue_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("country", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("purchased_date", StringType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("invoice_number", StringType(), True),
    StructField("is_returned", LongType(), True),
    StructField("customer_satisfaction_score", LongType(), True),
    StructField("product_details", StringType(), True),
    StructField("customer_first_purchased_date", StringType(), True),
    StructField("customer_first_product", StringType(), True),
    StructField("customer_first_revenue", DoubleType(), True)
])

# Generate test data for d_product_revenue table
d_product_revenue_data = [
    (1, "product1", "type1", 100, "country1", "customer1", "2022-01-01", "2022-01-01", "invoice1", 0, 5, "details1", "2022-01-01", "product1", 100.0),
    (2, "product2", "type2", 200, "country2", "customer2", "2022-02-01", "2022-02-01", "invoice2", 1, 4, "details2", "2022-02-01", "product2", 200.0),
    (3, "product3", "type3", 300, "country3", "customer3", "2022-03-01", "2022-03-01", "invoice3", 0, 5, "details3", "2022-03-01", "product3", 300.0),
    # Add more test data as needed
]

# Create DataFrame for d_product_revenue table
d_product_revenue_df = spark.createDataFrame(d_product_revenue_data, schema=d_product_revenue_schema)

# Display test data
d_product_revenue_df.show()

# Perform data quality checks
# Check for null values in item_nbr column
null_item_nbr_count = d_product_df.filter(d_product_df.item_nbr.isNull()).count()
print(f"Number of rows with null item_nbr: {null_item_nbr_count}")

# Display sample records with null item_nbr
null_item_nbr_samples = d_product_df.filter(d_product_df.item_nbr.isNull()).limit(5)
null_item_nbr_samples.show()

# Check for null values in sellable_qty column
null_sellable_qty_count = d_product_df.filter(d_product_df.sellable_qty.isNull()).count()
print(f"Number of rows with null sellable_qty: {null_sellable_qty_count}")

# Display sample records with null sellable_qty
null_sellable_qty_samples = d_product_df.filter(d_product_df.sellable_qty.isNull()).limit(5)
null_sellable_qty_samples.show()

# Check for invalid prod_exp_dt format
invalid_prod_exp_dt_count = d_product_df.filter(~d_product_df.prod_exp_dt.rlike("^\\d{8}$")).count()
print(f"Number of rows with invalid prod_exp_dt format: {invalid_prod_exp_dt_count}")

# Display sample records with invalid prod_exp_dt format
invalid_prod_exp_dt_samples = d_product_df.filter(~d_product_df.prod_exp_dt.rlike("^\\d{8}$")).limit(5)
invalid_prod_exp_dt_samples.show()
