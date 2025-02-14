from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    DecimalType
)

# Create Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define schema for the test data
schema = StructType([
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

# Generate diverse test data
data = [
    # Happy Path - Valid entries
    ("prod1", "item001", 12.5, 20240321, 3.5, "Addr1", "Loc1", "Line1", "Type1", 15, 5.0, "tracker001", "100", "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC1", "Entity1"),
    ("prod2", "item002", 20.7, 20240322, 4.1, "Addr2", "Loc2", "Line2", "Type2", 10, 8.0, "tracker002", "200", "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC2", "Entity1"),
    # Edge Cases - Boundary Values
    ("prod3", "item003", 5.0, 20240101, 2.0, "Addr3", "Loc3", "Line3", "Type3", 0, 0.0, "tracker003", "0", "Y", "2024-01-01T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC3", "Entity2"),
    # NULL Scenarios
    (None, "item004", 15.0, None, 1.0, None, "Loc4", "Line4", None, None, None, "tracker004", "500", "Y", "2024-02-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", None, "Entity3"),
    # Invalid Data
    ("prod5", "item005", -5.0, 20241231, 5.5, "Addr5", "Loc5", "Line5", "Type5", -10, -2.0, "tracker005", "-50", "N", "2025-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC5", "Entity5"),
    # Special Characters
    ("prod6", "item006_special_!@#", 18.4, 20240325, 3.9, "Addr&*^$", "Lo?<>(:)", "Line6{}[]", "Type6~`", 20, 7.5, "tracker006_^^", "300", "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC6", "Entity6"),
    # Multi-byte Characters
    ("prod7", "item007_日本語", 19.3, 20240401, 4.7, "Addr日本語", "Loc日本語", "Line7日本語", "Type7", 12, 6.3, "tracker007_日本語", "120", "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC7", "Entity7"),
    # More diverse examples
    # ... add up to 20-30 records as needed for thorough testing
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display the DataFrame
df.show(truncate=False)

# SQL data quality checks
# Check if 'item_nbr' or 'sellable_qty' is NULL and get the counts and sample records
df.createOrReplaceTempView("d_product")

# Check 'item_nbr' is not null
spark.sql("""
SELECT COUNT(*) AS null_item_nbr_count FROM d_product WHERE item_nbr IS NULL
""").show()

spark.sql("""
SELECT * FROM d_product WHERE item_nbr IS NULL LIMIT 5
""").show()

# Check 'sellable_qty' is not null
spark.sql("""
SELECT COUNT(*) AS null_sellable_qty_count FROM d_product WHERE sellable_qty IS NULL
""").show()

spark.sql("""
SELECT * FROM d_product WHERE sellable_qty IS NULL LIMIT 5
""").show()

# Check 'prod_exp_dt' is not in 'yyyymmdd' format
spark.sql("""
SELECT COUNT(*) AS invalid_prod_exp_dt_count FROM d_product WHERE NOT prod_exp_dt RLIKE '^\d{8}$'
""").show()

spark.sql("""
SELECT * FROM d_product WHERE NOT prod_exp_dt RLIKE '^\d{8}$' LIMIT 5
""").show()

