import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DecimalType, ArrayType, MapType

class TestPurgoPlayground(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("Test Purgo Playground").getOrCreate()

    def test_happy_path_test_data(self):
        happy_path_df = self.spark.table("purgo_playground.happy_path_test_data")
        self.assertEqual(happy_path_df.count(), 5)
        self.assertEqual(happy_path_df.schema, StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("double_value", DoubleType(), True),
            StructField("decimal_value", DecimalType(10, 2), True),
            StructField("array_value", ArrayType(StringType()), True),
            StructField("map_value", MapType(StringType(), StringType()), True)
        ]))

    def test_edge_cases_test_data(self):
        edge_cases_df = self.spark.table("purgo_playground.edge_cases_test_data")
        self.assertEqual(edge_cases_df.count(), 5)
        self.assertEqual(edge_cases_df.schema, StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("double_value", DoubleType(), True),
            StructField("decimal_value", DecimalType(10, 2), True),
            StructField("array_value", ArrayType(StringType()), True),
            StructField("map_value", MapType(StringType(), StringType()), True)
        ]))

    def test_error_cases_test_data(self):
        error_cases_df = self.spark.table("purgo_playground.error_cases_test_data")
        self.assertEqual(error_cases_df.count(), 5)
        self.assertEqual(error_cases_df.schema, StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("double_value", DoubleType(), True),
            StructField("decimal_value", DecimalType(10, 2), True),
            StructField("array_value", ArrayType(StringType()), True),
            StructField("map_value", MapType(StringType(), StringType()), True)
        ]))

    def test_data_type_conversions(self):
        happy_path_df = self.spark.table("purgo_playground.happy_path_test_data")
        self.assertEqual(happy_path_df.select(F.col("double_value").cast("string")).first().asDict()["double_value"], "10.5")
        self.assertEqual(happy_path_df.select(F.col("decimal_value").cast("string")).first().asDict()["decimal_value"], "10.50")

    def test_null_handling(self):
        edge_cases_df = self.spark.table("purgo_playground.edge_cases_test_data")
        self.assertEqual(edge_cases_df.select(F.col("name")).filter(F.col("name").isNull()).count(), 1)
        self.assertEqual(edge_cases_df.select(F.col("double_value")).filter(F.col("double_value").isNull()).count(), 1)
        self.assertEqual(edge_cases_df.select(F.col("decimal_value")).filter(F.col("decimal_value").isNull()).count(), 1)
        self.assertEqual(edge_cases_df.select(F.col("array_value")).filter(F.col("array_value").isNull()).count(), 1)
        self.assertEqual(edge_cases_df.select(F.col("map_value")).filter(F.col("map_value").isNull()).count(), 1)

    def test_delta_lake_operations(self):
        happy_path_df = self.spark.table("purgo_playground.happy_path_test_data")
        happy_path_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.happy_path_test_data_delta")
        self.assertEqual(self.spark.table("purgo_playground.happy_path_test_data_delta").count(), 5)

    def test_merge_update_delete_operations(self):
        happy_path_df = self.spark.table("purgo_playground.happy_path_test_data")
        happy_path_df.createOrReplaceTempView("happy_path_temp")
        self.spark.sql("MERGE INTO purgo_playground.happy_path_test_data AS target USING happy_path_temp AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = 'John Doe Updated'")
        self.assertEqual(self.spark.table("purgo_playground.happy_path_test_data").filter(F.col("name") == "John Doe Updated").count(), 1)
        self.spark.sql("DELETE FROM purgo_playground.happy_path_test_data WHERE id = '1'")
        self.assertEqual(self.spark.table("purgo_playground.happy_path_test_data").filter(F.col("id") == "1").count(), 0)

    def test_window_functions(self):
        happy_path_df = self.spark.table("purgo_playground.happy_path_test_data")
        window_df = happy_path_df.select(F.row_number().over(F.orderBy(F.col("id"))).alias("row_num"), F.col("id"), F.col("name"))
        self.assertEqual(window_df.filter(F.col("row_num") == 1).first().asDict()["id"], "1")

    def tearDown(self):
        self.spark.stop()

if __name__ == "__main__":
    unittest.main()



-- Test data type conversions
SELECT CAST(double_value AS STRING) FROM purgo_playground.happy_path_test_data;
SELECT CAST(decimal_value AS STRING) FROM purgo_playground.happy_path_test_data;

-- Test null handling
SELECT * FROM purgo_playground.edge_cases_test_data WHERE name IS NULL;
SELECT * FROM purgo_playground.edge_cases_test_data WHERE double_value IS NULL;
SELECT * FROM purgo_playground.edge_cases_test_data WHERE decimal_value IS NULL;
SELECT * FROM purgo_playground.edge_cases_test_data WHERE array_value IS NULL;
SELECT * FROM purgo_playground.edge_cases_test_data WHERE map_value IS NULL;

-- Test Delta Lake operations
CREATE TABLE purgo_playground.happy_path_test_data_delta USING delta AS SELECT * FROM purgo_playground.happy_path_test_data;
SELECT * FROM purgo_playground.happy_path_test_data_delta;

-- Test merge, update, delete operations
MERGE INTO purgo_playground.happy_path_test_data AS target
USING (SELECT * FROM purgo_playground.happy_path_test_data WHERE id = '1') AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.name = 'John Doe Updated';
SELECT * FROM purgo_playground.happy_path_test_data WHERE id = '1';
DELETE FROM purgo_playground.happy_path_test_data WHERE id = '1';
SELECT * FROM purgo_playground.happy_path_test_data WHERE id = '1';

-- Test window functions
SELECT row_number() OVER (ORDER BY id) AS row_num, id, name FROM purgo_playground.happy_path_test_data;
SELECT * FROM (
  SELECT row_number() OVER (ORDER BY id) AS row_num, id, name 
  FROM purgo_playground.happy_path_test_data
) WHERE row_num = 1;
