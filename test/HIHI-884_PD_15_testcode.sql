/* 
   Data Quality Check Test Script for `d_product` Table:
   This script is designed to validate critical data quality aspects of
   the `d_product` table in the `purgo_playground` schema.
*/

-- Test Setup: Create a temporary test environment by using a view or table
CREATE OR REPLACE TEMPORARY VIEW test_data AS
SELECT * FROM purgo_playground.d_product;

-- Validate 'item_nbr' is NOT NULL
-- Count the number of records where 'item_nbr' is NULL
SELECT 
  COUNT(*) AS null_item_nbr_count
FROM test_data
WHERE item_nbr IS NULL;

-- Retrieve 5 sample records where 'item_nbr' is NULL
SELECT * FROM test_data 
WHERE item_nbr IS NULL LIMIT 5;

-- Validate 'sellable_qty' is NOT NULL
-- Count the number of records where 'sellable_qty' is NULL
SELECT 
  COUNT(*) AS null_sellable_qty_count
FROM test_data
WHERE sellable_qty IS NULL;

-- Retrieve 5 sample records where 'sellable_qty' is NULL
SELECT * FROM test_data 
WHERE sellable_qty IS NULL LIMIT 5;

-- Validate 'prod_exp_dt' format is YYYYMMDD
-- Count the number of records where 'prod_exp_dt' does not match YYYYMMDD format
SELECT 
  COUNT(*) AS invalid_prod_exp_dt_count
FROM test_data
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$');

-- Retrieve 5 sample records where 'prod_exp_dt' format is invalid
SELECT * FROM test_data 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$') LIMIT 5;

/* 
   Additional cleanup operations if required.
   This section can include dropping temporary views or tables created for testing.
-- Drop the temporary view used for testing
DROP VIEW IF EXISTS test_data;
*/
