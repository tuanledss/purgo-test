/* Framework and Structure - SQL Testing for Data Quality Checks on d_product Table */

/* Check for NULL values in 'item_nbr' column */
SELECT COUNT(*) AS null_item_nbr_count 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

/* Check for NULL values in 'sellable_qty' column */
SELECT COUNT(*) AS null_sellable_qty_count 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

/* Validate 'prod_exp_dt' format to ensure it is in 'yyyymmdd' */
SELECT COUNT(*) AS invalid_prod_exp_dt_count 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^[0-9]{8}$');

SELECT * 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^[0-9]{8}$') 
LIMIT 5;

/* Clean up operations - Uncomment if a temporary table was created for testing 
-- DROP TABLE IF EXISTS purgo_playground.some_temp_table;

/* Integration tests - Ensure transformations are applied correctly followed by validation checks */
/*
INSERT INTO purgo_playground.transformed_data
SELECT ... -- Placeholder for sample data transformation logic
*/

/* Perform validation against this transformed data to verify correctness */

/* Logging messages for validation scenarios */
-- Example: log outputs in a Databricks notebook
-- print("Found records with null 'item_nbr'")
-- print("Found records with null 'sellable_qty'")
-- print("Found records with invalid 'prod_exp_dt' format")

/* Performance testing and optimization - Evaluate and optimize query performance */
/* Analyze query execution plans to identify potential optimization opportunities */

/* Further Data Quality Checks */
/* Extend checks to other columns or additional business rules as needed */

