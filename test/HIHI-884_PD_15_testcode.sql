/* 
   Data Quality Check Test Code: purgo_playground.d_product Table
   Ensure proper data quality validations for the columns: item_nbr, sellable_qty, prod_exp_dt
   using Databricks SQL.
*/

-- Test to validate 'item_nbr' is NOT NULL
-- Check count of records with 'item_nbr' as NULL and display 5 sample records
SELECT 
  COUNT(*) AS null_item_nbr_count 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

-- Test to validate 'sellable_qty' is NOT NULL
-- Check count of records with 'sellable_qty' as NULL and display 5 sample records
SELECT 
  COUNT(*) AS null_sellable_qty_count 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

-- Test to validate 'prod_exp_dt' format as YYYYMMDD
-- Check count of records with invalid 'prod_exp_dt' format and display 5 sample records
SELECT 
  COUNT(*) AS invalid_prod_exp_dt_count 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$');

SELECT * 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$') 
LIMIT 5;
