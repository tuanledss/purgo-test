-- SQL tests for data quality checks in Databricks environment

/* 
   Setup and configuration 
   This section handles any necessary setup before running tests 
*/

-- Use correct catalog and schema
-- Ensure the catalog name is correctly referenced as per your environment setup.
USE CATALOG purgo_playground;

/* 
   Data Quality Check for 'item_nbr' column 
   Verify that 'item_nbr' is not null
*/

-- Get count of records with null 'item_nbr'
SELECT 
  COUNT(*) AS null_item_nbr_count 
FROM 
  d_product 
WHERE 
  item_nbr IS NULL;

-- Display 5 sample records where 'item_nbr' is null
SELECT 
  * 
FROM 
  d_product 
WHERE 
  item_nbr IS NULL 
LIMIT 5;

/* 
   Data Quality Check for 'sellable_qty' column 
   Verify that 'sellable_qty' is not null
*/

-- Get count of records with null 'sellable_qty'
SELECT 
  COUNT(*) AS null_sellable_qty_count 
FROM 
  d_product 
WHERE 
  sellable_qty IS NULL;

-- Display 5 sample records where 'sellable_qty' is null
SELECT 
  * 
FROM 
  d_product 
WHERE 
  sellable_qty IS NULL 
LIMIT 5;

/* 
   Data Quality Check for 'prod_exp_dt' column 
   Validate that 'prod_exp_dt' is in the 'yyyyMMdd' format
*/

-- Get count of records with incorrect 'prod_exp_dt' format
SELECT 
  COUNT(*) AS invalid_prod_exp_dt_count 
FROM 
  d_product 
WHERE 
  prod_exp_dt NOT REGEXP '^[0-9]{8}$';

-- Display 5 sample records where 'prod_exp_dt' is not in 'yyyyMMdd' format
SELECT 
  * 
FROM 
  d_product 
WHERE 
  prod_exp_dt NOT REGEXP '^[0-9]{8}$' 
LIMIT 5;

/* 
   Cleanup operations 
   Ensure no temporary data or tables persist after testing
*/

-- Example of cleanup operation (uncomment if temporary tables/views were created)
-- DROP TABLE IF EXISTS temp_table_name;
