-- Use the Databricks SQL interface for the checks
-- Ensure the Unity Catalog is specified correctly

/* Data Quality Check: Validate non-null and format conditions for d_product table */

-- Check for NULL item_nbr and display sample records
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Retrieve and show 5 sample records with NULL item_nbr
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Check for NULL sellable_qty and display sample records
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Retrieve and show 5 sample records with NULL sellable_qty
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Check for incorrect prod_exp_dt format and display sample records
-- Using a regular expression to match YYYYMMDD format precisely
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt REGEXP '^[12][0-9]{3}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$';

-- Retrieve and show 5 sample records with incorrect prod_exp_dt
SELECT *
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt REGEXP '^[12][0-9]{3}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$'
LIMIT 5;

/* Cleanup Operations */
/* If there are temporary tables or views created for testing purposes, ensure they are dropped here */
-- DROP VIEW IF EXISTS temp_view_name;
