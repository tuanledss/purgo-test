/*
Data Quality Check on d_product Table

This SQL code performs data quality checks on the d_product table
as specified in the requirements.
*/

-- Check for null values in item_nbr and display 5 sample records if null
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Check for null values in sellable_qty and display 5 sample records if null
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Check for invalid prod_exp_dt format (not yyyymmdd) and display 5 sample records if invalid
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NULL OR length(cast(prod_exp_dt as string)) != 8;

SELECT *
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NULL OR length(cast(prod_exp_dt as string)) != 8
LIMIT 5;



