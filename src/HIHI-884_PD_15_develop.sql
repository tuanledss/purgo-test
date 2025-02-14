/* SQL Queries for Data Quality Checks on d_product Table */

/* Check for NULL values in 'item_nbr' column and display count and sample records */
SELECT COUNT(*) AS null_item_nbr_count 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

/* Check for NULL values in 'sellable_qty' column and display count and sample records */
SELECT COUNT(*) AS null_sellable_qty_count 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

SELECT * 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

/* Validate 'prod_exp_dt' format to ensure it is in 'yyyymmdd' and display count and sample records */
SELECT COUNT(*) AS invalid_prod_exp_dt_count 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^[0-9]{8}$');

SELECT * 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^[0-9]{8}$') 
LIMIT 5;
