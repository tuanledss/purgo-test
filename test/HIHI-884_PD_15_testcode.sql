/* Test Code for Data Quality Check on the 'd_product' Table */

/* Validate 'item_nbr' is not null */
-- Getting count of 'item_nbr' null records
SELECT COUNT(*) AS null_item_nbr_count 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

-- Displaying 5 sample records where 'item_nbr' is null
SELECT * 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

/* Validate 'sellable_qty' is not null */
-- Getting count of 'sellable_qty' null records
SELECT COUNT(*) AS null_sellable_qty_count 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

-- Displaying 5 sample records where 'sellable_qty' is null
SELECT * 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

/* Validate 'prod_exp_dt' format is yyyymmdd */
-- Getting count of records where 'prod_exp_dt' is not in yyyymmdd format
SELECT COUNT(*) AS invalid_prod_exp_dt_count 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$');

-- Displaying 5 sample records where 'prod_exp_dt' format is invalid
SELECT * 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$') 
LIMIT 5;

