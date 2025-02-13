/* 
   -- SQL Query: Validate Data Quality in d_product Table 
   -- Ensure item_nbr and sellable_qty are NOT NULL 
   -- Validate prod_exp_dt for correct 'yyyyMMdd' format 
*/

-- Count of records where item_nbr is NULL
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Sample 5 records where item_nbr is NULL
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Count of records where sellable_qty is NULL
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Sample 5 records where sellable_qty is NULL
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Count of records where prod_exp_dt is not in 'yyyyMMdd' format
SELECT COUNT(*) AS invalid_prod_exp_dt_format_count
FROM purgo_playground.d_product
WHERE LENGTH(prod_exp_dt) <> 8 OR NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Sample 5 records where prod_exp_dt is not in 'yyyyMMdd' format
SELECT *
FROM purgo_playground.d_product
WHERE LENGTH(prod_exp_dt) <> 8 OR NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;
