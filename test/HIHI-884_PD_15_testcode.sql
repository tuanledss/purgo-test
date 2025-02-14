-- SQL logic for data quality checks on the d_product table in purgo_playground catalog

/* 
   Verify item_nbr is not null 
   - Retrieve the count of records with null item_nbr
   - Display 5 sample records with null item_nbr 
*/
SELECT COUNT(*) AS count_null_item_nbr
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/* 
   Verify sellable_qty is not null 
   - Retrieve the count of records with null sellable_qty
   - Display 5 sample records with null sellable_qty 
*/
SELECT COUNT(*) AS count_null_sellable_qty
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* 
   Validate prod_exp_dt format 
   - Retrieve the count of records with incorrect prod_exp_dt format 
   - Display 5 sample records with incorrect prod_exp_dt format
   - Valid format: YYYYMMDD 
*/
SELECT COUNT(*) AS count_invalid_prod_exp_dt
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NULL OR prod_exp_dt NOT REGEXP '^[0-9]{8}$';

SELECT *
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NULL OR prod_exp_dt NOT REGEXP '^[0-9]{8}$'
LIMIT 5;
