/* 
   Data Quality Check for d_product Table
   This script checks for null values in 'item_nbr' and 'sellable_qty' columns,
   and validates the 'prod_exp_dt' format in the 'd_product' table.
   The table is located in the purgo_playground schema under Unity Catalog.
*/

/* Check for null values in 'item_nbr' */
-- Get count of records where 'item_nbr' is null
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records where 'item_nbr' is null
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/* Check for null values in 'sellable_qty' */
-- Get count of records where 'sellable_qty' is null
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records where 'sellable_qty' is null
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* Validate 'prod_exp_dt' format */
-- Get count of records where 'prod_exp_dt' is not in 'yyyymmdd' format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records where 'prod_exp_dt' is not in 'yyyymmdd' format
SELECT *
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;

