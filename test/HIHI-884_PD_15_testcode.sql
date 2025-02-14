-- SQL code for Data Quality Checks using Databricks SQL syntax

-- Setup: Ensure that the environment has access to the Unity Catalog and the specified catalog and schema for d_product
-- USE CATALOG purgo_playground;
-- USE SCHEMA default;

-- Check for NULL item_nbr
-- Display the count of records where 'item_nbr' is NULL
SELECT COUNT(*) AS null_item_nbr_count
FROM d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records where 'item_nbr' is NULL
SELECT *
FROM d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Check for NULL sellable_qty
-- Display the count of records where 'sellable_qty' is NULL
SELECT COUNT(*) AS null_sellable_qty_count
FROM d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records where 'sellable_qty' is NULL
SELECT *
FROM d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Check for incorrect prod_exp_dt format (not in YYYYMMDD)
-- Display the count of records where 'prod_exp_dt' is not in the correct format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records where 'prod_exp_dt' is not in the correct format
SELECT *
FROM d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;
