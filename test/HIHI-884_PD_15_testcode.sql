-- SQL Test Code for Data Quality Checks in Databricks Environment

-- /* --------------------------------------------------------------
-- Section: Setup Instructions
-- Note: Ensure Unity Catalog is configured correctly.
--       This code assumes you are using Databricks SQL syntax and
--       Databricks-native features like Delta Lake.
--       Use Unity Catalog for schema reference: "purgo_playground.d_product"
--          -------------------------------------------------------------- */

-- /* Check for NULL item_nbr */

-- Retrieve count of records with NULL 'item_nbr'
WITH null_item_nbr_check AS (
  SELECT COUNT(*) AS null_item_nbr_count
  FROM purgo_playground.d_product
  WHERE item_nbr IS NULL
)
SELECT null_item_nbr_count FROM null_item_nbr_check;

-- Display 5 sample records with NULL 'item_nbr'
SELECT *
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL
LIMIT 5;

-- /* Check for NULL sellable_qty */

-- Retrieve count of records with NULL 'sellable_qty'
WITH null_sellable_qty_check AS (
  SELECT COUNT(*) AS null_sellable_qty_count
  FROM purgo_playground.d_product
  WHERE sellable_qty IS NULL
)
SELECT null_sellable_qty_count FROM null_sellable_qty_check;

-- Display 5 sample records with NULL 'sellable_qty'
SELECT *
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL
LIMIT 5;

-- /* Check for incorrect 'prod_exp_dt' format */

-- Retrieve count of records with incorrect 'prod_exp_dt' format
WITH invalid_date_format_check AS (
  SELECT COUNT(*) AS invalid_prod_exp_dt_count
  FROM purgo_playground.d_product
  WHERE NOT REGEXP_LIKE(prod_exp_dt, '^[0-9]{8}$')
)
SELECT invalid_prod_exp_dt_count FROM invalid_date_format_check;

-- Display 5 sample records with incorrect 'prod_exp_dt' format
SELECT *
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(prod_exp_dt, '^[0-9]{8}$')
LIMIT 5;

-- /* Clean-up operations such as logging errors or storing results for audit can be added here. */
