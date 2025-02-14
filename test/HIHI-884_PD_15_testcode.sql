/* SQL Logic to perform data quality checks on the d_product table before moving to production */
/* Ensure proper data validation and integrity checks */

/* Unity Catalog Configuration */
-- USE CATALOG purgo_playground;

/* Scenario: Verify item_nbr is not null */
-- Count records where item_nbr is NULL
SELECT
  COUNT(*) AS null_item_nbr_count
FROM
  d_product
WHERE
  item_nbr IS NULL;

/* Display 5 sample records with null item_nbr */
SELECT
  *
FROM
  d_product
WHERE
  item_nbr IS NULL
LIMIT 5;

/* Scenario: Verify sellable_qty is not null */
-- Count records where sellable_qty is NULL
SELECT
  COUNT(*) AS null_sellable_qty_count
FROM
  d_product
WHERE
  sellable_qty IS NULL;

/* Display 5 sample records with null sellable_qty */
SELECT
  *
FROM
  d_product
WHERE
  sellable_qty IS NULL
LIMIT 5;

/* Scenario: Validate prod_exp_dt format */
-- Count records with invalid prod_exp_dt format not matching YYYYMMDD
SELECT
  COUNT(*) AS invalid_prod_exp_dt_format_count
FROM
  d_product
WHERE
  prod_exp_dt NOT REGEXP '^[0-9]{8}$';

-- Display 5 sample records with incorrect prod_exp_dt format
SELECT
  *
FROM
  d_product
WHERE
  prod_exp_dt NOT REGEXP '^[0-9]{8}$'
LIMIT 5;
