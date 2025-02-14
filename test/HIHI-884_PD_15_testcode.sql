/* 
  Data Quality Check SQL Logic for d_product Table 
  This SQL script performs mandatory data quality checks on the d_product table,
  ensuring critical fields are not null and date formats are validated.
  Applicable to Databricks SQL environment.
  Place any environment-specific configurations above this comment block.
*/

/* 
  Checking records with NULL 'item_nbr'
  - Retrieve the count of records where 'item_nbr' is NULL
  - Display 5 sample records with NULL 'item_nbr'
*/
-- Count of records with NULL 'item_nbr'
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Sample records with NULL 'item_nbr'
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/*
  Checking records with NULL 'sellable_qty'
  - Retrieve the count of records where 'sellable_qty' is NULL
  - Display 5 sample records with NULL 'sellable_qty'
*/
-- Count of records with NULL 'sellable_qty'
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Sample records with NULL 'sellable_qty'
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/*
  Validating 'prod_exp_dt' format
  - Retrieve the count of records where 'prod_exp_dt' is not in 'yyyyMMdd' format
  - Display 5 sample records with incorrect 'prod_exp_dt' format
*/
-- Count of records with incorrect 'prod_exp_dt' format
SELECT COUNT(*) AS incorrect_prod_exp_dt_format_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Sample records with incorrect 'prod_exp_dt' format
SELECT *
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;

/*
  Cleanup operations
  Include this section if any temporary tables/views or resources were used
  Ensure proper disposal to maintain environment integrity
*/
