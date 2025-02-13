-- Define the catalog and schema to be used
USE purgo_playground;

-- Query to check for NULL item_nbr
/* 
  Scenario: Verify item_nbr is not null 
  This scenario checks for missing values in the 'item_nbr' column.
*/

-- Get count of records with NULL item_nbr
SELECT COUNT(*) AS null_item_nbr_count
FROM d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records with NULL item_nbr
SELECT *
FROM d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Query to check for NULL sellable_qty
/* 
  Scenario: Verify sellable_qty is not null 
  This scenario checks for missing values in the 'sellable_qty' column.
*/

-- Get count of records with NULL sellable_qty
SELECT COUNT(*) AS null_sellable_qty_count
FROM d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records with NULL sellable_qty
SELECT *
FROM d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Query to check prod_exp_dt format
/* 
  Scenario: Validate prod_exp_dt format 
  This scenario checks for incorrect date format in 'prod_exp_dt' column.
  The expected format is YYYYMMDD without any delimiters.
*/

-- Get count of records with incorrect prod_exp_dt format
SELECT COUNT(*) AS invalid_date_format_count
FROM d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records with incorrect prod_exp_dt format
SELECT *
FROM d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;
