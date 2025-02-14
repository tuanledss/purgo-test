/* Test Code for Data Quality Checks on d_product Table */

/* Framework and Structure */
/* The following code uses Databricks SQL syntax to perform data quality checks */

/* Data Type Testing */
/* Validate NULL values and string format for specified columns */

-- Scenario: Verify item_nbr is not null
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records with null item_nbr
SELECT * 
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Scenario: Verify sellable_qty is not null
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records with null sellable_qty
SELECT * 
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Scenario: Validate prod_exp_dt format (should be YYYYMMDD)
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records with incorrect prod_exp_dt format
SELECT * 
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;

/* Databricks-Specific Features */
/* As the checks involve basic quality validations, Delta Lake operations, specific window functions, and analytics features are not required here */

/* Code Structure and Documentation */
-- This block of SQL code is designed to perform data quality checks
-- It checks for null values in crucial columns and validates date format
-- Each query block performs specific checks and displays results accordingly
