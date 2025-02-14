-- Data Quality Checks for d_product Table
-- SQL Tests for Null Checks and Date Format Validity

-- Setup Phase
-- Loading the Databricks SQL table configurations
-- Ensure the table d_product is properly configured in the 'purgo_playground' catalog.

-- Scenario: Verify item_nbr is not null
-- SQL to count records where item_nbr is null
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records with null item_nbr
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Scenario: Verify sellable_qty is not null
-- SQL to count records where sellable_qty is null
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records with null sellable_qty
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Scenario: Validate prod_exp_dt format
-- SQL to count records where prod_exp_dt is not in 'yyyyMMdd' format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records with incorrect prod_exp_dt format
SELECT *
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;
