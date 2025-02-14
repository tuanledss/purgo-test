-- Databricks SQL Test Code for Data Quality Checks on d_product Table

-- /* =============================================================== */
-- /* Setup Section: */
-- /* Ensure to use the appropriate catalog and schema */
-- /* =============================================================== */

USE CATALOG purgo_playground;
USE SCHEMA purgo_playground;

-- /* =============================================================== */
-- /* Test: Verify item_nbr is not null */
-- /* =============================================================== */

-- Check for records where item_nbr is null
WITH null_item_nbr AS (
  SELECT * FROM d_product WHERE item_nbr IS NULL
)
SELECT COUNT(*) AS null_item_nbr_count
FROM null_item_nbr;

-- Retrieve 5 sample records where item_nbr is null
SELECT * FROM null_item_nbr LIMIT 5;

-- /* =============================================================== */
-- /* Test: Verify sellable_qty is not null */
-- /* =============================================================== */

-- Check for records where sellable_qty is null
WITH null_sellable_qty AS (
  SELECT * FROM d_product WHERE sellable_qty IS NULL
)
SELECT COUNT(*) AS null_sellable_qty_count
FROM null_sellable_qty;

-- Retrieve 5 sample records where sellable_qty is null
SELECT * FROM null_sellable_qty LIMIT 5;

-- /* =============================================================== */
-- /* Test: Validate prod_exp_dt format (YYYYMMDD) */
-- /* =============================================================== */

-- Check for records where prod_exp_dt is not in the YYYYMMDD format
WITH invalid_prod_exp_dt AS (
  SELECT *
  FROM d_product 
  WHERE NOT (prod_exp_dt RLIKE '^[0-9]{8}$')
)
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM invalid_prod_exp_dt;

-- Retrieve 5 sample records where prod_exp_dt is not in the YYYYMMDD format
SELECT * FROM invalid_prod_exp_dt LIMIT 5;

-- /* =============================================================== */
-- /* Cleanup Section: */
-- /* Optional: Add any cleanup operations here if needed */
-- /* =============================================================== */

-- Cleanup can involve deletion of temporary tables or restoration of original state

