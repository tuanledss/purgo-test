-- SQL Test Script for Databricks Environment

/* 
   Data Quality Checks for `d_product` Table
   In order to ensure data integrity before moving to production
*/

/* 
   Setup or Configuration Information
   Uncomment and modify the following lines according to your database setup
   -- USE CATALOG purgo_playground;
   -- USE SCHEMA {your_schema};
*/

/* 
   Scenario: Verify `item_nbr` is not null 
   Retrieve the count and sample records with null `item_nbr`
*/

WITH null_item_nbr AS (
  SELECT *
  FROM d_product
  WHERE item_nbr IS NULL
)

SELECT COUNT(*) AS null_item_nbr_count FROM null_item_nbr;

SELECT *
FROM null_item_nbr
LIMIT 5;


/* 
   Scenario: Verify `sellable_qty` is not null 
   Retrieve the count and sample records with null `sellable_qty`
*/

WITH null_sellable_qty AS (
  SELECT *
  FROM d_product
  WHERE sellable_qty IS NULL
)

SELECT COUNT(*) AS null_sellable_qty_count FROM null_sellable_qty;

SELECT *
FROM null_sellable_qty
LIMIT 5;


/* 
   Scenario: Validate `prod_exp_dt` format 
   Ensure `prod_exp_dt` is in 'yyyyMMdd' format; retrieve count and samples
*/

WITH invalid_prod_exp_dt AS (
  SELECT *
  FROM d_product
  WHERE NOT REGEXP_LIKE(prod_exp_dt, '^\d{8}$')
)

SELECT COUNT(*) AS invalid_prod_exp_dt_count FROM invalid_prod_exp_dt;

SELECT *
FROM invalid_prod_exp_dt
LIMIT 5;


/* Cleanup operations, if needed, add here 
   -- Example cleanup code:
   -- DROP VIEW IF EXISTS null_item_nbr;
   -- DROP VIEW IF EXISTS null_sellable_qty;
   -- DROP VIEW IF EXISTS invalid_prod_exp_dt;
*/
