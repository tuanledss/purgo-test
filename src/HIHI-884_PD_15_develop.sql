/* Data Quality Check SQL Code for Databricks Environment */

/* 
   Validate that 'item_nbr' and 'sellable_qty' are not null in 'purgo_playground.d_product'.
   Also, validate that 'prod_exp_dt' follows the 'yyyymmdd' format.
*/

/* 
   1. Check for NULL values in 'item_nbr' and display sample records 
*/

/* Count records where 'item_nbr' is NULL */
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

/* Display 5 sample records where 'item_nbr' is NULL */
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/* 
   2. Check for NULL values in 'sellable_qty' and display sample records 
*/

/* Count records where 'sellable_qty' is NULL */
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

/* Display 5 sample records where 'sellable_qty' is NULL */
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* 
   3. Check for invalid 'prod_exp_dt' format and display sample records 
*/

/* Count records where 'prod_exp_dt' format is not 'yyyymmdd' */
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^\d{8}$';

/* Display 5 sample records where 'prod_exp_dt' is invalid */
SELECT *
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt RLIKE '^\d{8}$'
LIMIT 5;

/* 
   Additional checks for other data quality criteria can be implemented as needed,
   such as verifying acceptable threshold limits and logging errors.
*/
