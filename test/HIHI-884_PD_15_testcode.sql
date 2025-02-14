/* Data Quality Check on d_product Table */

/* Validate 'item_nbr' is not null */
SELECT 
  COUNT(*) AS null_item_nbr_count 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

SELECT * FROM purgo_playground.d_product 
WHERE item_nbr IS NULL LIMIT 5;

/* Validate 'sellable_qty' is not null */
SELECT 
  COUNT(*) AS null_sellable_qty_count 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

SELECT * FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL LIMIT 5;

/* Validate 'prod_exp_dt' format is yyyymmdd */
SELECT 
  COUNT(*) AS invalid_prod_exp_dt_count 
FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$');

SELECT * FROM purgo_playground.d_product 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$') LIMIT 5;

/* Example for potential Delta Lake operations tests (if applicable) */
-- Example MERGE operation validation
MERGE INTO purgo_playground.d_product AS target
USING (SELECT * FROM purgo_playground.d_product_clone WHERE flag_active = 'Y') AS source
ON target.prod_id = source.prod_id
WHEN MATCHED THEN 
  UPDATE SET target.unit_cost = source.unit_cost
WHEN NOT MATCHED THEN 
  INSERT (prod_id, item_nbr, unit_cost) VALUES (source.prod_id, source.item_nbr, source.unit_cost);

/* Test Delta Lake DELETE operation */
DELETE FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

/* Test Delta Lake UPDATE operation */
UPDATE purgo_playground.d_product
SET unit_cost = unit_cost * 1.1
WHERE hfm_entity = 'ENTITY2';

/* Validate schema */
-- Check schema for 'd_product' to ensure conformity
DESCRIBE TABLE purgo_playground.d_product;

/* Cleanup */
-- Clean up temporary views or tables if created during tests
DROP VIEW IF EXISTS test_data;
