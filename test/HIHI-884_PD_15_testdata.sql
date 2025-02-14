-- Generate test data for purgo_playground.d_product

CREATE OR REPLACE TEMPORARY VIEW test_data AS
SELECT
  CAST(CONCAT('prod-', seq.id) AS STRING) AS prod_id,
  CASE 
    WHEN seq.id % 5 = 0 THEN NULL
    ELSE CONCAT('item-', seq.id)
  END AS item_nbr, -- NULL handling scenario
  CAST((seq.id * 23.45) AS DOUBLE) AS unit_cost,
  CAST(seq.id AS DECIMAL(38, 0)) AS prod_exp_dt, -- Edge case: boundaries are tested with IDs in range
  CAST((seq.id / 2.0) AS DOUBLE) AS cost_per_pkg,
  CONCAT('address-', seq.id) AS plant_add,
  CASE
    WHEN seq.id % 4 = 0 THEN 'loc-01'
    ELSE 'loc-02'
  END AS plant_loc_cd,
  CASE
    WHEN seq.id % 3 = 0 THEN 'line-A'
    ELSE 'line-B'
  END AS prod_line,
  CASE
    WHEN seq.id % 7 = 0 THEN 'stock-C'
    ELSE 'stock-D'
  END AS stock_type,
  CAST((seq.id * 1.1) AS DOUBLE) AS pre_prod_days,
  CASE
    WHEN seq.id % 3 = 0 THEN NULL
    ELSE CAST(seq.id AS DOUBLE)
  END AS sellable_qty, -- NULL handling scenario
  CONCAT('tracker-', seq.id) AS prod_ordr_tracker_nbr,
  CASE
    WHEN seq.id % 6 = 0 THEN NULL
    ELSE CONCAT('max-qty-', CAST(seq.id AS STRING))
  END AS max_order_qty, -- Special characters and multi-byte characters
  CASE
    WHEN seq.id % 2 = 0 THEN 'Y'
    ELSE 'N'
  END AS flag_active,
  CURRENT_TIMESTAMP() AS crt_dt,
  CURRENT_TIMESTAMP() AS updt_dt,
  CASE
    WHEN seq.id % 3 = 0 THEN 'SRC1'
    ELSE 'SRC2'
  END AS src_sys_cd,
  CASE
    WHEN seq.id % 5 = 0 THEN 'ENTITY1'
    ELSE 'ENTITY2'
  END AS hfm_entity
FROM RANGE(1, 31) AS seq;

-- Happy path test data
SELECT * FROM test_data
WHERE item_nbr IS NOT NULL
AND sellable_qty IS NOT NULL
AND REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$') -- Valid prod_exp_dt format YYYYMMDD

-- Error cases: Null and invalid data type cases
SELECT 
  COUNT(*) AS null_item_nbr_count 
FROM test_data 
WHERE item_nbr IS NULL;

SELECT * FROM test_data 
WHERE item_nbr IS NULL LIMIT 5;

SELECT 
  COUNT(*) AS null_sellable_qty_count 
FROM test_data 
WHERE sellable_qty IS NULL;

SELECT * FROM test_data 
WHERE sellable_qty IS NULL LIMIT 5;

SELECT 
  COUNT(*) AS invalid_prod_exp_dt_count 
FROM test_data 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$');

SELECT * FROM test_data 
WHERE NOT REGEXP_LIKE(CAST(prod_exp_dt AS STRING), '^\d{8}$') LIMIT 5;
