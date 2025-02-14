-- Test Data Generation for d_product table

-- Create a temporary view with test data
CREATE OR REPLACE TEMP VIEW d_product_test_data AS
SELECT
  CAST(prod_id AS BIGINT) AS prod_id,
  CAST(item_nbr AS STRING) AS item_nbr,
  CAST(sellable_qty AS DOUBLE) AS sellable_qty,
  CAST(prod_exp_dt AS STRING) AS prod_exp_dt
FROM VALUES
  -- Happy path test data
  (1, 'ITEM001', 100.0, '20240321'),
  (2, 'ITEM002', 200.0, '20240322'),
  -- Edge cases
  (3, 'ITEM003', 0.0, '20240323'), -- Boundary condition for sellable_qty
  (4, 'ITEM004', 9999999999.99, '20240324'), -- Large sellable_qty
  -- Error cases
  (5, 'ITEM005', -10.0, '20240325'), -- Negative sellable_qty
  (6, 'ITEM006', 50.0, '2024-03-26'), -- Invalid date format
  -- NULL handling scenarios
  (7, NULL, 300.0, '20240327'), -- NULL item_nbr
  (8, 'ITEM008', NULL, '20240328'), -- NULL sellable_qty
  -- Special characters and multi-byte characters
  (9, 'ITEM@#$', 400.0, '20240329'), -- Special characters in item_nbr
  (10, 'ITEM漢字', 500.0, '20240330'), -- Multi-byte characters in item_nbr
  -- Additional diverse records
  (11, 'ITEM011', 600.0, '20240331'),
  (12, 'ITEM012', 700.0, '20240401'),
  (13, 'ITEM013', 800.0, '20240402'),
  (14, 'ITEM014', 900.0, '20240403'),
  (15, 'ITEM015', 1000.0, '20240404'),
  (16, 'ITEM016', 1100.0, '20240405'),
  (17, 'ITEM017', 1200.0, '20240406'),
  (18, 'ITEM018', 1300.0, '20240407'),
  (19, 'ITEM019', 1400.0, '20240408'),
  (20, 'ITEM020', 1500.0, '20240409'),
  (21, 'ITEM021', 1600.0, '20240410'),
  (22, 'ITEM022', 1700.0, '20240411'),
  (23, 'ITEM023', 1800.0, '20240412'),
  (24, 'ITEM024', 1900.0, '20240413'),
  (25, 'ITEM025', 2000.0, '20240414'),
  (26, 'ITEM026', 2100.0, '20240415'),
  (27, 'ITEM027', 2200.0, '20240416'),
  (28, 'ITEM028', 2300.0, '20240417'),
  (29, 'ITEM029', 2400.0, '20240418'),
  (30, 'ITEM030', 2500.0, '20240419');

-- Data Quality Check SQL Logic

-- Check for NULL item_nbr
SELECT COUNT(*) AS null_item_nbr_count
FROM d_product_test_data
WHERE item_nbr IS NULL;

-- Display 5 sample records where item_nbr is NULL
SELECT *
FROM d_product_test_data
WHERE item_nbr IS NULL
LIMIT 5;

-- Check for NULL sellable_qty
SELECT COUNT(*) AS null_sellable_qty_count
FROM d_product_test_data
WHERE sellable_qty IS NULL;

-- Display 5 sample records where sellable_qty is NULL
SELECT *
FROM d_product_test_data
WHERE sellable_qty IS NULL
LIMIT 5;

-- Check for invalid prod_exp_dt format (not yyyymmdd)
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM d_product_test_data
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display 5 sample records where prod_exp_dt is not yyyymmdd
SELECT *
FROM d_product_test_data
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$'
LIMIT 5;
