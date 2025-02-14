-- Count the number of records where 'item_nbr' is null
SELECT COUNT(*) 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL;

-- Display 5 sample records where 'item_nbr' is null
SELECT * 
FROM purgo_playground.d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

-- Count the number of records where 'sellable_qty' is null
SELECT COUNT(*) 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL;

-- Display 5 sample records where 'sellable_qty' is null
SELECT * 
FROM purgo_playground.d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

-- Count the number of records where 'prod_exp_dt' is not in the format 'yyyymmdd'
SELECT COUNT(*) 
FROM purgo_playground.d_product 
WHERE prod_exp_dt NOT LIKE '_______';

-- Display 5 sample records where 'prod_exp_dt' is not in the format 'yyyymmdd'
SELECT * 
FROM purgo_playground.d_product 
WHERE prod_exp_dt NOT LIKE '_______' 
LIMIT 5;
