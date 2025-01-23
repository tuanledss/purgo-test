
CREATE TABLE tpcds.inventory (
    inv_date_sk INT NOT NULL, 
    inv_item_sk INT NOT NULL, 
    inv_warehouse_sk INT NOT NULL, 
    inv_quantity_on_hand INT,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
) USING DELTA
