CREATE OR REPLACE TABLE purgo_playground.combined_agreement AS
SELECT 
    a.agree_no,
    a.agree_desc,
    a.agree_type,
    a.material_number,
    a.master_agreement_type,
    a.source_customer_id,
    c.address1,
    c.state,
    c.city,
    c.postal_code
FROM 
    purgo_playground.agreement a
JOIN 
    purgo_playground.customer c ON a.source_customer_id = c.source_customer_id

UNION ALL

SELECT 
    d.deal AS agree_no,
    NULL AS agree_desc,
    d.agree_type,
    d.material_number,
    d.master_agreement_type,
    d.source_customer_id,
    c.address1,
    c.state,
    c.city,
    c.postal_code
FROM 
    purgo_playground.deal d
JOIN 
    purgo_playground.customer c ON d.source_customer_id = c.source_customer_id;
