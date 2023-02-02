INSERT INTO dwh.fact_sales (
      "date"
    , "datetime"
    , invoice_id
    , branch_id
    , customer_type_id
    , gender_id
    , product_line_id
    , payment_id
    , unit_price
    , quantity
    , rating
)
SELECT 
      to_char(s.datetime, 'YYYYMMDD') :: int AS "date"
    , s.datetime AS "datetime"
    , s.invoice_id
    , dwh.dim_branch_lookup_update(s.branch_id, b.branch, c.city) AS branch_id
    , dwh.dim_customer_type_lookup_update(s.customer_type, ct.customer_type) AS customer_type_id
    , dwh.dim_gender_lookup_update(s.gender, g.gender) AS gender_id
    , dwh.dim_product_line_lookup_update(s.product_line, pl.product_line) AS product_line_id
    , dwh.dim_payment_lookup_update(s.payment, p.payment) AS payment_id
    , s.unit_price 
    , s.quantity 
    , s.rating 
FROM nds.sales AS s 
JOIN nds.customer_type AS ct ON ct.id = s.customer_type 
JOIN nds.gender AS g ON g.id = s.gender 
JOIN nds.product_line AS pl ON pl.id = s.product_line 
JOIN nds.payment AS p ON p.id = s.payment 
JOIN nds.branch AS b ON b.id = s.branch_id 
JOIN nds.city AS c ON c.id = b.city_id;