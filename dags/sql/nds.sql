INSERT INTO nds.sales (
      invoice_id
    , branch_id
    , customer_type
    , gender
    , product_line
    , payment
    , datetime
    , unit_price
    , quantity
    , rating
)
SELECT
      invoice_id 
    , nds.branch_lookup_update(branch, city)
    , nds.customer_type_lookup_update(customer_type)
    , nds.gender_lookup_update(gender)
    , nds.product_line_lookup_update(product_line)
    , nds.payment_lookup_update(payment)
    , "date" + "time"
    , unit_price
    , quantity
    , rating
FROM stage.valid_sales;
