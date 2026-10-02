{{ config(severity = 'warn') }}

SELECT 1
FROM 
    {{ ref('obt') }} AS obt_b
WHERE 
    obt_b.order_id IS NULL
OR 
    obt_b.product_id IS NULL
OR 
    obt_b.store_id IS NULL
OR 
    obt_b.employee_id IS NULL
OR 
    obt_b.customer_id IS NULL
OR 
    obt_b.order_item_id IS NULL