SELECT order_date,count(cust_id) cust_id_cnt,products from (
select order_date,cust_id,listagg(product_name, ',') within group (order by product_name) as products from orders
group by order_date,cust_id
order by order_date,cust_id
)foo
GROUP BY order_date,products
ORDER BY order_date,products