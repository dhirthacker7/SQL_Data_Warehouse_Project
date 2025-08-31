
-- creating the fact tables
go
create view gold.fact_sales as
select
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
go

-- checking foreign key integrity
select *
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where c.customer_key is null

select *
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where p.product_key is null
