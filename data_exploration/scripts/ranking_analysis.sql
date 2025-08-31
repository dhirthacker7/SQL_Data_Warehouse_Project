/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY
===============================================================================
*/

-- Which 5 products Generating the Highest Revenue?
-- Simple Ranking
select top 5
    dp.product_name,
    sum(f.sales_amount) as highest_revenue
from gold.fact_sales f
left join gold.dim_products dp
on f.product_key = dp.product_key
group by dp.product_name
order by highest_revenue desc

-- Complex but Flexibly Ranking Using Window Functions
select * 
from (
    select
        dp.product_name,
        sum(f.sales_amount) as highest_revenue,
        ROW_NUMBER() over (order by sum(f.sales_amount) desc) as rank_products
    from gold.fact_sales f
    left join gold.dim_products dp
    on f.product_key = dp.product_key
    group by dp.product_name
) t
where rank_products < 6

-- What are the 5 worst-performing products in terms of sales?
select top 5
    dp.product_name,
    sum(f.sales_amount) as highest_revenue
from gold.fact_sales f
left join gold.dim_products dp
on f.product_key = dp.product_key
group by dp.product_name
order by highest_revenue asc

-- Find the top 10 customers who have generated the highest revenue
select top 10
    dc.customer_key,
    dc.first_name,
    dc.last_name,
    sum(f.sales_amount) as highest_revenue
from gold.fact_sales f
left join gold.dim_customers dc
on f.customer_key = dc.customer_key
group by 
    dc.customer_key,
    dc.first_name,
    dc.last_name
order by highest_revenue desc
        

-- The 3 customers with the fewest orders placed
select top 3
    dc.customer_key,
    dc.first_name,
    dc.last_name,
    count(distinct order_number) as total_orders
from gold.fact_sales f
left join gold.dim_customers dc
on dc.customer_key = f.customer_key
group by 
    dc.customer_key,
    dc.first_name,
    dc.last_name
order by total_orders 


-- The 3 customers with the fewest orders placed
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders ;

