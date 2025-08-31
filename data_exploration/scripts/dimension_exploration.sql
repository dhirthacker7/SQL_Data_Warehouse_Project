-- TASKS

-- Retrieve a list of unique countries from which customers originate
select distinct country
from gold.dim_customers

-- Retrieve a list of unique categories, subcategories, and products
select distinct category, subcategory, product_name 
from gold.dim_products
order by 1,2,3
