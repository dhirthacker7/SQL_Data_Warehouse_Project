select sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   sls_order_dt,
	   sls_ship_dt,
	   sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
from bronze.crm_sales_details


-- 1) check sls_ord_num
select sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   sls_order_dt,
	   sls_ship_dt,
	   sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)


-- 2) check sls_prd_key and sls_cust_id
select sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   sls_order_dt,
	   sls_ship_dt,
	   sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

select sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   sls_order_dt,
	   sls_ship_dt,
	   sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)


-- 4) check sls_order_dt, sls_ship_dt, sls_due_dt
-- check for invalid dates
select nullif(sls_order_dt, 0) sls_order_dt
from bronze.crm_sales_details
where len(sls_order_dt) != 8 or sls_order_dt > 20500101

select sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date)
	   end as sls_order_dt,
	   case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date)
	   end as sls_ship_dt,
	   case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			else cast(cast(sls_due_dt as varchar) as date)
	   end as sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
from bronze.crm_sales_details

-- check that order date must be earlier than shipping or due date
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- 5) check sls_sales, sls_quantity, sls_price
-- all must be positive
-- sales = quantity * price
select distinct sls_sales as old_sls_sales,
	   sls_quantity,
	   sls_price as old_sls_price, 
	   case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			else sls_sales
	   end as sls_sales,
	   case when sls_price is null or sls_price <= 0
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price
	   end as sls_price
from bronze.crm_sales_details
where sls_sales != (sls_quantity * sls_price)
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales < 0  or sls_quantity < 0 or sls_price < 0



-- final check before inserting data into silver schema

insert into silver.crm_sales_details (
	sls_ord_num,
    sls_prd_key, 
	sls_cust_id,
    sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
) 
select sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date)
	   end as sls_order_dt,
	   case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date)
	   end as sls_ship_dt,
	   case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			else cast(cast(sls_due_dt as varchar) as date)
	   end as sls_due_dt,
	   case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			else sls_sales
	   end as sls_sales,
	   sls_quantity,
	   case when sls_price is null or sls_price <= 0
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price
	   end as sls_price
from bronze.crm_sales_details

-- change ddl to convert all date columns from int to date
