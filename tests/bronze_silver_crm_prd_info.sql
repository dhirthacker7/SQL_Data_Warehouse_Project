

select * from bronze.crm_prd_info


select prd_id,
	   prd_key,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
from bronze.crm_prd_info

-- 1) check prd_id
-- check for nulls or duplicates in primary key
-- expectation: no result

select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

-- 2) check prd_key
-- breaking prd_key into 2 columns
-- cat_id to match/join with id of erp_px_cat_g1v2 table
-- prd_key to match with sls_prod_key from sales_order table
select prd_id,
	   prd_key,
	   replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	   substring(prd_key, 7, len(prd_key)) as prd_key,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
from bronze.crm_prd_info
--where replace(substring(prd_key, 1, 5), '-', '_') not in
--(select distinct id from bronze.erp_px_cat_g1v2)
where substring(prd_key, 7, len(prd_key)) not in
(select sls_prd_key from bronze.crm_sales_details)

-- checking for format consistency of cat_id with categories table
select distinct id 
from bronze.erp_px_cat_g1v2

-- checking for format consistency of prd_key with sales details table
select sls_prd_key
from bronze.crm_sales_details


-- 3) check prd_nm
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)
-- all good here

-- 4) check prd_cost
-- check for negative or nulls
-- expectation: no results
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null
-- only 2 nulls, can replace with a 0 if business allows
select prd_id,
	   prd_key,
	   replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	   substring(prd_key, 7, len(prd_key)) as prd_key,
	   prd_nm,
	   isnull(prd_cost, 0) as prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
from bronze.crm_prd_info


-- 5) check prd_line
select distinct prd_line
from bronze.crm_prd_info

select prd_id,
	   prd_key,
	   replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	   substring(prd_key, 7, len(prd_key)) as prd_key,
	   prd_nm,
	   isnull(prd_cost, 0) as prd_cost,
	   case upper(trim(prd_line))
			when 'M' then 'Mountain'
	        when 'R' then 'Road'
	        when 'S' then 'Other sales'
	        when 'T' then 'Touring'
			else 'N/A'
	   end as prd_line,
	   prd_start_dt,
	   prd_end_dt
from bronze.crm_prd_info



-- 6) start and end date
select * 
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

select prd_id,
	   prd_key,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt,
	   lead(prd_start_dt) over (partition by prd_key order by prd_start_dt asc) - 1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')



select prd_id,
	   prd_key,
	   replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	   substring(prd_key, 7, len(prd_key)) as prd_key,
	   prd_nm,
	   isnull(prd_cost, 0) as prd_cost,
	   case upper(trim(prd_line))
			when 'M' then 'Mountain'
	        when 'R' then 'Road'
	        when 'S' then 'Other sales'
	        when 'T' then 'Touring'
			else 'N/A'
	   end as prd_line,
	   cast(prd_start_dt as date) as prd_start_dt,
	   cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt asc) - 1 as date) as prd_end_dt_test
from bronze.crm_prd_info

-- change ddl script of silver.crm_prd_info to add new cat_id column and change datetime to date for the date columns


-- finally insert into silver table
insert into silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
select prd_id,
	   replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	   substring(prd_key, 7, len(prd_key)) as prd_key,
	   prd_nm,
	   isnull(prd_cost, 0) as prd_cost,
	   case upper(trim(prd_line))
			when 'M' then 'Mountain'
	        when 'R' then 'Road'
	        when 'S' then 'Other sales'
	        when 'T' then 'Touring'
			else 'N/A'
	   end as prd_line,
	   cast(prd_start_dt as date) as prd_start_dt,
	   cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt asc) - 1 as date) as prd_end_dt_test
from bronze.crm_prd_info


