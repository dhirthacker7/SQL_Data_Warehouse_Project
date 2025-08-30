
select * from bronze.erp_loc_a101

select * from bronze.crm_cust_info

-- 1) check cid, remove - to match crm_cst_id from cust_info
select replace(cid, '-', '') as cid,
	   cntry
from bronze.erp_loc_a101
-- checking mismatch with crm_cust_id table
select replace(cid, '-', '') as cid,
	   cntry
from bronze.erp_loc_a101
where replace(cid, '-', '') not in (
	select cst_key from silver.crm_cust_info
)

-- 2) cntry
select distinct cntry
from bronze.erp_loc_a101
order by cntry

-- transformation
select replace(cid, '-', '') as cid,
	   case when upper(trim(cntry)) = 'DE' then 'GERMANY'
			when upper(trim(cntry)) in ('US', 'USA') then 'United States'
			when upper(trim(cntry)) = '' or cntry is null then 'N/A'
			else trim(cntry) 
		end as cntry
from bronze.erp_loc_a101


-- loading silver table
insert into silver.erp_loc_a101 (
	cid,
	cntry
)
select replace(cid, '-', '') as cid,
	   case when upper(trim(cntry)) = 'DE' then 'GERMANY'
			when upper(trim(cntry)) in ('US', 'USA') then 'United States'
			when upper(trim(cntry)) = '' or cntry is null then 'N/A'
			else trim(cntry) 
		end as cntry
from bronze.erp_loc_a101
