
-- 1) check cid
-- it has to join with silver table, so format to bring in the required format
select case when cid like 'NAS%' then substring(cid, 4, len(cid))
			else cid
	   end as cid,
	   bdate,
	   gen
from bronze.erp_cust_az12

-- 2) check bdate
select cid,
	   bdate,
	   gen
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()


select case when cid like 'NAS%' then substring(cid, 4, len(cid))
			else cid
	   end as cid,
	   case when bdate > getdate() then null
			else bdate
	   end as bdate,
	   gen
from bronze.erp_cust_az12


-- 3) check gender
select distinct gen
from bronze.erp_cust_az12


-- inserting the values in silver table

insert into silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
select case when cid like 'NAS%' then substring(cid, 4, len(cid))
			else cid
	   end as cid,
	   case when bdate > getdate() then null
			else bdate
	   end as bdate,
	   case when upper(trim(gen)) in ('M', 'Male') then 'Male'
			when upper(trim(gen)) in ('F', 'Female') then 'Female'
			else 'N/A'
	   end as gen
from bronze.erp_cust_az12
