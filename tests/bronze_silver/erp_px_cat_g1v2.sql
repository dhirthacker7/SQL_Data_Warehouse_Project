
select id,
	   cat,
	   subcat,
	   maintenance
from bronze.erp_px_cat_g1v2


-- 1) check id
select id
from bronze.erp_px_cat_g1v2


-- 2) check cat
select cat
from bronze.erp_px_cat_g1v2
where cat != trim(cat)


-- 3) check subcat
select subcat
from bronze.erp_px_cat_g1v2
where subcat != trim(subcat)


-- 4) maintenance
select maintenance
from bronze.erp_px_cat_g1v2
where cat != trim(maintenance)

-- insert values
insert into silver.erp_px_cat_g1v2 (
	id,
    cat,
	subcat,
	maintenance
)
select id,
	   cat,
	   subcat,
	   maintenance
from bronze.erp_px_cat_g1v2

