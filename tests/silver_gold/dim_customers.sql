
select ci.cst_id,
		ci.cst_key,
	    ci.cst_firstname,
	    ci.cst_lastname,
	    ci.cst_marital_status,
	    ci.cst_gndr,
	    ci.cst_create_date,
	    ca.bdate,
	    ca.gen,
	    la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

select distinct ci.cst_gndr, 
				ca.gen,
				case when ci.cst_gndr != 'N/A' then ci.cst_gndr
					 else coalesce(ca.gen, 'N/A')
				end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2
go
-- assumption: CRM data is more accurate
-- final logic for dimension_customers
create view gold.dim_customers as
select ROW_NUMBER() over (order by cst_id) as customer_key,
	   ci.cst_id as customer_id,
	   ci.cst_key as customer_number,
	   ci.cst_firstname as first_name,
	   ci.cst_lastname as last_name,
	   la.cntry as country,
	   ci.cst_marital_status as marital_status,
	   case when ci.cst_gndr != 'N/A' then ci.cst_gndr
			else coalesce(ca.gen, 'N/A')
	   end as gender,
	   ca.bdate as birthdate,
	   ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid



