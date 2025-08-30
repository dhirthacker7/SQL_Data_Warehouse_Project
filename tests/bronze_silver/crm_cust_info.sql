-- check for NULLS in primary key
-- expectation: No Result

SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [dataWarehouse].[bronze].[crm_cust_info]


select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;


select cst_key, count(*) as countOcc 
from bronze.crm_cust_info
group by cst_key
having count(*) > 1;


--below query shows multiple records, but latest records have best data, so, we will pick the latest record
--using the window function

select * 
from (
    select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last 
    from bronze.crm_cust_info
) t where flag_last = 1 and cst_id = 29466

-- check for unwanted spaces in string values
-- expectation: no results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

-- already proper data
select cst_marital_status
from bronze.crm_cust_info
where cst_marital_status != trim(cst_marital_status)

-- already proper data
select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)


-- data standardization and consistency
select distinct cst_gndr
from bronze.crm_cust_info


select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)



-- correcting column name errors
--EXEC sp_rename 'bronze.crm_cust_info.cst_material_status', 'cst_marital_status', 'COLUMN';
--EXEC sp_rename 'silver.crm_cust_info.cst_material_status', 'cst_marital_status', 'COLUMN';




-- Generalizing gender and marital status values

-- check all possible values of cst_gndr
select distinct cst_gndr
from bronze.crm_cust_info

-- check all possible values of cst_marital_status
select distinct cst_marital_status
from bronze.crm_cust_info



select cst_id, cst_key, 
       trim(cst_firstname) as cst_firstname, 
       trim(cst_lastname) as cst_lastname, 
       case when upper(trim(cst_marital_status)) = 'S' then 'Single'
            when upper(trim(cst_marital_status)) = 'M' then 'Married'
            else 'N/A'
       end cst_marital_status,
       case when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'N/A'
       end cst_gndr,
       cst_create_date
from (
    select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
    from bronze.crm_cust_info
    where cst_id is not null
) t where flag_last = 1






-- insert into the Silver schema tables!

insert into silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
select cst_id, cst_key, 
       trim(cst_firstname) as cst_firstname, 
       trim(cst_lastname) as cst_lastname, 
       case when upper(trim(cst_marital_status)) = 'S' then 'Single'
            when upper(trim(cst_marital_status)) = 'M' then 'Married'
            else 'N/A'
       end cst_marital_status,
       case when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'N/A'
       end cst_gndr,
       cst_create_date
from (
    select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
    from bronze.crm_cust_info
    where cst_id is not null
) t where flag_last = 1




select * from silver.crm_cust_info

