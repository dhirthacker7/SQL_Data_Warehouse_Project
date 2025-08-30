
create or alter procedure silver.load_silver as
begin
	
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime; 
	begin try
		set @batch_start_time = GETDATE()
		PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        set @start_time = GETDATE()
		print '>> Truncating Table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info';
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
		set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info';
		truncate table silver.crm_prd_info
		print '>> Inserting Data Into: silver.crm_prd_info';
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
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();

		print '>> Truncating Table: silver.crm_sales_details';
		truncate table silver.crm_sales_details
		print '>> Inserting Data Into: silver.crm_sales_details';
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
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();

		print '>> Truncating Table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12
		print '>> Inserting Data Into: silver.erp_cust_az12';
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
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();

		print '>> Truncating Table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101
		print '>> Inserting Data Into: silver.erp_loc_a101';
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
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();

		print '>> Truncating Table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
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
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end

