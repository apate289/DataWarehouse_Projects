/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
Create or Alter procedure Bronze.load_Bronze AS
Begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	Begin Try
		set @batch_start_time = GETDATE();
		print '=======================================================';
		print 'Loading Bronze Layer';
		print '=======================================================';

		print '---------------------------------';
		print 'loading CRM Tables';
		print '---------------------------------';

		-- Delete the entire  table and load--
		set @start_time = GETDATE();
		print '>> Truncating Table : Bronze.crm_cust_info';
		TRUNCATE TABLE Bronze.crm_cust_info;

		-- Bulk insert from CSV file --
		print '>> Inserting Data Into : Bronze.crm_cust_info';
		BULK INSERT Bronze.crm_cust_info
		from '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'--'D:\DataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator=',',
			tablock
		);
		
		--select count(*) from Bronze.crm_cust_info;
		set @end_time = GETDATE();
		print '>> Load Duration : ' + cast (datediff(microsecond, @start_time, @end_time) as nvarchar) + ' microseconds';
		print '';

		set @start_time = GETDATE();
		print '>> Truncating Table : Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sales_details;
	
		print '>> Inserting Data Into : Bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sales_details
		from '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'--'D:\DataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator=',',
			tablock
		);

		--select count(*) from Bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>> Load Duration : ' + cast (datediff(millisecond, @start_time, @end_time) as nvarchar) + ' milliseconds';
		print '';

		set @start_time = GETDATE();
	
		print '>> Truncating Table : Bronze.crm_prod_info';
		TRUNCATE TABLE Bronze.crm_prod_info;
	
		print '>> Inserting Data Into : Bronze.crm_prod_info';
		BULK INSERT Bronze.crm_prod_info
		from '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'--'D:\DataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator=',',
			tablock
		);

		--select count(*) from Bronze.crm_prod_info;
		
		set @end_time = GETDATE();
		print '>> Load Duration : ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '';

		set @start_time = GETDATE();
		print '---------------------------------';
		print 'loading ERP Tables';
		print '---------------------------------';

		print '>> Truncating Table : Bronze.erp_cust_az12';
		TRUNCATE TABLE Bronze.erp_cust_az12;

		print '>> Inserting Data Into : Bronze.erp_cust_az12';
		BULK INSERT Bronze.erp_cust_az12
		from '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'--'D:\DataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator=',',
			tablock
		);

		--select count(*) from Bronze.erp_cust_az12;
		set @end_time = GETDATE();
		print '>> Load Duration : ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '';

		set @start_time = GETDATE();
	
		print '>> Truncating Table : Bronze.erp_loc_a101';
		TRUNCATE TABLE Bronze.erp_loc_a101;
	
		print '>> Inserting Data Into : Bronze.erp_loc_a101';
		BULK INSERT Bronze.erp_loc_a101
		from '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'--'D:\DataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator=',',
			tablock
		);

		--select count(*) from Bronze.erp_loc_a101;
		set @end_time = GETDATE();
		print '>> Load Duration : ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '';

		set @start_time = GETDATE();
	
		print '>> Truncating Table : Bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
	
		print '>> Inserting Data Into : Bronze.erp_px_cat_g1v2';
		BULK INSERT Bronze.erp_px_cat_g1v2
		from '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'--'D:\DataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator=',',
			tablock
		);

		--select count(*) from Bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE();
		print '>> Load Duration : ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '';

		set @batch_end_time = GETDATE();
		print '=======================================================';
		print 'Loading Bronze Layer is completed';
		print 'Total Load Duration : ' + cast (datediff(millisecond, @batch_start_time, @batch_end_time) as nvarchar) + ' milli seconds';
		print '=======================================================';

		-- EXEC Bronze.load_Bronze
	END Try
	Begin Catch
		print '===============================================';
		print 'Error occurred while loading Bronze Layer';
		print 'Error Message :' + Error_message();
		print 'Error Number :' + cast(Error_number() as nvarchar);
		print 'Error State :' + cast(Error_state() as nvarchar);
		print '===============================================';
	END Catch
End
