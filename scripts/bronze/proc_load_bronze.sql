/*
===========================================================================================================

This stored procedure loads data into the ''bronze schema from external CSV files

It performs the following functions:
-Truncates the bronze tables before loading the data
-Uses the BULK INSERT command to load the CSV Files to Bronze Tables

Parameters: None
This stored procedure does not accept any parameters or return any values.

Usage Example: EXEC Bronze.load_bronze

===========================================================================================================
*/
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
--Create a start time and end time operator to calculate load times for each table and the entire bronze layer
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	--Try
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();
		PRINT '==============================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================================';

		PRINT '--------------------------------------------------------------';
		PRINT 'Loading crm tables';
		PRINT '--------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		--Make the table empty
		PRINT '>>Truncating crm customer info table';
		TRUNCATE TABLE Bronze.crm_cust_info;
		--Full Load
		PRINT 'Loading crm customer info table ';
		BULK INSERT Bronze.crm_cust_info
		FROM "C:\Users\JIMMY OKOTH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>-----------------';
		
		SET @start_time = GETDATE();
		--Make the table empty
		PRINT '>> Truncating crm product info table';
		TRUNCATE TABLE Bronze.crm_prd_info;
		--Full Load
		PRINT '>> Loading crm product info table';
		BULK INSERT Bronze.crm_prd_info
		FROM "C:\Users\JIMMY OKOTH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>-----------------';

		SET @start_time = GETDATE();
		--Make the table empty
		PRINT '>> Truncating crm sales details table';
		TRUNCATE TABLE Bronze.crm_sales_details;
		--Full Load
		PRINT '>> Loading crm sales details table';
		BULK INSERT Bronze.crm_sales_details
		FROM "C:\Users\JIMMY OKOTH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>-----------------';

		PRINT '--------------------------------------------------------------';
		PRINT 'Loading erp tables'
		PRINT '--------------------------------------------------------------';
		
		
		SET @start_time = GETDATE();
		--Make the table empty
		PRINT '>> Truncating erp customer az12 table';
		TRUNCATE TABLE Bronze.erp_cust_az12;
		--Full Load
		PRINT '>> Loading erp customer az12 table';
		BULK INSERT Bronze.erp_cust_az12
		FROM "C:\Users\JIMMY OKOTH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>----------------------------';

		SET @start_time = GETDATE();
		--Make the table empty
		PRINT '>> Truncating erp location a101 table';
		TRUNCATE TABLE Bronze.erp_loc_a101;
		--Full Load
		PRINT '>> Loading erp location a101 table';
		BULK INSERT Bronze.erp_loc_a101
		FROM "C:\Users\JIMMY OKOTH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>---------------------------';

		SET @start_time = GETDATE();
		--Make the table empty
		PRINT '>> Truncating erp product category g1v2 table';
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
		--Full Load
		PRINT '>> Loading erp product category g1v2 table';
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM "C:\Users\JIMMY OKOTH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>--------------------------';

	--get the entire batch load time
	PRINT '=============================================================================================================';
	PRINT 'Loading Bronze Layer is Complete';
	SET @batch_end_time = GETDATE();
	PRINT 'Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ' + 'seconds';
	PRINT '=============================================================================================================';
	END TRY
	
	--catch
	BEGIN CATCH
		PRINT '==========================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================';
	END CATCH
END
