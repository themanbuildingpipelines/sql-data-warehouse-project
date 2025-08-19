/*
===========================================================================================================

This stored procedure loads data into the ''bronze schema from external CSV files

It performs the following functions:
-Truncates the silver tables before loading the data
-Uses the INSERT command to load the transformed data to the Silver Tables

Data Transformations inside this stored procedure include:
-Data Cleaning
-Data Standardization
-Data Normalization
-Derived columns
-Data Enrichment

Parameters: None
This stored procedure does not accept any parameters or return any values.

Usage Example: EXEC Silver.load_bronze

===========================================================================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
--Create a start time and end time operator to calculate load times for each table and the entire Silver layer
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	--Try
	BEGIN TRY

		SET @batch_start_time = GETDATE();
		PRINT '==============================================================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================================================';

		PRINT '--------------------------------------------------------------';
		PRINT 'Loading crm tables';
		PRINT '--------------------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.crm_cust_info'
		TRUNCATE TABLE Silver.crm_cust_info;
		PRINT '>>Inserting Data into Silver.crm_cust_info'
		INSERT INTO Silver.crm_cust_info (
						cst_id,
						cst_key,
						cst_firstname,
						cst_lastname,
						cst_marital_status,
						cst_gndr,
						cst_create_date)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname)  AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
		FROM Bronze.crm_cust_info
		WHERE cst_id IS NOT NULL)t
		WHERE flag_last = 1
	
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>---------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.crm_prd_info'
		TRUNCATE TABLE Silver.crm_prd_info;
		PRINT '>>Inserting Data into Silver.prd_cust_info'
		INSERT INTO Silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			CAST(ISNULL(prd_cost, 0) AS INT) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'M' THEN 'Mountain'
				WHEN 'T' THEN 'Touring'
				WHEN 'S' THEN 'Other Sales'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_date,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ) -1 AS DATE) AS prd_end_dt
		FROM Bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>--------------------------------------------------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.crm_sales_details'
		TRUNCATE TABLE Silver.crm_sales_details;
		PRINT '>>Inserting Data into Silver.crm_sales_details'
		INSERT INTO Silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cus_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cus_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales = sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END sls_price
		FROM Bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>--------------------------------------------------------------------------------------------';

		PRINT '----------------------------------------------------------------------------------------------';
		PRINT 'Loading erp tables'
		PRINT '----------------------------------------------------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.erp_cust_az12'
		TRUNCATE TABLE Silver.erp_cust_az12;
		PRINT '>>Inserting Data into Silver.erp_cust_az12'
		INSERT INTO Silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)

		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END gen
		FROM Bronze.erp_cust_az12
	
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>--------------------------------------------------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.erp_loc_a101'
		TRUNCATE TABLE Silver.erp_loc_a101;
		PRINT '>>Inserting Data into Silver.erp_loc_a101'
		INSERT INTO Silver.erp_loc_a101 (cid, cntry)

		SELECT
			REPLACE(cid, '-', '') cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN  ('USA', 'US') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a' 
				 ELSE TRIM(cntry)
			END cntry
		FROM Bronze.erp_loc_a101
	
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>--------------------------------------------------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.erp_px_cat_g1v2'
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;
		PRINT '>>Inserting Data into Silver.erp_px_cat_g1v2'
		INSERT INTO Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)

		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM Bronze.erp_px_cat_g1v2
	
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '>>--------------------------------------------------------------------------------------------';

		--get the entire batch load time
		PRINT '=============================================================================================================';
		PRINT 'Loading Silver Layer is Complete';
		SET @batch_end_time = GETDATE();
		PRINT 'Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '=============================================================================================================';
	END TRY

	--catch
	BEGIN CATCH
		PRINT '==========================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================';
	END CATCH
END
