/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_silver as 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();
		PRINT'===============================';
		PRINT'Loading Silver Layer'
		PRINT'===============================';

		PRINT'-------------------------------';
		PRINT'Loading CRM Table'
		PRINT'--------------------------------';

		-- loading silver.crm_cust_info
		SET @start_time = GETDATE();
		--1) Truncate Table & Insert Into silver.crm_cust_info
		PRINT'>>1)Truncating Table  silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'>>1)Inserting Table silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
					cst_id, 
					cst_key, 
					cst_firstname, 
					cst_lastname, 
					cst_material_status, 
					cst_gndr,
					cst_create_date
				)
				SELECT
					cst_id,
					cst_key,
					RTRIM(LTRIM(cst_firstname)) AS cst_firstname,
					RTRIM(LTRIM(cst_lastname)) AS cst_lastname,
					CASE 
						WHEN UPPER(RTRIM(LTRIM(cst_material_status))) = 'S' THEN 'Single'
						WHEN UPPER(RTRIM(LTRIM(cst_material_status))) = 'M' THEN 'Married'
						ELSE 'n/a'
					END AS cst_marital_status, -- Normalize marital status values to readable format
					CASE 
						WHEN UPPER(RTRIM(LTRIM(cst_gndr))) = 'F' THEN 'Female'
						WHEN UPPER(RTRIM(LTRIM(cst_gndr))) = 'M' THEN 'Male'
						ELSE 'n/a'
					END AS cst_gndr, -- Normalize gender values to readable format
					cst_create_date
				FROM (
					SELECT
						*,
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
				) t
				WHERE flag_last = 1; -- Select the most recent record per customer
				SET @end_time = GETDATE();
			    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';



			-- loading silver.crm_prd_info
			SET @start_time = GETDATE();
			--2) Truncate Table & Insert Into Silver.crm_prd_info
			PRINT'>>2)Truncating Table  Silver.crm_prd_info';
			TRUNCATE TABLE Silver.crm_prd_info;
			PRINT'>>2)Truncating Table  Silver.crm_prd_info';
			INSERT INTO Silver.crm_prd_info (
					prd ,
				cat_id ,
				prd_key ,
				prd_nm ,
				prd_cost,
				prd_line  ,
				prd_start_dt,
				prd_end_dt 
			)
			SELECT 
				prd,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
				SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost, 0) AS prd_cost,
				CASE UPPER(LTRIM(RTRIM(prd_line))) 
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'N/A'
				END AS prd_line,
				CAST(prd_start_dt AS DATE) as prd_start_dt,
				CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))AS DATE) AS prd_end_dt
			FROM Bronze.crm_prd_info;
			SET @end_time = GETDATE();
			    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';



			-- loading silver.crm_sales_details
		    SET @start_time = GETDATE();
			--3) Truncate Table & Insert Into Silver.erp_px_cat_g1v2
			PRINT'>>3)Truncating Table Silver.crm_sales_details';
			TRUNCATE TABLE Silver.crm_sales_details;
			PRINT'>>3)Inserting Table Silver.crm_sales_details';
			INSERT INTO Silver.crm_sales_details (
				sls_ord_num ,
				sls_prd_key ,
				sls_cust_id,
				sls_order_dt ,
				sls_ship_dt ,
				sls_due_dt ,
				sls_sales ,
				sls_quantity ,
				sls_price 
			)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS varchar)AS DATE)
				END sls_order_dt,
				CASE WHEN sls_ship_date <= 0 OR LEN(sls_ship_date) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_date AS varchar)AS DATE)
				END sls_ship_dt,
				CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS varchar)AS DATE)
				END sls_due_dt,
				case when sls_sales is null or sls_sales <0 or sls_sales != ABS(sls_quantity) * ABS(sls_price)
						then ABS(sls_quantity) * ABS(sls_price)
					ELSE sls_sales
				END sls_sales,
				sls_quantity,
				CASE WHEN sls_price IS NULL OR sls_price < 0
						THEN sls_sales / NULLIF(sls_quantity,0)
					ELSE sls_price
				END sls_price
			FROM Bronze.crm_sales_details;
			SET @end_time = GETDATE();
			    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			PRINT'-------------------------------';
			PRINT'-------------------------------';
			PRINT'Loading ERP Table'
			PRINT'--------------------------------';
			PRINT'-------------------------------';

			-- loading silver.erp_cust_az12 
			SET @start_time = GETDATE();
			--4) Truncate Table & Insert Into Silver.erp_cust_az12 
			PRINT'>>4)Truncating Table  Silver.erp_cust_az12 ';
			TRUNCATE TABLE Silver.erp_cust_az12 ;
			PRINT'>>4)Inserting Table  Silver.erp_cust_az12  ';
			INSERT INTO Silver.erp_cust_az12 (
				cid,
				bdate,
				gen
			)
			SELECT
				CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' Prefix if present
					 ELSE cid 
				end cid,
				CASE WHEN bdate > GETDATE() THEN NULL -- Set Future Birthdates
					 ELSE bdate
				END bdate,
				CASE WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
					 WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
					 ELSE 'N/A' 
				END gen		-- Normalize Gender values And Handel Unknown Cases
			FROM Bronze.erp_CUST_AZ12;
			SET @end_time = GETDATE();
			    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';



			-- loading silver.erp_loc_a101
			SET @start_time = GETDATE();
			--5) Truncate Table & Insert Into Silver.erp_loc_a101 
			PRINT'>>5)Truncating Table  Silver.erp_loc_a101 ';
			TRUNCATE TABLE Silver.erp_loc_a101 ;
			PRINT'>>5)Inserting Table  Silver.erp_loc_a101 ';
			INSERT INTO Silver.erp_loc_a101 (
				cid,
				cntry
			)
			select 
				replace(LTRIM(RTRIM(cid)),'-','') AS cid,
				CASE 
					when RTRIM(LTRIM(cntry)) = 'DE' THEN 'Germany'
					when RTRIM(LTRIM(cntry))  in('USA','US') THEN 'United States'
					when RTRIM(LTRIM(cntry)) = '' or RTRIM(LTRIM(cntry)) IS NULL THEN 'N/A'
					ELSE RTRIM(LTRIM(cntry)) 
				END AS cntry	-- Normalize And handel missing or blank country code
			from Bronze.erp_LOC_A101;
			SET @end_time = GETDATE();
			    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';



			-- loading silver.erp_px_cat_g1v2
			SET @start_time = GETDATE();
			--6) Truncate Table & Insert Into Silver.erp_px_cat_g1v2
			PRINT'>>6)Truncating Table  Silver.erp_px_cat_g1v2';
			TRUNCATE TABLE Silver.erp_px_cat_g1v2;
			PRINT'>>6)Inserting Table  Silver.erp_px_cat_g1v2'
			INSERT INTO Silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat
				,maintenance
			)
			select 
				id,
				cat,
				subcat,
				maintenance
			from Bronze.erp_px_cat_g1v2;
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
END
