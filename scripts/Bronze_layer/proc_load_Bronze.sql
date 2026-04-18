/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER  PROCEDURE Bronze.load_bronze AS 
BEGIN
	DECLARE @Start_time DATETIME , @End_time DATETIME
	BEGIN TRY
		print'=============================';
		print'Loading Bronze Layer';
		print'=============================';

		print'------------------------------';
		print'Loading CRM Table';
		print'------------------------------';

		SET @Start_time = GETDATE();
		PRINT'>> TRUNCATE TABLE: Bronze.crm_cust_info';
		TRUNCATE TABLE Bronze.crm_cust_info;

		PRINT'>> INSERT DATA INTO: Bronze.crm_cust_info';
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\123\Downloads\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();

		PRINT'>> LOAD DURATION: '+ cast(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+'Second';
		PRINT'-----------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT'>> TRUNCATE TABLE: Bronze.crm_prd_info';
		TRUNCATE TABLE Bronze.crm_prd_info;

		PRINT'>> INSERT DATA INTO: Bronze.crm_prd_info';
		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\123\Downloads\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();

		PRINT'>> LOAD DURATION: '+ cast(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+'Second';
		PRINT'-----------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT'>> TRUNCATE TABLE: Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sales_details;

		PRINT'>> INSERT DATA INTO: Bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\123\Downloads\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();

		PRINT'>> LOAD DURATION: '+ cast(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+'Second';
		PRINT'-----------------------------------------------';

		print'------------------------------';
		print'Loading ERP Table';
		print'------------------------------';

		SET @Start_time = GETDATE();
		PRINT'>> TRUNCATE TABLE: Bronze.erp_CUST_AZ12';
		TRUNCATE TABLE Bronze.erp_CUST_AZ12;

		PRINT'>> INSERT DATA INTO: Bronze.erp_CUST_AZ12';
		BULK INSERT Bronze.erp_CUST_AZ12
		FROM 'C:\Users\123\Downloads\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();

		PRINT'>> LOAD DURATION: '+ cast(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+'Second';
		PRINT'-----------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT'>> TRUNCATE TABLE: Bronze.erp_LOC_A101';
		TRUNCATE TABLE Bronze.erp_LOC_A101;

		PRINT'>> INSERT DATA INTO: Bronze.erp_LOC_A101';
		BULK INSERT Bronze.erp_LOC_A101
		FROM 'C:\Users\123\Downloads\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();

		PRINT'>> LOAD DURATION: '+ cast(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+'Second';
		PRINT'-----------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT'>> TRUNCATE TABLE: Bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE Bronze.erp_PX_CAT_G1V2;

		PRINT'>> INSERT DATA INTO: Bronze.erp_PX_CAT_G1V2';
		BULK INSERT Bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\123\Downloads\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();

		PRINT'>> LOAD DURATION: '+ cast(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+'Second';
		PRINT'-----------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT'=====================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'ERROR MESSAGE:'+ ERROR_MESSAGE();
		PRINT'ERROR MESSAGE:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=====================================';
	END CATCH

END
