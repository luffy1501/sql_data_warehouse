/*
===========================================================
STORED PROCEDURE : LOAD BRONZE LAYER(SOURCE -> BRONZE)
===========================================================
Script purpose:
  This Script is use to load data in bronze schema from external csv files.
  In this we use TRUNCATE to clear the data in the table if exsit.
  Then we use BULK_INSERT to load all data  at one time.

Parameter:
    NO
      This stored data does not accept any parameter or any values.

Usage Example:
              EXEC bronze.load_bronze;


*/


CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
	PRINT '==========================================';
	PRINT 'Loading Bronze Layer';
	PRINT '==========================================';


	PRINT '------------------------------------------';
	PRINT 'Loading CRM Table';
	PRINT '------------------------------------------';

	SET @start_time = GETDATE();
PRINT '>>Truncating Table : bronze.crm_cust_info'
TRUNCATE TABLE bronze.crm_cust_info;

PRINT '>>Inserting Data Into : bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info
FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';

	SET @start_time = GETDATE();
PRINT '>>Truncating Table : bronze.crm_prd_info'
TRUNCATE TABLE bronze.crm_prd_info;

PRINT '>>Inserting Data Into : bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info
FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


	SET @start_time = GETDATE();
PRINT '>>Truncating Table : bronze.crm_sales_detail'
TRUNCATE TABLE bronze.crm_sales_detail;

PRINT '>>Inserting Data Into :  bronze.crm_sales_detail'
BULK INSERT bronze.crm_sales_detail
FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


	PRINT '------------------------------------------';
	PRINT 'Loading ERP Table';
	PRINT '------------------------------------------';


	SET @start_time = GETDATE();
PRINT '>>Truncating Table : bronze.erp_CUST_AZ12'
TRUNCATE TABLE bronze.erp_CUST_AZ12;

PRINT '>>Inserting Data Into : bronze.erp_CUST_AZ12'
BULK INSERT bronze.erp_CUST_AZ12
FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


	SET @start_time = GETDATE();
PRINT '>>Truncating Table : bronze.erp_LOCA101'
TRUNCATE TABLE bronze.erp_LOCA101;

PRINT '>>Inserting Data Into : bronze.erp_LOCA101'
BULK INSERT bronze.erp_LOCA101
FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


	SET @start_time = GETDATE();
PRINT '>>Truncating Table : bronze.erp_PX_CAT_G1V2'
TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

PRINT '>>Inserting Data Into : bronze.erp_PX_CAT_G1V2'
BULK INSERT bronze.erp_PX_CAT_G1V2
FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';

	SET @batch_end_time = GETDATE();
	PRINT '======================================================================';
	PRINT '>>Loading Bronze Layer ';
	PRINT '>>Total Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
	PRINT '======================================================================';
END TRY

BEGIN CATCH 
	PRINT '==================================='
	PRINT 'ERROR OCCURED DUEING LOADING BRONZE LAYER'
	PRINT  'Error Message' + ERROR_MESSAGE();
	PRINT  'Error Message' +  CAST (ERROR_MESSAGE() AS NVARCHAR);
	PRINT  'Error Message' + CAST (ERROR_MESSAGE() AS NVARCHAR);
	END CATCH
END
