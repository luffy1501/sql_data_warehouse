/* 
=======================================================================
Stored Procedure : Load Silver Layer (Broze -> Silver)
=======================================================================
Script Purpose:
  This stored procedure performs the ETL(Extract,Transform,Load) process to
  populate the 'silver' schema table from the 'bronze' schema.
Action Performed:
  - Truncate Silver tables.
  - Inserts transformed and cleaned data from Bronze into silver table.

Parameter:
  None.
  This stored procedure does not accept any parameter or return any values.

Usage examples:
  EXEC silver.load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
	PRINT '==========================================';
	PRINT 'Loading Silver Layer';
	PRINT '==========================================';


	PRINT '------------------------------------------';
	PRINT 'Loading CRM Table';
	PRINT '------------------------------------------';
	SET @start_time = GETDATE();
PRINT'Truncating silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
PRINT'Inserting into silver.crm_cust_info'
INSERT INTO silver.crm_cust_info(
cst_id ,
cst_key ,
cst_Firstname ,
cst_lastname ,
cst_marital_status ,
cst_gender ,
cst_create_date 
)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gender)) ='F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gender)) ='M' THEN 'Male'
	ELSE 'n/a'
END cst_gender,
cst_create_date

FROM(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t 
WHERE flag_last = 1 
ORDER BY cst_id, cst_create_date DESC;
SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


PRINT'Truncating silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info
PRINT'Inserting silver.crm_prd_info'
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_date,
	prd_end_date
)
SELECT
prd_id,

REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL (prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	Else 'n/a'
End prd_line,
prd_start_date,

DATEADD(DAY, -1,LEAD(prd_start_date)OVER (PARTITION BY prd_key ORDER BY prd_start_date)) AS prd_end_date
FROM bronze.crm_prd_info
SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


SET @start_time = GETDATE();
PRINT 'Truncating silver.crm_sales_detail '
TRUNCATE TABLE silver.crm_sales_detail
PRINT 'Inserting silver.crm_sales_detail '
INSERT INTO silver.crm_sales_detail(
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
SELECT
sls_ord_num,
[sls_prd_key],
[sls_cust_id],

CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
END AS sls_order_dt,

CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
END AS sls_ship_dt,

CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
END AS sls_due_dt,


CASE WHEN sls_sales IS NULL 
OR sls_sales <=0 
OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity* ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,


[sls_quantity],
CASE WHEN sls_price IS NULL 
OR sls_price <=0 
THEN sls_sales / NULLIF(sls_quantity,0)
		
	ELSE sls_price
END AS sls_price


FROM bronze.crm_sales_detail
SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


	PRINT '------------------------------------------';
	PRINT 'Loading ERP Table';
	PRINT '------------------------------------------';


SET @start_time = GETDATE();

PRINT 'Truncating silver.erp_CUST_AZ12'
TRUNCATE TABLE silver.erp_CUST_AZ12
PRINT'Inserting into silver.erp_CUST_AZ12'
INSERT INTO silver.erp_CUST_AZ12(
CID,
BDATE,
GEN)
SELECT 
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4, LEN(CID))
	ELSE CID
END AS CID,
CASE WHEN BDATE > GETDATE() OR BDATE< '1924-01-01' THEN NULL
	ELSE BDATE
END AS BDATE,
CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE' )THEN 'Female'
	WHEN UPPER(TRIM(GEN )) IN ('M','MALE')THEN 'Male'
	ELSE 'n/a'	
END AS GEN
	

FROM bronze.erp_cust_AZ12

SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


SET @start_time = GETDATE();
PRINT'Truncating silver.erp_LOCA101 '
TRUNCATE TABLE silver.erp_LOCA101
PRINT 'Inserting into silver.erp_LOCA101'
INSERT INTO silver.erp_LOCA101(
cid,
cntry)
SELECT
REPLACE(cid,'-','')cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	ELSE TRiM(cntry)
END cntry
FROM bronze.erp_LOCA101
SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';


SET @start_time = GETDATE();
PRINT 'Truncating silver.erp_PX_CAT_G1V2'
TRUNCATE TABLE silver.erp_PX_CAT_G1V2
PRINT 'Inserting  into silver.erp_PX_CAT_G1V2'
INSERT INTO silver.erp_PX_CAT_G1V2(
id,
cat,
subcat,
maintenance)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_PX_CAT_G1V2
SET @end_time = GETDATE();
	PRINT '>>Load Duration:' + CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>----------------------------';
	SET @batch_end_time = GETDATE();
	PRINT '======================================================================';
	PRINT '>>Loading Silver Layer ';
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

