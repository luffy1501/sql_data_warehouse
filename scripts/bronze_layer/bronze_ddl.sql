/*
======================================================
DDL Scripts
======================================================
This Scripts creates table  in the bronze schema dropping the
table if already exist.
Run this script and redefine the bronze DDL layer
*/




IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_Firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gender VARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_date DATE,
prd_end_date DATE
);


IF OBJECT_ID('bronze.crm_sales_detail','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_detail;

CREATE TABLE bronze.crm_sales_detail(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt NVARCHAR(50),
sls_ship_dt NVARCHAR(50),
sls_due_dt NVARCHAR(50),
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_LOCA101','U') IS NOT NULL
	DROP TABLE bronze.erp_LOCA101;

CREATE TABLE bronze.erp_LOCA101(
CID NVARCHAR(50),
CNTRY VARCHAR(50)
);

IF OBJECT_ID(' bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE  bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50)
);

