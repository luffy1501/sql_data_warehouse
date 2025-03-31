



IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gender VARCHAR(50),
cst_create_date DATE,
dwh_cereate_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_date DATE,
prd_end_date DATE,
dwh_cereate_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.crm_sales_detail','U') IS NOT NULL
	DROP TABLE silver.crm_sales_detail;

CREATE TABLE silver.crm_sales_detail(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_cereate_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50),
dwh_cereate_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_LOCA101','U') IS NOT NULL
	DROP TABLE silver.erp_LOCA101;

CREATE TABLE silver.erp_LOCA101(
CID NVARCHAR(50),
CNTRY VARCHAR(50),
dwh_cereate_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE  silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50),
dwh_cereate_date DATETIME2 DEFAULT GETDATE()
);


