/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ===============================
-- Checking : Silver.crm_prd_info 
-- ===============================

-- Checks For Nulls Or Duplicates In Primary Key
-- Expectaion: No Result
select 
	prd,
	count(*)
from Silver.crm_prd_info
group by prd
having 	count(*) >1;

-- ===============================
-- Checking : Silver.crm_prd_info 
-- ===============================

-- Check For Unwated Spaces
-- Expectaion: No Result
select prd_nm
from Silver.crm_prd_info
WHERE prd_nm != LTRIM(RTRIM(prd_nm)); -- USE LTRIM(RTRIM()) here,because i use old version


-- ===============================
-- Checking : Silver.crm_prd_info 
-- ===============================

-- Check For NULLS OR Negative Numbers
-- Expectaion: No Result
select prd_cost
from Silver.crm_prd_info
where prd_cost < 0 or prd_cost IS NULL


-- ===============================
-- Checking : Silver.erp_CUST_AZ12 
-- ===============================

-- Identify Out-Of-Range Dates
select distinct
	bdate
from Silver.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


-- ==================================
-- Checking : Silver.erp_PX_CAT_G1V2 
-- ==================================

-- Data Standarization & Consistency
select distinct
	maintenance
from Silver.erp_PX_CAT_G1V2;



-- ===============================
-- Checking : Silver.crm_prd_info
-- ===============================

-- Check For Invaild Date Orders
select 
	*
from Silver.crm_prd_info b
where b.prd_end_dt < b.prd_start_dt;
