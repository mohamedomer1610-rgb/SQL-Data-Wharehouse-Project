/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/ 
-- ============================================
--	Create Dimension: Gold.dim_customer
-- ============================================

IF OBJECT_ID('Gold.dim_customer','V') IS NOT NULL
	DROP VIEW Gold.dim_customer;
GO 

	CREATE VIEW Gold.dim_customer AS 
	SELECT 
			ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
			ci.cst_id                        AS customer_id,
			ci.cst_key                       AS customer_number,
			ci.cst_firstname                 AS first_name,
			ci.cst_lastname				     AS last_name,
			lo.cntry                         AS country,
			ci.cst_material_status           AS  martal_status,
			CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
					ELSE COALESCE( az.gen,'N/A')
			END AS new_gen,
			az.bdate						  AS birthdate,
			ci.cst_create_date				  AS create_date
	FROM Silver.crm_cust_info ci
	LEFT JOIN Silver.erp_cust_az12 AS az 
	ON ci.cst_key = az.cid
	LEFT JOIN Silver.erp_loc_a101 AS lo 
	ON ci.cst_key = lo.cid;
GO


-- ============================================
--	Create Dimension: Gold.product_dim
-- ============================================

IF OBJECT_ID('Gold.product_dim','V') IS NOT NULL
	DROP VIEW Gold.product_dim ;
GO 

CREATE VIEW Gold.product_dim AS
SELECT 
		ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt ,pr.prd_key ) AS proudact_key,
		pr.prd AS proudact_id,
		pr.prd_key							AS proudact_namber,
		pr.prd_nm							AS proudact_name,
		pr.cat_id							AS category_id,
		px.cat								AS categoury,
		px.subcat						    AS subcategoury,
		px.maintenance ,
		pr.prd_cost							AS cost,
		pr.prd_line						    AS proudact_line,
		pr.prd_start_dt						AS start_date
FROM Silver.crm_prd_info pr
LEFT JOIN  Silver.erp_px_cat_g1v2 AS px 
	ON pr.cat_id = px.id
WHERE pr.prd_end_dt IS NULL;-- filter out all historical data
GO
 
-- ============================================
--	Create Dimension: Gold.fact_sales
-- ============================================

IF OBJECT_ID('Gold.fact_sales','V') IS NOT NULL
	DROP VIEW Gold.fact_sales;
GO 

	CREATE VIEW Gold.fact_sales AS
	SELECT 
		sa.sls_ord_num				AS order_number, 
		pr.proudact_key ,
		cu.customer_key,
		sa.sls_order_dt				AS order_date,
		sa.sls_ship_dt				AS shipping_date,
		sa.sls_due_dt				AS due_date,
		sa.sls_sales				AS sales_amount,
		sa.sls_quantity				AS quantity,
		sa.sls_price				AS price
	FROM Silver.crm_sales_details sa
	LEFT JOIN Gold.product_dim AS pr
		ON sa.sls_prd_key = pr.proudact_namber
	LEFT JOIN Gold.dim_customer AS cu
		ON sa.sls_cust_id = cu.customer_id;
GO
