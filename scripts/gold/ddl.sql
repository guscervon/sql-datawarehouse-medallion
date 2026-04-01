
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
  ROW_NUMBER() OVER(ORDER BY cci.cst_id) AS customer_key, --surrogate_key use for connect our data without depend of the source
  cci.cst_id AS customer_id,
  cci.cst_key AS customer_number,
  cci.cst_firstname AS first_name,
  cci.cst_lastname AS last_name,
  ela.cntry AS country,
  cci.cst_martial_status AS marital_status,
  CASE
    WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
    ELSE COALESCE(eca.gen, 'n/a')
  END AS gender,
  eca.bdate AS birthday,
  cci.cst_create_date AS created_at
FROM
  silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
ON
  cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
ON
  cci.cst_key = ela.cid;

CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
  ROW_NUMBER() OVER(ORDER BY cpi.prd_id) AS product_key,
  cpi.prd_id AS product_id,
  cpi.prd_key AS product_number,
  cpi.prd_nm AS product_name,
  cpi.cat_id AS category_id,
  epcg.cat AS category,
  epcg.subcat AS subcategory,
  epcg.maintenance,
  cpi.prd_cost AS cot,
  cpi.prd_line AS product_line,
  cpi.prd_start_dt AS start_date
FROM
  silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
ON
  epcg.id = cpi.cat_id
WHERE
  cpi.prd_end_dt IS NULL; -- Getting only active prices


CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
  csd.sls_ord_num AS order_number,
  dp.product_key,
  dc.customer_key,
  csd.sls_order_dt AS order_date,
  csd.sls_ship_dt AS shiping_date,
  csd.sls_due_dt AS due_date,
  csd.sls_sales AS sales_amount,
  csd.sls_quantity AS quantity,
  csd.sls_price AS price
FROM
  silver.crm_sales_details csd
LEFT JOIN gold.dim_products dp
ON
  dp.product_number = csd.sls_prd_key
LEFT JOIN gold.dim_customers dc
ON
  dc.customer_id = csd.sls_cust_id;

