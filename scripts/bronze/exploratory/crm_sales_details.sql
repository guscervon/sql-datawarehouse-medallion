select
  *
from
  bronze.crm_sales_details
limit 20


SELECT
  sls_ord_num,
  COUNT(*)
FROM
  bronze.crm_sales_details
GROUP BY sls_ord_num;

SELECT
  sls_ord_num,
  sls_cust_id,
  sls_sales,
  sls_price
FROM
  bronze.crm_sales_details
WHERE
  sls_price < 0 OR sls_price IS NULL
  OR sls_sales < 0 OR sls_sales IS NULL

SELECT
  sls_ord_num,
  sls_cust_id,
  sls_quantity
FROM
  bronze.crm_sales_details
WHERE
  sls_quantity < 0 OR sls_quantity IS NULL OR sls_quantity % 1 != 0

SELECT
  sls_order_dt,
  sls_ship_dt :: text :: date
FROM
  bronze.crm_sales_details
WHERE
  length(sls_order_dt :: text) != 8 OR sls_order_dt = 0
  OR length(sls_ship_dt :: text) != 8 OR sls_ship_dt = 0
