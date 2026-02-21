--NOTE:
-- cst_firstname and cst_lastname With inecessary spaces. Need to trim
SELECT
  *
FROM
  bronze.crm_cust_info
LIMIT 100;

SELECT
  cst_key,
  COUNT(*)
FROM
  bronze.crm_cust_info
GROUP BY cst_key
HAVING COUNT(*) > 1;

--NOTE:
-- cst_gndr is one character M: Male, F: Female
SELECT
  cst_gndr,
  COUNT(*)
FROM
  bronze.crm_cust_info
GROUP BY cst_gndr;

--NOTE:
-- cst_martial_status is one character M: Married, S: Single
SELECT
  cst_martial_status,
  COUNT(*)
FROM
  bronze.crm_cust_info
GROUP BY cst_martial_status;

--NOTE:
-- The most recent created record is the one with more complete data
-- With nulls there is no much to do. No complete data only cst_key
WITH cte_duplicate_cst_id AS (
  SELECT
    cst_id,
    COUNT(*)
  FROM
    bronze.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1
)
SELECT
  *
FROM
  bronze.crm_cust_info
WHERE
  cst_id IN (SELECT cst_id FROM cte_duplicate_cst_id) OR  cst_id IS NULL;
