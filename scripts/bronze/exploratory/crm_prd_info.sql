SELECT
  prd_id,
  COUNT(*)
FROM
  bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

SELECT
  prd_line,
  COUNT(*)
FROM
  bronze.crm_prd_info
GROUP BY prd_line;

--NOTE:
-- * prd_start_dt doesn't match with the previous prd_end_dt
-- * prd_end_dt is smaller than prd_end_dt
-- * 
WITH cte_diff_dates AS (
  SELECT
    prd_start_dt :: date,
    prd_end_dt :: date,
    prd_end_dt - prd_start_dt AS diff
  FROM
    bronze.crm_prd_info
)
SELECT
  *
FROM
  cte_diff_dates
WHERE
  diff < 0
  OR prd_start_dt IS NULL;

