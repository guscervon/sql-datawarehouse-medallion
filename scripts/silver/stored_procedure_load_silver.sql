CREATE OR REPLACE PROCEDURE silver.sp_load_silver_data()
LANGUAGE PLPGSQL
AS $$
DECLARE
    v_row_count INT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_start_time_table TIMESTAMP;
    v_end_time_table TIMESTAMP;
    v_duration_table INTERVAL;
BEGIN

    v_start_time := CLOCK_TIMESTAMP();

    RAISE NOTICE '===================================';
    RAISE NOTICE 'Loading Silver Layer Data';
    RAISE NOTICE '===================================';


    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'Inserting data into silver.crm_cust_info from bronze.crm_cust_info';
    INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_martial_status, cst_gndr, cst_create_date)
    WITH cte_format_crm_cust_info AS (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_martial_status DESC) AS rn_flag_last -- Column to identify last record for same id
      FROM
        bronze.crm_cust_info
      WHERE
        cst_id IS NOT NULL
    )
    SELECT
      cst_id,
      cst_key,
      TRIM(cst_firstname) AS cst_firstname,
      TRIM(cst_lastname) AS cst_lastname,
      CASE
        WHEN TRIM(UPPER(cst_martial_status)) = 'M' THEN 'Married'
        WHEN TRIM(UPPER(cst_martial_status)) = 'S' THEN 'Single'
        WHEN TRIM(UPPER(cst_martial_status)) IS NULL THEN 'N/A'
      ELSE NULL END AS cst_martial_status, -- Normalize marital status
      CASE
        WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
        WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
        WHEN TRIM(UPPER(cst_gndr)) IS NULL THEN 'N/A'
      ELSE NULL END AS cst_gender, -- Normalize gender
      cst_create_date
    FROM
      cte_format_crm_cust_info
    WHERE
      rn_flag_last = 1;

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into silver.crm_cust_info', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading silver.crm_cust_info data: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE 'Loading Table silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT
      prd_id,
      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract and format category id
      substring(prd_key, 7, length(prd_key)) AS prd_key, -- Extract prd_key
      prd_nm,
      COALESCE(prd_cost, 0) AS prd_cost,
      CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
      ELSE 'N/A' END prd_line, -- Normalize prd_line
      prd_start_dt :: date AS prd_start_dt,
      (LEAD(prd_start_dt, 1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) :: date AS prd_end_dt -- Fix invalid end_dt
    FROM
      bronze.crm_prd_info;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into silver.crm_prd_info', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading silver.crm_prd_info: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE 'Loading Table silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE
        WHEN length(sls_order_dt :: text) != 8 OR sls_order_dt = 0 THEN NULL
      ELSE sls_order_dt :: text :: date END AS sls_order_dt,
      CASE
        WHEN length(sls_ship_dt :: text) != 8 OR sls_ship_dt = 0 THEN NULL
      ELSE sls_ship_dt :: text :: date END AS sls_ship_dt,
      CASE
        WHEN length(sls_due_dt :: text) != 8 OR sls_due_dt = 0 THEN NULL
      ELSE sls_due_dt :: text :: date END AS sls_due_dt,
      CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity * sls_price) THEN ABS(sls_quantity * sls_price)
      ELSE sls_sales END AS sls_sales,
      sls_quantity,
      CASE
        WHEN sls_quantity != 0 AND (sls_price IS NULL OR sls_price <= 0) THEN ABS(sls_sales / sls_quantity)
      ELSE sls_price END AS sls_price
    FROM
      bronze.crm_sales_details;

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into silver.crm_sales_details', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading silver.crm_sales_details: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '-----------------------------------';


    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE 'Loading Table silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
    SELECT
      CASE
        WHEN length(cid) > 10 THEN SUBSTRING(cid, 4, length(cid))
      ELSE cid END AS cid,
      CASE
        WHEN bdate:: date > CURRENT_DATE THEN NULL
        WHEN (CURRENT_DATE - bdate :: date) / 365 > 100 THEN NULL
      ELSE bdate:: date END AS bdate,
      CASE
        WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a' END AS gen
    FROM
      bronze.erp_cust_az12;

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into silver.erp_cust_az12', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading silver.erp_cust_az12: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE 'Loading Table silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT
      REPLACE(cid, '-', '') AS cid,
      CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
      ELSE TRIM(cntry) END AS cntry
    FROM
      bronze.erp_loc_a101;

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into silver.erp_loc_a101', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading silver.erp_loc_a101: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Loading Table silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    select
      trim(id),
      trim(cat),
      trim(subcat),
      trim(maintenance)
    from
      bronze.erp_px_cat_g1v2;
        v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into silver.erp_px_cat_g1v2', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading silver.erp_px_cat_g1v2: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_end_time := CLOCK_TIMESTAMP();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE 'Duration of loading silver.Layer: %', v_duration;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error loading silver.Layer: %', SQLERRM;
        RAISE EXCEPTION 'Error Code: %', SQLSTATE;
END;
$$;
