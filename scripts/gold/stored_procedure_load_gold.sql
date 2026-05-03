CREATE OR REPLACE PROCEDURE gold.sp_load_golden_data()
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
    RAISE NOTICE 'Loading Golden Layer Data';
    RAISE NOTICE '===================================';


    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Loading Dimensional Tables';
    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table gold.dim_customers';
    TRUNCATE TABLE gold.dim_customers CASCADE;

    RAISE NOTICE 'Inserting data into gold.dim_customers';
    INSERT INTO gold.dim_customers(customer_key, customer_id, customer_number, first_name, last_name, country, marital_status, gender, birthday, created_at)
    SELECT
      MD5(cast(cci.cst_id AS TEXT) || 'crm') AS customer_key, --surrogate_key use for connect our data without depend of the source
      cci.cst_id AS customer_id,
      cci.cst_key AS customer_number,
      cci.cst_firstname AS first_name,
      cci.cst_lastname AS last_name,
      ela.cntry AS country,
      cci.cst_marital_status AS marital_status,
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

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    PERFORM logs.fn_audit_table_quality('gold', 'dim_customers', 'customer_key');
    PERFORM logs.fn_audit_business_rules('gold', 'dim_customers', 'INVALID_GENDER', 'gender NOT IN (''Male'', ''Female'', ''n/a'')', 'WARNING');

    RAISE NOTICE 'Loaded % rows into gold.dim_customers', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading gold.dim_customers data: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table gold.dim_products';
    TRUNCATE TABLE gold.dim_products CASCADE;

    RAISE NOTICE 'Loading Table gold.dim_products';
    INSERT INTO gold.dim_products(product_key, product_id, product_number, product_name, category_id, category, subcategory, maintenance, cost, product_line, start_date)
    SELECT
      MD5(cast(cpi.prd_id AS TEXT) || 'crm') AS product_key,
      cpi.prd_id AS product_id,
      cpi.prd_key AS product_number,
      cpi.prd_nm AS product_name,
      cpi.cat_id AS category_id,
      epcg.cat AS category,
      epcg.subcat AS subcategory,
      epcg.maintenance,
      cpi.prd_cost AS cost,
      cpi.prd_line AS product_line,
      cpi.prd_start_dt AS start_date
    FROM
      silver.crm_prd_info cpi
    LEFT JOIN silver.erp_px_cat_g1v2 epcg
    ON
      epcg.id = cpi.cat_id
    WHERE
      cpi.prd_end_dt IS NULL; -- Getting only active prices

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    PERFORM logs.fn_audit_table_quality('gold', 'dim_products', 'product_key');
    PERFORM logs.fn_audit_business_rules('gold', 'dim_products', 'INVALID_COST', 'cost < 0', 'WARNING');

    RAISE NOTICE 'Loaded % rows into gold.dim_products', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading gold.dim_products data: %', v_duration_table;


    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Loading Fact Tables';
    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table gold.fact_sales';
    TRUNCATE TABLE gold.fact_sales CASCADE;

    RAISE NOTICE 'Loading Table gold.fact_sales';
    INSERT INTO gold.fact_sales(order_key, product_key, customer_key, order_number, order_date, shipping_date, due_date, sales_amount, quantity, price)
    SELECT
      MD5(cast(csd.sls_ord_num AS TEXT) || 'crm') AS order_key,
      dp.product_key,
      dc.customer_key,
      csd.sls_ord_num AS order_number,
      csd.sls_order_dt AS order_date,
      csd.sls_ship_dt AS shipping_date,
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

    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    PERFORM logs.fn_audit_table_quality('gold', 'fact_sales', 'order_key');
    PERFORM logs.fn_audit_business_rules('gold', 'fact_sales', 'INVALID_SHIPPING_DATE', 'shipping_date < order_date', 'WARNING');
    PERFORM logs.fn_audit_business_rules('gold', 'fact_sales', 'INVALID_SALES_AMOUNT', 'sales_amount != quantity * price', 'WARNING');

    RAISE NOTICE 'Loaded % rows into gold.fact_sales', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading gold.fact_sales data: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';

    v_end_time := CLOCK_TIMESTAMP();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE 'Duration of loading gold.Layer: %', v_duration;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error loading gold.Layer: %', SQLERRM;
        RAISE EXCEPTION 'Error Code: %', SQLSTATE;
END;
$$;
