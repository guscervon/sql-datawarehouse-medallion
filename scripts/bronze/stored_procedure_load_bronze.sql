CREATE OR REPLACE PROCEDURE bronze.sp_load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '===================================';


    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-----------------------------------';

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE 'Loading Table bronze.crm_cust_info';
    COPY bronze.crm_cust_info FROM '/datasets/source_crm/cust_info.csv' DELIMITER ',' CSV HEADER;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading bronze.crm_cust_info: %', v_duration_table;


    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE 'Loading Table bronze.crm_prd_info';
    COPY bronze.crm_prd_info FROM '/datasets/source_crm/prd_info.csv' DELIMITER ',' CSV HEADER;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading bronze.crm_prd_info: %', v_duration_table;


    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE 'Loading Table bronze.crm_sales_details';
    COPY bronze.crm_sales_details FROM '/datasets/source_crm/sales_details.csv' DELIMITER ',' CSV HEADER;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading bronze.crm_sales_details: %', v_duration_table;

    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '-----------------------------------';


    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE 'Loading Table bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 FROM '/datasets/source_erp/cust_az12.csv' DELIMITER ',' CSV HEADER;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into bronze.erp_cust_az12', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading bronze.erp_cust_az12: %', v_duration_table;

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE 'Loading Table bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 FROM '/datasets/source_erp/loc_a101.csv' DELIMITER ',' CSV HEADER;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into bronze.erp_loc_a101', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading bronze.erp_loc_a101: %', v_duration_table;

    v_start_time_table := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Truncating Table bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE 'Loading Table bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 FROM '/datasets/source_erp/px_cat_g1v2.csv' DELIMITER ',' CSV HEADER;
    v_end_time_table := CLOCK_TIMESTAMP();

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Loaded % rows into bronze.erp_px_cat_g1v2', v_row_count;

    v_duration_table := v_end_time_table - v_start_time_table;
    RAISE NOTICE 'Duration of loading bronze.erp_px_cat_g1v2: %', v_duration_table;


    v_end_time := CLOCK_TIMESTAMP();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE 'Duration of loading Bronze Layer: %', v_duration;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error loading Bronze Layer: %', SQLERRM;
        RAISE EXCEPTION 'Error Code: %', SQLSTATE;
END;
$$;