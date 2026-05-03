CREATE OR REPLACE FUNCTION logs.fn_audit_table_quality(p_layer_name TEXT, p_table_name TEXT, p_column_id TEXT)

RETURNS VOID AS $$
DECLARE
  v_total_rows BIGINT;
  v_count_nulls BIGINT;
  v_count_dup BIGINT;
  v_error_count INT;
  v_severity_name logs.severity_names;
BEGIN

  -- 1. Check for insert values
  EXECUTE format('SELECT COALESCE(COUNT(*), 0) FROM %I.%I', p_layer_name, p_table_name)
  INTO v_total_rows;

  IF v_total_rows = 0 THEN
    v_severity_name = 'CRITICAL';
    v_error_count = 1;
  ELSE
    v_severity_name = 'PASS';
    v_error_count = 0;
  END IF;

  INSERT INTO logs.data_quality(layer_name, table_name, check_name, error_count, severity)
  VALUES
    (p_layer_name, p_table_name, 'INSERT_VALUE_CHECK', v_error_count, v_severity_name);


  -- 2. Check for null values
  EXECUTE format('SELECT COALESCE(COUNT(*), 0) FROM %I.%I WHERE %I IS NULL', p_layer_name, p_table_name, p_column_id)
  INTO v_count_nulls;

  IF v_count_nulls = 0 THEN
    v_severity_name = 'PASS';
    v_count_nulls = 0;
  ELSE
    v_severity_name = 'WARNING';
  END IF;

  INSERT INTO logs.data_quality(layer_name, table_name, check_name, error_count, severity)
  VALUES
    (p_layer_name, p_table_name, 'NULL_VALUE_CHECK', v_count_nulls, v_severity_name);


  -- 3. Check for duplicate values
  EXECUTE format('SELECT COALESCE(COUNT(*) - COUNT(DISTINCT %I), 0) FROM %I.%I', p_column_id, p_layer_name, p_table_name, p_column_id)
  INTO v_count_dup;

  IF v_count_dup = 0 THEN
    v_severity_name = 'PASS';
    v_count_dup = 0;
  ELSE
    v_severity_name = 'WARNING';
  END IF;

  INSERT INTO logs.data_quality(layer_name, table_name, check_name, error_count, severity)
  VALUES
    (p_layer_name, p_table_name, 'DUPLICATE_KEY_CHECK', v_count_dup, v_severity_name);
END; $$
LANGUAGE plpgsql;

