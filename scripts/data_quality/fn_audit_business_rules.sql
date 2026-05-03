CREATE OR REPLACE FUNCTION logs.fn_audit_business_rules(p_layer_name TEXT, p_table_name TEXT, p_check_name TEXT, p_condition_sql TEXT, p_severity_name logs.severity_names)

RETURNS VOID AS $$
DECLARE
  v_error_count INT;
  v_final_severity logs.severity_names;
BEGIN

  EXECUTE format('SELECT COALESCE(COUNT(*), 0) FROM %I.%I WHERE %s;', p_layer_name, p_table_name, p_condition_sql)
  INTO v_error_count;

  IF v_error_count = 0 THEN
    v_final_severity = 'PASS';
  ELSE
    v_final_severity = p_severity_name;
  END IF;

  INSERT INTO logs.data_quality(layer_name, table_name, check_name, error_count, severity)
  VALUES
    (p_layer_name, p_table_name, p_check_name, v_error_count, v_final_severity);

END; $$
LANGUAGE plpgsql;
