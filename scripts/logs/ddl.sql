/*
DDL Script: Create logs tables
*/


CREATE TYPE logs.severity_names AS
ENUM('PASS', 'CRITICAL', 'WARNING');
COMMENT ON TYPE logs.severity_names IS 'PASS: there is no error and can continue, WARNING: somthing happend but it can procedere, CRITICAL: A critical error happend so it need to be stoped the pipeline';

CREATE TABLE IF NOT EXISTS logs.data_quality(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  layer_name VARCHAR(50) NOT NULL,
  table_name VARCHAR(50) NOT NULL,
  check_name VARCHAR(50) NOT NULL,
  error_count INT NOT NULL,
  severity logs.severity_names NOT NULL,
  executed_by VARCHAR(50) NOT NULL DEFAULT CURRENT_USER, -- Automatic trazability with postgresql user
  registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE logs.data_quality IS 'Central audit table for Data Quality checks across the Medallion architecture.';

