/*
DDL Script: Create gold tables
*/

-- In each table is added dwh_created_at for auditoon

------------------ DIMENSION TABLES --------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_customers (
  customer_key CHAR(32) PRIMARY KEY NOT NULL,
  customer_id INT NOT NULL,
  customer_number VARCHAR(30),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  country VARCHAR(30) NOT NULL,
  marital_status VARCHAR(30) NOT NULL,
  gender VARCHAR(30) NOT NULL,
  birthday DATE,
  created_at TIMESTAMP NOT NULL,
  dwh_registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE gold.dim_customers IS 'Master dimension for customers integrating CRM and ERP data using MD5 surrogate keys';

CREATE TABLE IF NOT EXISTS gold.dim_products(
  product_key CHAR(32) PRIMARY KEY NOT NULL,
  product_id INT NOT NULL,
  product_number VARCHAR(100) NOT NULL,
  product_name VARCHAR(100)NOT NULL,
  category_id VARCHAR(50) NOT NULL,
  category VARCHAR(100),
  subcategory VARCHAR(100),
  maintenance VARCHAR(100),
  cost DECIMAL(10, 2) NOT NULL,
  product_line VARCHAR(100),
  start_date DATE NOT NULL,
  dwh_registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE gold.dim_products IS 'Master dimension for products integrating CRM and ERP data using MD5 surrogate keys';


------------------ FACT TABLES --------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_sales(
  order_key CHAR(32) NOT NULL,
  product_key CHAR(32) NOT NULL REFERENCES gold.dim_products(product_key),
  customer_key CHAR(32) NOT NULL REFERENCES gold.dim_customers(customer_key),
  order_number VARCHAR(50) NOT NULL,
  order_date DATE,
  shipping_date DATE,
  due_date DATE,
  sales_amount INT NOT NULL,
  quantity INT NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  dwh_registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE gold.fact_sales IS 'Master fact for customers and products integrating CRM and ERP data using MD5 surrogate keys';

