#!/bin/bash

# List with all the sql script for bronze layer
readonly SQL_SCHEMA="./scripts/schemas.sql"
readonly BRONZE_LAYER_SCRIPTS=(
  "./scripts/bronze/ddl.sql"
  "./scripts/bronze/stored_procedure_load_bronze.sql"
)
readonly SILVER_LAYER_SCRIPTS=(
  "./scripts/silver/ddl.sql"
  "./scripts/silver/stored_procedure_load_silver.sql"
)

readonly GOLD_LAYER_SCRIPTS=(
  "./scripts/gold/ddl.sql"
  "./scripts/gold/stored_procedure_load_gold.sql"
)

readonly AUDIT_LOGS_SCRIPTS=(
  "./scripts/logs/ddl.sql"
  "./scripts/data_quality/fn_audit_business_rules.sql"
  "./scripts/data_quality/fn_audit_table_quality.sql"
)

# Check if .env file exists and load environment variables
if [ -f .env ]; then
  echo "Loading environment variables from .env"
  source .env
else
  echo ".env file not found!"
  exit 1

fi

# Run the datawarehouse database using docker compose
echo "Running Data Warehouse..."
docker compose up -d --wait
echo "Database is ready"

sleep 4

# Loading schemas
if [ -f "$SQL_SCHEMA" ]; then
  echo "Loading Schemas"
  psql "$DATABASE_URL" -f "$SQL_SCHEMA"
else
  echo "File not found: $SQL_SCHEMA"

  echo "Turning down docker compose"
  docker compose stop
  docker compose down
  exit 1
fi

#Loading Audit Logs
echo "Loading Audit Logs"

for FILE in "${AUDIT_LOGS_SCRIPTS[@]}"; do
  if [ -f "$FILE" ]; then
    echo "Executing $FILE"
    psql "$DATABASE_URL" -f "$FILE"
  else
    echo "File not found: $FILE"

    echo "Turning down docker compose"
    docker compose stop
    docker compose down
    exit 1
  fi
done

# Loading Bronze Layer
echo "Loading Bronze Layer and Data"

for FILE in "${BRONZE_LAYER_SCRIPTS[@]}"; do
  if [ -f "$FILE" ]; then
    echo "Executing $FILE"
    psql "$DATABASE_URL" -f "$FILE"
  else
    echo "File not found: $FILE"

    echo "Turning down docker compose"
    docker compose stop
    docker compose down
    exit 1
  fi
done
psql "$DATABASE_URL" -c "CALL bronze.sp_load_bronze();"

# Loading Silver Layer
echo "Loading Silver Layer and Data"
for FILE in "${SILVER_LAYER_SCRIPTS[@]}"; do
  if [ -f "$FILE" ]; then
    echo "Executing $FILE"
    psql "$DATABASE_URL" -f "$FILE"
  else
    echo "File not found: $FILE"

    echo "Turning down docker compose"
    docker compose stop
    docker compose down
    exit 1
  fi
done

psql "$DATABASE_URL" -c "CALL silver.sp_load_silver_data();"

# Loading Gold Layer
echo "Loading Gold Layer and Data"
for FILE in "${GOLD_LAYER_SCRIPTS[@]}"; do
  if [ -f "$FILE" ]; then
    echo "Executing $FILE"
    psql "$DATABASE_URL" -f "$FILE"
  else
    echo "File not found: $FILE"

    echo "Turning down docker compose"
    docker compose stop
    docker compose down
    exit 1
  fi
done

psql "$DATABASE_URL" -c "CALL gold.sp_load_golden_data();"
