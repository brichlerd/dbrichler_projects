#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${POSTGRES_DB:-dbt_sigma_poc}"
DB_SUPERUSER="${POSTGRES_USER:-postgres}"
APP_USER="${APP_USER:-sigmacomputing_poc}"
APP_PASSWORD="${APP_PASSWORD:-leaflink123}"
TARGET_SCHEMA="${TARGET_SCHEMA:-hospital_beds}"

# Create the app role if it doesn't exist
psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d postgres <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${APP_USER}') THEN
    CREATE ROLE ${APP_USER} LOGIN PASSWORD '${APP_PASSWORD}';
  END IF;
END
\$\$;
SQL

# Grant ability to create schemas in the DB
psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" \
  -c "GRANT CREATE ON DATABASE ${DB_NAME} TO ${APP_USER};"

# Create target schema and set ownership
psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" \
  -c "CREATE SCHEMA IF NOT EXISTS ${TARGET_SCHEMA} AUTHORIZATION ${APP_USER};"

# Nice-to-haves: usage + default privileges for future objects in the schema
psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" \
  -c "GRANT USAGE ON SCHEMA ${TARGET_SCHEMA} TO ${APP_USER};"

psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" \
  -c "ALTER DEFAULT PRIVILEGES IN SCHEMA ${TARGET_SCHEMA} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${APP_USER};"

psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" \
  -c "ALTER DEFAULT PRIVILEGES IN SCHEMA ${TARGET_SCHEMA} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO ${APP_USER};"

echo "Schema '${TARGET_SCHEMA}' ready for user '${APP_USER}'."
