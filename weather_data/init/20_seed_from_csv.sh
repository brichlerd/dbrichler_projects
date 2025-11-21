#!/usr/bin/env bash
# dbt_sigma_poc/init/20_seed_from_csv.sh
# Auto-creates tables (all TEXT columns) from CSV headers and bulk loads data.
# Also creates/grants the app user.

set -euo pipefail

DB_NAME="${POSTGRES_DB:-dbt_sigma_poc}"
DB_SUPERUSER="${POSTGRES_USER:-postgres}"
APP_USER="${APP_USER:-sigmacomputing_poc}"
APP_PASSWORD="${APP_PASSWORD:-leaflink123}"
SEED_DIR="/docker-entrypoint-initdb.d/seeds"

psql_super() {
  psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" -Atc "$1"
}

echo "==> Ensuring application user '${APP_USER}' exists..."
psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d postgres <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${APP_USER}') THEN
    CREATE ROLE ${APP_USER} LOGIN PASSWORD '${APP_PASSWORD}';
  END IF;
END
\$\$;
SQL

echo "==> Granting database & schema access to '${APP_USER}'..."
psql_super "GRANT CONNECT ON DATABASE ${DB_NAME} TO ${APP_USER};"
psql_super "GRANT USAGE ON SCHEMA public TO ${APP_USER};"
# Future tables/sequences: default privileges
psql_super "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${APP_USER};"
psql_super "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO ${APP_USER};"

if [ -d "$SEED_DIR" ]; then
  shopt -s nullglob
  for csv in "$SEED_DIR"/*.csv; do
    fname="$(basename "$csv")"
    table="${fname%.*}"  # patients.csv -> patients
    echo "==> Preparing table '$table' from '$fname'..."

    # Read header line, normalize CRLF
    IFS= read -r header < "$csv" || true
    header="${header%$'\r'}"

    if [ -z "$header" ]; then
      echo "WARNING: '$fname' has empty header; skipping."
      continue
    fi

    # Split on commas (assumes headers do not contain commas)
    IFS=',' read -r -a cols <<< "$header"

    # Sanitize column names to safe unquoted identifiers (lowercase, a-z0-9_)
    # Ensure they start with a letter or underscore; ensure uniqueness
    declare -a safe_cols=()
    declare -A seen=()
    for raw in "${cols[@]}"; do
      c="$(echo "$raw" | tr '[:upper:]' '[:lower:]')"
      c="$(echo "$c" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"     # trim
      c="$(echo "$c" | sed -E 's/[^a-z0-9_]+/_/g')"                    # non-alnum -> _
      c="$(echo "$c" | sed -E 's/^([0-9])/_\1/')"                      # start with letter/_
      c="$(echo "$c" | sed -E 's/_+/_/g; s/^_+|_+$//g')"               # collapse/trim _
      [ -z "$c" ] && c="col"
      base="$c"; n=1
      while [[ -n "${seen[$c]:-}" ]]; do
        n=$((n+1))
        c="${base}_${n}"
      done
      seen[$c]=1
      safe_cols+=("$c")
    done
    unset seen

    # Build CREATE TABLE ... all TEXT columns
    cols_sql=""
    for c in "${safe_cols[@]}"; do
      cols_sql+="$c TEXT, "
    done
    cols_sql="${cols_sql%, }"

    # Create table if missing
    psql_super "CREATE TABLE IF NOT EXISTS ${table} (${cols_sql});"

    # Load data (server-side COPY)
    # Use CSV HEADER, handle common quoting/escaping; treat empty as NULL
    psql -v ON_ERROR_STOP=1 -U "$DB_SUPERUSER" -d "$DB_NAME" <<SQL
TRUNCATE TABLE ${table};
COPY ${table}
FROM '${csv}'
WITH (FORMAT CSV, HEADER TRUE, QUOTE '"', ESCAPE '"', NULL '');
SQL

    echo "==> Loaded $(wc -l < "$csv" | tr -d '[:space:]') lines into ${table} (includes header)."

  done

  echo "==> Granting table/sequence privileges on existing objects to '${APP_USER}'..."
  psql_super "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${APP_USER};"
  psql_super "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO ${APP_USER};"
else
  echo "NOTE: No seeds directory found at ${SEED_DIR}; skipping CSV load."
fi

echo "==> Seeding complete."
