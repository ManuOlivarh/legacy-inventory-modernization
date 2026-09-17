#!/usr/bin/env bash
# Applies the legacy ERP scripts in order. Every script is idempotent, so this
# is safe to run on every `docker compose up`.
set -euo pipefail

for script in /init/sql/*.sql; do
  echo "applying ${script##*/}"
  /opt/mssql-tools18/bin/sqlcmd -C -b -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" \
    -v DEBEZIUM_PASSWORD="$DEBEZIUM_PASSWORD" ERP_APP_PASSWORD="$ERP_APP_PASSWORD" \
    -i "$script"
done
echo "legacy ERP ready"
