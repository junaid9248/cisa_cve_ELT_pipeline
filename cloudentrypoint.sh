#!/bin/bash
set -e

echo "Running dbt clean up command..."

cd /app/dbt
dbt deps
dbt clean 
dbt compile

cd ..

echo 'Executing container override commands now...'
exec "$@"
