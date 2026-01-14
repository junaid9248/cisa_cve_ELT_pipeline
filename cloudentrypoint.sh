#!/bin/bash
set -ex
echo "Running dbt clean up command..."
ls -la /app

cd /app/dbt
pwd

dbt deps
dbt clean 
dbt compile

cd ..

echo 'Executing container override commands now...'
exec "$@"
