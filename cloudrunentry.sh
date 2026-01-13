#!/bin/bash
set -e

echo "Pulling latest code from the cisa_cve_ELT_pipeline repository..."

# Clone or pull the specific branch
if [ -d "/app/.git" ]; then
    echo "Repository exists, pulling latest changes..."
    cd /app
    git fetch origin
    #switxhiching to dev branch
    git checkout dev-dbt-cloud
    git pull origin dev-dbt-cloud

    echo "Code updated successfully!"
else
    echo "Since the repo does not exist here, cloning repository from the cisa_cve_ELT_pipeline repository..."
    # Fetching from dev-dbt-cloud will change to prod later when published
    #cloned content goes into a temp folder
    git clone -b dev-dbt-cloud https://github.com/junaid9248/cisa_cve_ELT_pipeline /app-temp
    
    # Copyigng all the content into the actual app folder and then removing the temp app
    cp -r /app-temp/* /app/
    cp -r /app-temp/.git /app/
    rm -rf /app-temp

    echo "Cloning succesful!"
fi
# This will execute whatever command is passed to the container built from the Dockerfile that calls this entrypoint
# In our case it will be the container-overrides called from the elt_extraction task triggered by dag run 
exec "$@"