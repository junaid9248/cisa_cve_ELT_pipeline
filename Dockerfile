# This is the dockerfile for the cloud run job image
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy directories and requirements files from the vm machine first
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY src/ ./src/
COPY secrets/ ./secrets/
COPY dbt/ ./dbt/

#Removed cloud entrypoint.sh call will manually update the vm code folder for now and automate cron job later
#Resolving the bigquery adapter issues by cleaning up target file first and reinitializing all targets
WORKDIR /app/dbt

RUN dbt deps
RUN dbt clean 
RUN dbt compile

WORKDIR /app
