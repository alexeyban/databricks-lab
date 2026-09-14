#!/usr/bin/env bash
# Script to set up correct paths for Docker compose with new directory structure

# Set environment variables for Docker compose
export DVDRENTAL_SQL_PATH="${DVDRENTAL_SQL_PATH:-$(pwd)/../../../infra/docker/dvdrental.sql}"
export POSTGRES_CONNECTOR_PATH="${POSTGRES_CONNECTOR_PATH:-$(pwd)/../../../ingestion/cdc/postgres-connector.json}"

echo "Environment variables set:"
echo "DVDRENTAL_SQL_PATH=$DVDRENTAL_SQL_PATH"
echo "POSTGRES_CONNECTOR_PATH=$POSTGRES_CONNECTOR_PATH"
