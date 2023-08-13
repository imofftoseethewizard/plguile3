#!/bin/bash

# Avoids directory permission warnings from running psql during sudo postgres
cd /tmp

# Check if password argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 [password]"
    echo "Please provide a password as the first argument."
    exit 1
fi

PASSWORD="$1"

# Check if PostgreSQL is already installed
if ! command -v psql &> /dev/null; then
    sudo apt update
    sudo apt install -y postgresql postgresql-contrib
    sudo service postgresql start
else
    echo "PostgreSQL is already installed."
fi

# Configure PostgreSQL with the provided password
# First, check if user already exists
USER_EXISTS=$(sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$USER'")

if [ -z "$USER_EXISTS" ]; then
    sudo -u postgres psql -c "CREATE USER $USER WITH SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD '$PASSWORD'"
else
    echo "User $USER already exists in PostgreSQL."
fi

# Check the current log_statement setting
CURRENT_LOG_SETTING=$(sudo -u postgres psql -tAc "SHOW log_statement")

if [ "$CURRENT_LOG_SETTING" != "all" ]; then
    sudo -u postgres psql -c "ALTER SYSTEM SET log_statement = 'all';"
    sudo service postgresql restart
else
    echo "log_statement is already set to 'all'."
fi

# Check if a database for the current user already exists
DB_EXISTS=$(sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='$USER'")

if [ -z "$DB_EXISTS" ]; then
    sudo -u postgres createdb -O $USER $USER
    echo "Database $USER created for user $USER."
else
    echo "Database $USER already exists for user $USER."
fi
