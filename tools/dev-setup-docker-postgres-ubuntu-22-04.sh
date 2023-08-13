#!/usr/bin/env bash

# Ensure the script fails on any error
set -e

# Check if password argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 [password]"
    echo "Please provide a password as the first argument."
    exit 1
fi

PASSWORD="$1"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Installing Docker..."

    # Update package database
    sudo apt-get update

    # Install prerequisites
    sudo apt-get install -y \
         apt-transport-https \
         ca-certificates \
         curl \
         software-properties-common

    # Add Docker's GPG key
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

    # Add Docker apt repository
    sudo add-apt-repository \
         "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

    # Update package database again
    sudo apt-get update

    # Install Docker
    sudo apt-get install -y docker-ce

    # Start and enable Docker
    sudo systemctl start docker
    sudo systemctl enable docker

    # Adding current user to Docker group (to run Docker without sudo)
    sudo usermod -aG docker $USER

    echo "Docker installed successfully."
else
    echo "Docker is already installed."
fi

# Container name
CONTAINER_NAME="pg_scruple_test"
IMAGE_NAME="pg14-guile3.0"

# Check if a PostgreSQL Docker container with the specified name exists
if [ $(docker ps -a -f "name=$CONTAINER_NAME" --format "{{.Names}}" | grep -w $CONTAINER_NAME | wc -l) -eq 0 ]; then

    echo "PostgreSQL Docker container does not exist. Creating and starting PostgreSQL..."

    # Pull the official PostgreSQL Docker image if not present
    if [ $(docker images -q postgres:14 | wc -l) -eq 0 ]; then
        docker pull postgres:14
    fi

    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    echo docker build -t $IMAGE_NAME $(dirname "$0")/../test
    docker build -t $IMAGE_NAME $(dirname "$0")/../test

    # Create and start a new PostgreSQL container with port mapping
    docker run --name $CONTAINER_NAME -e POSTGRES_PASSWORD=$PASSWORD -p $HOST_PORT:5432 -d $IMAGE_NAME

    echo "PostgreSQL started in a new Docker container successfully on port $HOST_PORT."

elif [ $(docker ps -f "name=$CONTAINER_NAME" --format "{{.Names}}" | grep -w $CONTAINER_NAME | wc -l) -eq 0 ]; then

    echo "PostgreSQL Docker container exists but is not running. Starting PostgreSQL..."

    # Start the existing PostgreSQL container
    docker start $CONTAINER_NAME

    echo "PostgreSQL started in existing Docker container successfully."

else
    echo "PostgreSQL Docker container is already running."
fi

# Pause for a few seconds to let the PostgreSQL instance fully initialize before sending commands
sleep 5

# Check the current log_statement setting
CURRENT_LOG_SETTING=$(docker exec $CONTAINER_NAME psql -U postgres -tAc "SHOW log_statement")

if [ "$CURRENT_LOG_SETTING" != "all" ]; then
    docker exec $CONTAINER_NAME psql -U postgres -c "ALTER SYSTEM SET log_statement = 'all';"
    docker restart $CONTAINER_NAME
else
    echo "log_statement is already set to 'all'."
fi
