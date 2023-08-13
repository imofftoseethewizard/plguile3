#! /usr/bin/env bash

# Update package lists
sudo apt update

# Install build essentials (gcc, make, etc.)
sudo apt install -y build-essential

# Install clang/llvm to build .bc files for Postgres' JIT
sudo apt-get install -y clang llvm

# Install PostgreSQL development libraries
sudo apt install -y postgresql-server-dev-all

# Install Guile and its development libraries
sudo apt install -y guile-3.0 guile-3.0-dev

echo "Development dependencies for 'scruple' have been installed successfully."
