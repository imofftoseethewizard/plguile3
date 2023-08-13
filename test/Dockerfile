# Use the official PostgreSQL 14 base image
FROM postgres:14

# Set environment variables to non-interactive (this ensures apt-get won't prompt for input)
ENV DEBIAN_FRONTEND=non-interactive

# Install the dependencies necessary for Guile 3.0
RUN apt-get update && \
    apt-get install -y guile-3.0 guile-3.0-dev && \
        apt-get clean

# Expose the usual PostgreSQL port
EXPOSE 5432
