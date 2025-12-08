# dbt_sigma_poc/Dockerfile
FROM postgres:16.6

# Default DB name (compose will also set this)
ENV POSTGRES_DB=healthcare_claims_db

# Copy init scripts and seed files into the standard init directory.
# Everything in /docker-entrypoint-initdb.d runs automatically on first container start.
COPY init/ /docker-entrypoint-initdb.d/
COPY seeds/ /docker-entrypoint-initdb.d/seeds/

# Ensure scripts are executable
RUN chmod -R 755 /docker-entrypoint-initdb.d
