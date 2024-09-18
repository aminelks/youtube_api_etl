
#!/bin/bash
set -e

# Wait for the database to be ready
echo "Waiting for the database..."
while ! nc -z postgres 5432; do
  sleep 1
done

echo "Database is ready."

# Initialize the database
airflow db init

# Create users
airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow