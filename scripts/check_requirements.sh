#!/bin/bash

# Path to the requirements file
REQUIREMENTS_FILE="/opt/airflow/dags/repo/requirements.txt"

# Read the requirements from the file into an array
readarray -t REQUIREMENTS < "$REQUIREMENTS_FILE"

# Iterate over each requirement and check if it is installed
for requirement in "${REQUIREMENTS[@]}"; do
  if ! dpkg -s "$requirement" >/dev/null 2>&1; then
    echo "Requirement '$requirement' is not installed."
  fi
done
