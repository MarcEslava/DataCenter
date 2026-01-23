#!/bin/bash
set -euo pipefail

PROFILE=""
[[ "${1:-}" == "--dev" ]] && PROFILE="--profile dev"

[[ ! -f ".env" ]] && { echo "Error: .env file not found. Copy env_example.txt to .env"; exit 1; }
set -a; source .env; set +a

echo "Building and starting services..."
docker compose $PROFILE up -d --build || { echo "Failed to start services"; exit 1; }

echo "Waiting for postgres..."
until docker compose exec -T postgres pg_isready -U "${POSTGRES_USER:-airflow}" >/dev/null 2>&1; do
  sleep 2
done

echo "Running Airflow migrations..."
docker compose run --rm apiserver airflow db migrate

echo ""
echo "Done! Services:"
echo "  Airflow:        http://localhost:8080"
echo "  Control Center: http://localhost:9021"
echo "  Spark UI:       http://localhost:9090"
echo ""
echo "Create admin user:"
echo "  docker compose run --rm apiserver airflow users create \\"
echo "    --username admin --role Admin --email admin@local --password admin"
