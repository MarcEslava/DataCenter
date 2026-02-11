# Pipeline

Data pipeline with Airflow, Kafka, Spark, and PostgreSQL.

## Prerequisites

- Docker & Docker Compose
- Bash

## Deploy

```bash
# 1. Configure environment
cp env_example.txt .env
cp -r secrets_example secrets

# 2. Start services
bash setup.sh          # production
bash setup.sh --dev    # includes mongo, reverse-proxy
```

## Services

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| Control Center | http://localhost:9021 |
| Spark UI | http://localhost:9090 |
| Schema Registry | http://localhost:8081 |

## Create Airflow User

```bash
docker compose run --rm apiserver airflow users create \
  --username admin --role Admin --email admin@local --password admin
```

## Commands

```bash
docker compose up -d         # start
docker compose down          # stop
docker compose logs -f       # logs
docker compose ps            # status
```

## Project Structure

```
pipeline/
├── apps/               # Python apps
├── conf/               # Airflow & httpd config
├── dags/               # Airflow DAGs
├── data/               # Data directory
├── secrets/            # Kafka secrets (not in git)
├── docker-compose.yaml
├── Dockerfile          # python-master image
├── Dockerfile.airflow  # airflow-custom image
└── setup.sh            # Deploy script
```
