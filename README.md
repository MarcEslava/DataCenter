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
bash setup.sh --dev    # local dev: also starts mongo, reverse-proxy, db-tunnel
```

### Local-dev-only: SSH DB tunnel

`db-tunnel` runs **only** under the `dev` profile (`setup.sh --dev` / `docker compose --profile dev up`).
It port-forwards the internal DBs through the bastion so local containers can reach them at
host `db-tunnel`. **Production connects to the DBs directly, so the tunnel is not started there.**

These `.env` variables are therefore **local-dev only** (leave them unset in prod):

```bash
BASTION_HOST=            BASTION_PORT=22            BASTION_USER=
ECOFAMS_REMOTE_HOST=     ECOFAMS_REMOTE_PORT=3306
ECOEXTRACT_REMOTE_HOST=  ECOEXTRACT_REMOTE_PORT=3306
BIFARMA_REMOTE_HOST=     BIFARMA_REMOTE_PORT=1433
SSH_KEY_DIR=~/.ssh       SSH_KEY_NAME=id_ed25519
```

In local dev the Airflow DB connections point at host `db-tunnel`; in prod they point at the
real DB hosts directly.

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
