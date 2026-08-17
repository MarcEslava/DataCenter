# Pipeline

Data pipeline on **Airflow + PostgreSQL**. Kafka, Spark and other extras run **only** under the
`dev` profile — see [Profiles](#profiles).

## Prerequisites

- Docker & Docker Compose
- Bash

## Deploy

```bash
# 1. Configure environment
cp env_example.txt .env
cp -r secrets_example secrets

# 2. Start services
bash setup.sh          # production: Airflow + PostgreSQL only
bash setup.sh --dev    # local dev: also Kafka, Spark, mongo, reverse-proxy, db-tunnel
```

### Profiles

**Production runs a lean core: Airflow (`apiserver`, `scheduler`, `dag-processor`) + `postgres`.**
Everything else — the Kafka stack (`zookeeper`, `broker`, `schema-registry`, `control-center`,
`connect`), Spark (`spark-master`/`spark-worker`), `mongo`, `reverse-proxy`, `db-tunnel`,
`python-master` — is gated behind the `dev` profile and only starts with `setup.sh --dev`
(`docker compose --profile dev up`). This keeps prod light and avoids pulling the heavy
Kafka/Spark images on every deploy. To run one of the extras in prod on demand, start it with
its profile, e.g. `docker compose --profile dev up -d broker`.

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

| Service | URL | Profile |
|---------|-----|---------|
| Airflow | http://localhost:8080 | always |
| Control Center | http://localhost:9021 | dev only |
| Spark UI | http://localhost:9090 | dev only |
| Schema Registry | http://localhost:8081 | dev only |

## Create Airflow User

```bash
docker compose run --rm apiserver airflow users create \
  --username admin --role Admin --email admin@local --password admin
```

## Airflow connections & variables

Connections and variables are provisioned from `.env` (nothing hardcoded), applied with:

```bash
docker compose exec apiserver python /opt/airflow/setup_airflow_connections.py
```

The KPI mailing report ETL (`kpiMailing_ETL`) renders per-pharmacy PDFs with **WeasyPrint**
(bundled in `Dockerfile.airflow`) via the reusable `dags/utils/clsPdf.py` component + Jinja2
templates under `dags/templates/kpi/`. It needs the `bifarma_origen` and `bifarma_agreg_db`
MSSQL connections (the `bifarma_origen` default catalog **must** be `bifarma`), and sends mail
through `zepto_mail`. It defaults to `dry_run=True` — set `test_recipient`, then `dry_run=False`,
to send for real.

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
├── apps/                # Python apps
├── conf/                # Airflow & httpd config
├── dags/                # Airflow DAGs
│   ├── utils/           # clsSQL, clsPdf, ftp, mailers, shared helpers
│   ├── templates/kpi/   # Jinja2 report + email templates (kpiMailing_ETL)
│   └── assets/kpi/      # logo + Montserrat fonts for the KPI PDFs
├── data/                # Data directory
├── docs/                # HTML runbooks / process docs
├── secrets/             # Kafka secrets + local-only creds (not in git)
├── docker-compose.yaml
├── Dockerfile           # python-master image
├── Dockerfile.airflow   # airflow-custom image (ODBC + WeasyPrint)
├── setup_airflow_connections.py  # provision connections/variables from .env
└── setup.sh             # Deploy script
```
