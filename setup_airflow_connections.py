"""
Setup Airflow connections and variables from .env file.

Run inside the Airflow container:
  docker compose exec apiserver python /opt/airflow/setup_airflow_connections.py

Reads credentials from .env (mounted read-only) so nothing is hardcoded.
Passwords are Fernet-encrypted automatically when stored in the DB.
"""
import os
import json
from pathlib import Path
from airflow.models.connection import Connection
from airflow.models import Variable
from airflow.settings import Session

# Load .env file into os.environ
_env_file = Path("/opt/airflow/.env")
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if key and value:
            os.environ.setdefault(key.strip(), value.strip())

session = Session()


def env(key, default=""):
    return os.environ.get(key, default)


# ─────────────────────────────────────────────────────────────
# Connections
# ─────────────────────────────────────────────────────────────

CONNECTIONS = [
    Connection(
        conn_id="alloga_ftp",
        conn_type="ftp",
        host=env("ALLOGA_FTP_HOST"),
        login=env("ALLOGA_FTP_LOGIN"),
        password=env("ALLOGA_FTP_PASSWORD"),
        port=int(env("ALLOGA_FTP_PORT", "21")),
        extra=json.dumps({"protocol": "ftp"}),
    ),
    Connection(
        conn_id="logicommerce_api",
        conn_type="http",
        host=env("LOGICOMMERCE_HOST"),
        extra=json.dumps({
            "app_id": env("LOGICOMMERCE_APP_ID"),
            "secret": env("LOGICOMMERCE_SECRET"),
        }),
    ),
    Connection(
        conn_id="ecoceutics_api",
        conn_type="http",
        host=env("ECOCEUTICS_HOST"),
        extra=json.dumps({
            "api_key": env("ECOCEUTICS_API_KEY"),
        }),
    ),
    Connection(
        conn_id="ecovital_ssh",
        conn_type="ssh",
        host=env("ECOVITAL_SSH_HOST"),
        login=env("ECOVITAL_SSH_LOGIN"),
        password="",
        port=int(env("ECOVITAL_SSH_PORT", "22")),
        extra=json.dumps({
            "key_file": env("ECOVITAL_SSH_KEY_FILE"),
        }),
    ),
    Connection(
        conn_id="ecovital_db",
        conn_type="mysql",
        host=env("ECOVITAL_DB_HOST"),
        login=env("ECOVITAL_DB_LOGIN"),
        password=env("ECOVITAL_DB_PASSWORD"),
        port=int(env("ECOVITAL_DB_PORT", "3306")),
        schema=env("ECOVITAL_DB_SCHEMA"),
    ),
    Connection(
        conn_id="novedades_acords_db",
        conn_type="mssql",
        host=env("NOVEDADES_ACORDS_DB_HOST"),
        login=env("NOVEDADES_ACORDS_DB_LOGIN"),
        password=env("NOVEDADES_ACORDS_DB_PASSWORD"),
        port=int(env("NOVEDADES_ACORDS_DB_PORT", "1433")),
        schema=env("NOVEDADES_ACORDS_DB_SCHEMA"),
    ),
    Connection(
        conn_id="novedades_products_db",
        conn_type="mssql",
        host=env("NOVEDADES_PRODUCTS_DB_HOST"),
        login=env("NOVEDADES_PRODUCTS_DB_LOGIN"),
        password=env("NOVEDADES_PRODUCTS_DB_PASSWORD"),
        port=int(env("NOVEDADES_PRODUCTS_DB_PORT", "1433")),
        schema=env("NOVEDADES_PRODUCTS_DB_SCHEMA"),
    ),
    Connection(
        conn_id="zoho_crm",
        conn_type="generic",
        login=env("ZOHO_CLIENT_ID"),
        password=env("ZOHO_CLIENT_SECRET"),
        extra=json.dumps({
            "refresh_token": env("ZOHO_REFRESH_TOKEN"),
            "redirect_uri": env("ZOHO_REDIRECT_URI"),
        }),
    ),
]


# ─────────────────────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────────────────────

VARIABLES = {
    "ecovital_output_path": env("ECOVITAL_OUTPUT_PATH", "/opt/airflow/dags/output/EcoVital_FactTable.csv"),
    "ecovital_ftp_remote_path": env("ECOVITAL_FTP_REMOTE_PATH", "fichero/EcoVital_FactTable.csv"),
    "ecovital_api_rate_limit": env("ECOVITAL_API_RATE_LIMIT", "0.3"),
    "ecovital_tax_mapping": env("ECOVITAL_TAX_MAPPING", '{"1": 21, "2": 10, "3": 4}'),
}


# ─────────────────────────────────────────────────────────────
# Apply
# ─────────────────────────────────────────────────────────────

def upsert_connections():
    for conn in CONNECTIONS:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == conn.conn_id)
            .first()
        )
        if existing:
            session.delete(existing)
            session.commit()
            print(f"  [updated] {conn.conn_id}")
        else:
            print(f"  [created] {conn.conn_id}")
        session.add(conn)
        session.commit()


def upsert_variables():
    for key, value in VARIABLES.items():
        Variable.set(key, value)
        print(f"  [set] {key}")


if __name__ == "__main__":
    print("Creating connections...")
    upsert_connections()
    print("\nSetting variables...")
    upsert_variables()
    print("\nDone. All connections and variables are ready.")
    session.close()
