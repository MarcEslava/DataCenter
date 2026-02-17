"""
Setup Airflow connections and variables for EcoVital ETL.

Run inside the Airflow container:
  docker compose exec apiserver python /opt/airflow/dags/setup_airflow_connections.py

Uses Airflow's Connection model so passwords are Fernet-encrypted automatically.
"""
import json
from airflow.models.connection import Connection
from airflow.models import Variable
from airflow.settings import Session

session = Session()


# ─────────────────────────────────────────────────────────────
# Connections
# ─────────────────────────────────────────────────────────────

CONNECTIONS = [
    # ── FTP: Alloga ──
    Connection(
        conn_id="alloga_ftp",
        conn_type="ftp",
        host="ftp.ecoceutics.com",
        login="alloga",
        password="Vx2019Alloga",
        port=735,
        extra=json.dumps({"protocol": "ftp"}),
    ),
    # ── HTTP: LogiCommerce API ──
    Connection(
        conn_id="logicommerce_api",
        conn_type="http",
        host="https://api.logicommerce.net/v1",
        extra=json.dumps({
            "app_id": "pK3c76MsxY",
            "secret": "pK3c76MsxY73eS9F2ke9gAvfBb2x84",
        }),
    ),
    # ── HTTP: Ecoceutics FID API ──
    Connection(
        conn_id="ecoceutics_api",
        conn_type="http",
        host="https://apifidfarma.ecoceutics.com/v1",
        extra=json.dumps({
            "api_key": "657A8288P7156",
        }),
    ),
    # ── SSH: EcoVital DB Tunnel ──
    Connection(
        conn_id="ecovital_ssh",
        conn_type="ssh",
        host="cecobd1.ecoceutics.com",
        login="U4NsrvTwqF",
        password="",
        port=12984,
        extra=json.dumps({
            "key_file": "/root/.ssh/id_ed25519",
        }),
    ),
    # ── MySQL: fidfarma (accessed via SSH tunnel) ──
    Connection(
        conn_id="ecovital_db",
        conn_type="mysql",
        host="127.0.0.1",
        login="wED2iQTl",
        password="BS0jIbTe",
        port=3306,
        schema="fidfarma",
    ),
    # ── Zoho CRM (OAuth2) ──
    # login = client_id, password = client_secret, extra.refresh_token = refresh token
    Connection(
        conn_id="zoho_crm",
        conn_type="generic",
        login="1000.TB4P58146F209UUX572QQK5DGD2G6V",
        password="813e36e342c089dbc89cf14b42e1f1e61c50df47d6",
        extra=json.dumps({
            "refresh_token": "1000.a969c1c21564d6fdf99ddb1ea08fd395.ef9c552deb5ab38ce55d45224117493e",
            "redirect_uri": "https://www.ecoceutics.com/wp-json/bitgfzc/redirect",
        }),
    ),
]


# ─────────────────────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────────────────────

VARIABLES = {
    "ecovital_output_path": "/opt/airflow/dags/output/EcoVital_FactTable.csv",
    "ecovital_ftp_remote_path": "fichero/EcoVital_FactTable.csv",
    "ecovital_api_rate_limit": "0.3",
    "ecovital_tax_mapping": json.dumps({"1": 21, "2": 10, "3": 4}),
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
        print(f"  [set] {key} = {value}")


if __name__ == "__main__":
    print("Creating connections...")
    upsert_connections()
    print("\nSetting variables...")
    upsert_variables()
    print("\nDone. All connections and variables are ready.")
    session.close()

