"""
Ecofams ETL

<Describe what this DAG does>
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SQL_CONN_ID  = "BIFarma_db"   # mssql
ACORDS_CONN_ID = "biOps_db"   # mysql
ZOHO_CONN_ID = "zoho_crm"
SSH_CONN_ID  = "ecoextract_ssh"

MAIL_TO = [{"address": "meslava@ecoceutics.com", "name": "Marc Eslava"}]

# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────

_DIALECT_DEFAULTS = {
    "mssql": {"driver": "pymssql", "port": 1433},
    "mysql": {"driver": "pymysql",  "port": 3306},
}

def _query_sql(conn_id: str, sql: str, dialect: str):
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    conn = BaseHook.get_connection(conn_id)
    defs = _DIALECT_DEFAULTS[dialect]
    db = SQLConnection(
        db_host=conn.host, db_port=conn.port or defs["port"],
        db_database=conn.schema, db_username=conn.login,
        db_password=conn.password, dialect=dialect, driver=defs["driver"],
    )
    with db:
        return db.fech_dataframe(sql)

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='ecofams_ETL',
    description='<short description>',
    schedule=Variable.get("ecofams_etl_schedule", default_var="0 3 1 * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)
def ecofams_etl():

    @task
    def extract() -> list[dict]:
        df = _query_sql(SQL_CONN_ID, """
            SELECT id AS idunit, description
            FROM Unit
            WHERE ecoFams = 1 AND ecoBuy = 0
        """, dialect="mssql")
        print(f"Extracted {len(df)} farmacias")
        return df.to_dict("records")

    @task
    def run_scripts(farmacias: list[dict]) -> None:
        import paramiko
        from airflow.hooks.base import BaseHook
        if not farmacias:
            print("No farmacias to process")
            return
        conn = BaseHook.get_connection(SSH_CONN_ID)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=conn.host,
            port=conn.port or 22,
            username=conn.login,
            password=conn.password or None,
        )
        try:
            for farmacia in farmacias:
                idunit = farmacia["idunit"]
                cmd = f"bash /var/www/ecoextract/scripts/ecoextract_batch_art.sh {idunit}"
                _, stdout, stderr = client.exec_command(cmd)
                exit_code = stdout.channel.recv_exit_status()
                out = stdout.read().decode().strip()
                err = stderr.read().decode().strip()
                if exit_code != 0:
                    print(f"[{idunit}] ERROR (exit {exit_code}): {err}")
                else:
                    print(f"[{idunit}] OK: {out[:200]}")
        finally:
            client.close()

    # ── Wire ──────────────────────────────────────────────────
    farmacias = extract()
    run_scripts(farmacias)

ecofams_etl()