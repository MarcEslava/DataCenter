"""
udpFarmatic ETL  ·  port of the Talend job `ecoUpd_All`

For each enabled pharmacy, trigger the ecoextract "update" API (varios + parámetros).

Flow (Talend ecoUpd_All → ecoUpd_all_stp2):
  1. extract_units — SELECT id FROM Unit WHERE port <> '' AND enabled = 1   (ecoextract MySQL)
  2. update_unit   — per unit, two REST GETs in order:
        GET {API_BASE}/unit/{id}/upd_varios
        GET {API_BASE}/unit/{id}/upd_parametro

No DB writes, no auth on the endpoints (matches the Talend tREST config).
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ECOEXTRACT_CONN_ID = "ecoextract_db"     # ecoextract MySQL (Unit table), via db-tunnel

# API base — Talend pointed at the TEST host; override the Variable for production
# (e.g. https://ecoextract.ecoceutics.com/api/ecoupdate).
API_BASE     = Variable.get("ecoupdate_api_base",
                            default_var="https://ecoextract.ecoceutics.com/api/ecoupdate")
ENDPOINTS    = ["upd_varios", "upd_parametro"]   # called per unit, in this order
HTTP_TIMEOUT = int(Variable.get("ecoupdate_http_timeout", default_var="120"))

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="udpFarmatic_ETL",
    tags=["farmatic", "update", "api"],
    description="Trigger ecoextract update API (upd_varios + upd_parametro) per pharmacy",
    schedule=Variable.get("udpfarmatic_schedule", default_var="0 6 * * *"),  # daily 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=4,   # limit concurrent API calls
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def udpfarmatic_etl():

    @task
    def extract_units() -> list[int]:
        """Enabled pharmacies with a configured port."""
        from airflow.hooks.base import BaseHook
        from utils.clsSQL import SQLConnection
        c = BaseHook.get_connection(ECOEXTRACT_CONN_ID)
        db = SQLConnection(
            db_host=c.host, db_port=c.port or 3306, db_database=c.schema,
            db_username=c.login, db_password=c.password, dialect="mysql", driver="pymysql",
        )
        with db:
            df = db.fech_dataframe("SELECT id FROM Unit WHERE port <> '' AND enabled = 1")
        units = [int(x) for x in df["id"].tolist()]
        print(f"{len(units)} unit(s) to update")
        return units

    @task
    def update_unit(unit_id: int) -> None:
        """Call the two update endpoints for one pharmacy, in order."""
        import requests
        for ep in ENDPOINTS:
            url = f"{API_BASE}/unit/{unit_id}/{ep}"
            r = requests.get(url, timeout=HTTP_TIMEOUT)
            print(f"[{unit_id}] GET {url} -> {r.status_code}")
            r.raise_for_status()

    update_unit.expand(unit_id=extract_units())


udpfarmatic_etl()
