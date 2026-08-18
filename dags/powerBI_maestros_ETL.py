from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

SRC_CONN_ID = "BIFarmaCentral_db"
DST_CONN_ID = "powerbi_dest_db"

TABLE_TRANSFERS = [
    ("dbo.tme_superfamilias",           "SELECT * FROM dbo.tme_superfamilias"),
    ("dbo.v_tme_productos",             "SELECT * FROM dbo.v_tme_productos"),
    ("dbo.tme_LaboratoriosE",           "SELECT * FROM dbo.tme_LaboratoriosE"),
    ("dbo.tme_delegaciones",            "SELECT * FROM dbo.tme_delegaciones"),
    ("dbo.teco_familias",               "SELECT * FROM dbo.teco_familias"),
    ("dbo.tbi_sinonimos",               "SELECT * FROM dbo.tbi_sinonimos"),
    ("dbo.v_dim_productos",             "SELECT * FROM dbo.v_dim_productos"),
    ("dbo.dwRecepciones",               "SELECT * FROM dbo.dwRecepciones"), #check
    ("dbo.dwVentas",                    "SELECT * FROM dbo.dwVentas"),
    ("dbo.tbi_productosWORKTMPALL",     "SELECT * FROM dbo.tbi_productosWORKTMPALL"),
]

dag = DAG(
    "powerbi_etl",
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=5)},
    schedule=Variable.get("powerbi_etl_schedule", default_var="0 6 * * *"),
    tags=["powerbi", "bifarma", "sync"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
)


CHUNK_SIZE = 50_000

def _transfer_all(**_):
    import time
    import pandas as pd
    from sqlalchemy import text as sa_text
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection

    def _make_conn(conn_id):
        c = BaseHook.get_connection(conn_id)
        return SQLConnection(
            db_host=c.host, db_port=c.port or 1433, db_database=c.schema,
            db_username=c.login, db_password=c.password,
            dialect="mssql", driver="pyodbc",
            odbc_driver="ODBC Driver 18 for SQL Server",
            odbc_trust_server_cert=True, odbc_encrypt=False,
        )

    src = _make_conn(SRC_CONN_ID)
    src.connect()
    dst = _make_conn(DST_CONN_ID)
    dst.connect()
    print("Connections open.", flush=True)

    try:
        for dest_table, query in TABLE_TRANSFERS:
            schema, tbl = dest_table.split(".", 1) if "." in dest_table else (None, dest_table)
            t = time.time()
            total = 0
            first = True
            print(f"[{dest_table}] fetching…", flush=True)
            with src.engine.connect() as conn:
                for df in pd.read_sql(sa_text(query), conn, chunksize=CHUNK_SIZE):
                    with dst.engine.begin() as dst_conn:
                        df.to_sql(name=tbl, schema=schema, con=dst_conn,
                                  if_exists="replace" if first else "append", index=False)
                    total += len(df)
                    first = False
                    print(f"[{dest_table}] {total:,} rows written…", flush=True)
            print(f"[{dest_table}] done — {total:,} rows in {time.time()-t:.1f}s", flush=True)
    finally:
        src.close()
        dst.close()


PythonOperator(task_id="transfer_all_tables", python_callable=_transfer_all, dag=dag)

