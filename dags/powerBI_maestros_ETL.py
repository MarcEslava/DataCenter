"""
PowerBI ETL Pipeline

Extracts data from a source SQL database using configurable queries,
then drops & recreates each target table in the destination database
and loads the fresh data. One Airflow task per table transfer.
"""

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────
# Connections  (set these in Airflow UI → Admin → Connections)
# ─────────────────────────────────────────────────────────────
SRC_CONN_ID  = "BIFarmaCentral_db"   # source MSSQL connection
DST_CONN_ID  = "powerbi_dest_db"     # destination MSSQL connection

# ─────────────────────────────────────────────────────────────
# Table transfer definitions
# Each entry: (destination_table_name, source_sql_query)
# Add / remove rows here to control which tables are transferred.
# ─────────────────────────────────────────────────────────────
TABLE_TRANSFERS = [
    (
        "dbo.tme_superfamilias",
        "SELECT * FROM dbo.tme_superfamilias",
    ),
    (
        "dbo.v_tme_productos",
        "SELECT * FROM dbo.v_tme_productos",
    ),
    (
        "dbo.tme_LaboratoriosE",
        "SELECT * FROM dbo.tme_LaboratoriosE",
    ),
    (
        "dbo.tme_delegaciones",
        "SELECT * FROM dbo.tme_delegaciones",
    ),
    (
        "dbo.teco_familias",
        "SELECT * FROM dbo.teco_familias",
    ),
    (
        "dbo.tbi_sinonimos",
        "SELECT * FROM dbo.tbi_sinonimos",
    ),
    (
        "dbo.v_dim_productos",
        """SELECT dbo.v_dim_productos.identidad,
		dbo.v_dim_productos.iddelegacion,
		dbo.v_dim_productos.idProducto,
		dbo.v_dim_productos.nombreProducto,
		dbo.v_dim_productos.nombreProductoEco,
		dbo.v_dim_productos.ean,
		dbo.v_dim_productos.efg,
		dbo.v_dim_productos.efp,
		dbo.v_dim_productos.idGrupoProducto,
		dbo.v_dim_productos.nombreGrupoProducto,
		dbo.v_dim_productos.idSubgrupoProducto,
		dbo.v_dim_productos.nombreSubgrupoProducto,
		dbo.v_dim_productos.idSuperfamilia,
		dbo.v_dim_productos.nombreSuperfamilia,
		dbo.v_dim_productos.idFamilia,
		dbo.v_dim_productos.nombreFamilia,
		dbo.v_dim_productos.idSubgrupoProductoEco,
		dbo.v_dim_productos.nombreSubgrupoProductoEco,
		dbo.v_dim_productos.idSuperFamiliaEco,
		dbo.v_dim_productos.nombreSuperFamiliaEco,
		dbo.v_dim_productos.idFamiliaEco,
		dbo.v_dim_productos.nombreFamiliaEco,
		dbo.v_dim_productos.idMarcoMental,
		dbo.v_dim_productos.nombreMarcoMental,
		dbo.v_dim_productos.idSubMarcoMental,
		dbo.v_dim_productos.nombreSubMarcoMental,
		dbo.v_dim_productos.idLab,
		dbo.v_dim_productos.tipoLab,
		dbo.v_dim_productos.nombreLab,
		dbo.v_dim_productos.idLabEco,
		dbo.v_dim_productos.nombreLabEco,
		dbo.v_dim_productos.pvp,
		dbo.v_dim_productos.pvpaux,
		dbo.v_dim_productos.puc,
		dbo.v_dim_productos.pmc,
		dbo.v_dim_productos.stockactual,
		dbo.v_dim_productos.stockminimo,
		dbo.v_dim_productos.stockmaximo,
		dbo.v_dim_productos.fechaultimaentrada,
		dbo.v_dim_productos.fechaultimasalida,
		dbo.v_dim_productos.fechaalta,
		dbo.v_dim_productos.grutera,
		dbo.v_dim_productos.campousuario1,
		dbo.v_dim_productos.track,
		dbo.v_dim_productos.uact,
		dbo.v_dim_productos.p0,
		dbo.v_dim_productos.p1,
		dbo.v_dim_productos.p2,
		dbo.v_dim_productos.p3,
		dbo.v_dim_productos.p4,
		dbo.v_dim_productos.p5,
		dbo.v_dim_productos.p6,
		dbo.v_dim_productos.p7,
		dbo.v_dim_productos.p8,
		dbo.v_dim_productos.p9,
		dbo.v_dim_productos.p10,
		dbo.v_dim_productos.p11,
		dbo.v_dim_productos.p12,
		dbo.v_dim_productos.IDPROD,
		dbo.v_dim_productos.IDEAN1,
		dbo.v_dim_productos.IDEAN2,
		dbo.v_dim_productos.pasoCodif,
		dbo.v_dim_productos.codProducto,
		dbo.v_dim_productos.desProducto,
		dbo.v_dim_productos.codLab,
		dbo.v_dim_productos.desLab,
		dbo.v_dim_productos.generico,
		dbo.v_dim_productos.idGrupoIva,
		dbo.v_dim_productos.loteOptimo,
		dbo.v_dim_productos.idProvHabitual,
		dbo.v_dim_productos.caducidad,
		dbo.v_dim_productos.fechaCaducidad,
		dbo.v_dim_productos.IdAgrupHomo,
		dbo.v_dim_productos.cantTAM,
		dbo.v_dim_productos.impTAM
        FROM	dbo.v_dim_productos""",
    ),
    # Add more (destination_table, query) tuples here…
]

# ─────────────────────────────────────────────────────────────
# DAG defaults
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "powerbi_etl",
    default_args=default_args,
    description="PowerBI ETL – drop & recreate destination tables from source queries",
    schedule=Variable.get("powerbi_etl_schedule", default_var="0 6 * * *"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=2,
)


# ─────────────────────────────────────────────────────────────
# Core transfer function
# ─────────────────────────────────────────────────────────────
READ_CHUNK_SIZE  = 50_000   # rows read from source at a time (controls memory)
WRITE_CHUNK_SIZE = 5_000    # rows per INSERT batch to destination

def _transfer_table(dest_table: str, query: str, **_):
    """
    Streams data from source to destination in chunks to avoid OOM on large tables.
    First chunk: DROP + CREATE (if_exists='replace').
    Subsequent chunks: INSERT (if_exists='append').
    """
    from sqlalchemy import text as sa_text
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection

    def _make_conn(conn_id: str) -> SQLConnection:
        c = BaseHook.get_connection(conn_id)
        return SQLConnection(
            db_host=c.host,
            db_port=c.port or 1433,
            db_database=c.schema,
            db_username=c.login,
            db_password=c.password,
            dialect="mssql",
            driver="pymssql",
        )

    schema, tbl = (dest_table.split(".", 1) if "." in dest_table else (None, dest_table))

    src = _make_conn(SRC_CONN_ID)
    src.connect()
    dst = _make_conn(DST_CONN_ID)
    dst.connect()

    try:
        total_rows = 0
        first_chunk = True

        import pandas as pd
        import time

        # Use server-side cursor via execution_options to avoid buffering full result
        with src.engine.connect().execution_options(stream_results=True) as src_conn:
            chunks = pd.read_sql(sa_text(query), src_conn, chunksize=READ_CHUNK_SIZE)
            for df in chunks:
                if df.empty:
                    continue
                t0 = time.time()
                # Commit each chunk in its own transaction — avoids one giant lock
                with dst.engine.begin() as dst_conn:
                    df.to_sql(
                        name=tbl,
                        schema=schema,
                        con=dst_conn,
                        if_exists="replace" if first_chunk else "append",
                        index=False,
                        chunksize=WRITE_CHUNK_SIZE,
                    )
                total_rows += len(df)
                first_chunk = False
                print(f"[{dest_table}] {total_rows:,} rows written ({len(df):,} in {time.time()-t0:.1f}s)")

        if first_chunk:
            print(f"[{dest_table}] No data returned – skipping load.")
        else:
            print(f"[{dest_table}] Done. Total rows loaded: {total_rows:,}.")
    finally:
        src.close()
        dst.close()


# ─────────────────────────────────────────────────────────────
# Dynamically create one task per table transfer
# ─────────────────────────────────────────────────────────────
for _dest_table, _query in TABLE_TRANSFERS:
    # Build a safe task_id from the table name (strip schema prefix)
    _task_id = "transfer__" + _dest_table.replace(".", "_").replace(" ", "_")

    PythonOperator(
        task_id=_task_id,
        python_callable=_transfer_table,
        op_kwargs={"dest_table": _dest_table, "query": _query},
        dag=dag,
    )

