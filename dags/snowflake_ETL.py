"""
Snowflake incremental load (carga_incremental_Snowflake)

Translated from the Talend project `carga_incremental_Snowflake`. Moves data from the
BifarmaCentral DW (MSSQL) into the Snowflake data warehouse — one task per entity.

Pattern per entity (mirrors the Talend tMSSqlInput → tMap → tSnowflakeOutput jobs):
    1. read the source query from MSSQL (BIFarma_db)
    2. TRUNCATE the target Snowflake table
    3. load the rows with write_pandas

Target: Snowflake DEV (account/warehouse/db/schema come from the `snowflake_dev`
Airflow connection). Dimensions are full reloads; fact tables are date-floored windows
(2022/2023); commandes_au_fournisseur is a rolling 3-day delta.

NOTE: the Talend `NonStockable` job sources from an Excel file (not a DB) — not included here.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SOURCE_CONN_ID    = "bifarma_origen"  # mssql — Bifarma DW database (source; all source tables live here)
SNOWFLAKE_CONN_ID = "snowflake_dev"   # snowflake DEV target

# Shared source filters used by the pharmacy-scoped queries (from the Talend WHERE clauses)
_DELEG_FILTER = "td.activo = '1' AND dv.identidad < '3900' AND td.swBench <> '0' AND dv.identidad NOT LIKE '34%'"

# One entry per Talend job: source query (MSSQL) → Snowflake table (uppercase French names)
# + the exact source→target column mapping from the Talend tMap. Only the mapped columns
# are loaded (extras like fcarga are dropped — they aren't columns in the target tables).
ENTITIES = [
    # ── dimensions (full reload) ─────────────────────────────
    {"name": "families", "table": "FAMILLES",
     "query": "SELECT idfamiliaEco, nombreFamiliaEco, idsuperfamiliaEco FROM teco_familias",
     "columns": {"idfamiliaEco": "ID_FAMILLE_ECO", "nombreFamiliaEco": "NOM_FAMILLE_ECO",
                 "idsuperfamiliaEco": "ID_SUPER_FAMILLE"}},
    {"name": "superfamilles", "table": "SUPERFAMILLES",
     "query": "SELECT idsuperfamiliaEco, nombreSuperfamiliaEco, idSubgrupoProducto FROM tme_superfamilias",
     "columns": {"idsuperfamiliaEco": "ID_SUPER_FAMILLE", "nombreSuperfamiliaEco": "NOM_SUPER_FAMILLLE_ECO",
                 "idSubgrupoProducto": "ID_SOUSGROUPE"}},
    {"name": "groupes", "table": "GROUPES",
     "query": "SELECT idGrupoProducto, nombreGrupoProducto FROM tme_gruposProducto",
     "columns": {"idGrupoProducto": "ID_GROUPE", "nombreGrupoProducto": "NOM_GROUPE"}},
    {"name": "sousgroupes", "table": "SOUSGROUPES",
     "query": "SELECT DISTINCT idSubgrupoProducto, nombreSubgrupoProducto, idGrupoProducto FROM tme_subGruposProducto",
     "columns": {"idSubgrupoProducto": "ID_SOUSGROUPE", "nombreSubgrupoProducto": "NOM_SOUSGROUPE",
                 "idGrupoProducto": "ID_GROUPE"}},
    {"name": "laboratoires", "table": "LABORATOIRES",
     "query": "SELECT DISTINCT idLab, nombreLab FROM vOtme_productos",
     "columns": {"idLab": "ID_LABORATOIRE", "nombreLab": "NOM_LABORATOIRE"}},

    # ── fact / bridge tables (date-floored windows) ──────────
    {"name": "produits_par_pharmacie", "table": "PRODUITS_PAR_PHARMACIE",
     "query": f"""SELECT DISTINCT dv.identidad, dv.iddelegacion, dv.idProducto, dv.ean,
                     dv.nombreProducto, dv.stockminimo, dv.stockmaximo
                 FROM [dbo].[tbi_productosFMTNew] AS dv
                 INNER JOIN tme_delegaciones AS td
                     ON dv.identidad = td.identidad AND dv.iddelegacion = td.iddelegacion
                 WHERE {_DELEG_FILTER} AND dv.fcarga >= '2023-01-01'""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "idProducto": "ID_PRODUIT",
                 "ean": "EAN", "nombreProducto": "NOM_PRODUIT", "stockminimo": "STOCK_MINIMUM",
                 "stockmaximo": "STOCK_MAXIMUM"}},
    {"name": "lignes_vente", "table": "LIGNES_VENTE",
     "query": f"""SELECT DISTINCT dv.identidad, dv.iddelegacion, dv.fecha, dv.idProducto,
                     dv.idVendedor, dv.tipoVenta, dv.idAportacion, dv.cantidad, dv.importe, dv.costePUC
                 FROM [dbo].[dwVentas] AS dv
                 INNER JOIN tme_delegaciones AS td ON dv.identidad = td.identidad
                 WHERE {_DELEG_FILTER} AND dv.fecha >= '2022-01-01'""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "fecha": "DATE_VENTE",
                 "idProducto": "ID_PRODUIT", "idVendedor": "ID_VENDEUR", "tipoVenta": "TYPE_VENTE",
                 "idAportacion": "ID_SAISIR", "cantidad": "MONTANT", "importe": "PVP",
                 "costePUC": "MONTANT_COUT"}},
    {"name": "lignes_reception", "table": "LIGNES_RECEPTION",
     "query": f"""SELECT DISTINCT dv.identidad, dv.iddelegacion, dv.fecha, dv.idProveedor,
                     dv.idProducto, dv.pedidas, dv.recibidas, dv.bonificadas, dv.importePvp,
                     dv.importePuc, dv.importe, dv.importeNeto
                 FROM [dbo].[dwRecepciones] AS dv
                 INNER JOIN tme_delegaciones AS td ON dv.identidad = td.identidad
                 WHERE {_DELEG_FILTER} AND dv.fecha >= '2022-01-01'""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "fecha": "DATE_RECEPTION",
                 "idProducto": "ID_PRODUIT", "idProveedor": "ID_FOURNISSEUR", "pedidas": "U_COMMANDEES",
                 "recibidas": "U_RECUES", "bonificadas": "U_BONUS", "importePvp": "MONTANT_PVP",
                 "importePuc": "MONTANT_PUC", "importe": "MONTANT", "importeNeto": "MONTANT_NET"}},
    {"name": "inventaires", "table": "INVENTAIRES",
     "query": f"""SELECT DISTINCT dv.identidad, dv.iddelegacion, dv.idProducto, dv.fecha,
                     dv.estoc, dv.pvp
                 FROM [dbo].[tbi_invenID] AS dv
                 INNER JOIN tme_delegaciones AS td
                     ON dv.identidad = td.identidad AND dv.iddelegacion = td.iddelegacion
                 WHERE {_DELEG_FILTER} AND dv.fecha >= '2023-01-01'""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "fecha": "DATE",
                 "idProducto": "ID_PRODUIT", "estoc": "STOCK", "pvp": "PVP"}},

    # ── true incremental delta (last 3 days) ─────────────────
    {"name": "commandes_au_fournisseur", "table": "COMMANDES_AU_FOURNISSEUR",
     "query": """SELECT identidad, iddelegacion, idMov, idProducto, fechaEntrada, tipomov
                 FROM dbo.tfa_desabasinfo
                 WHERE fechaEntrada >= DATEADD(DAY, -3, GETDATE())
                   AND CAST(fcarga AS DATE) = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "idMov": "ID_MOUVEMENT",
                 "idProducto": "ID_PRODUIT", "fechaEntrada": "DATE_ENTREE", "tipomov": "TYPE_DE_MOUVEMENT"}},

    # ── NOT YET TRANSLATED ───────────────────────────────────
    # produits   → PRODUITS:  multi-source (vtme_productos_homogeneo + a secondary EAN/book-units
    #              source + computed ACCORD/NOSTOCKABLE). Also vtme_productos_homogeneo is not in
    #              the Bifarma DB — needs its real source DB.
    # pharmacies → PHARMACIES: multi-source (tme_delegaciones + secondary lookups for
    #              TRANSFORME/METRES/BOOK_*/ecostock*/LABORATOIRE_PREFERE_*).
    # laboratories_ECO → LABORATORIES_ECO: source table SnowFlakeLaboratoriesECO not found in any
    #              accessible DB — needs the correct source.
    # Excluded for now: a truncate+load with only the primary columns would NULL out the
    # enrichment columns these tables already hold in DEV. Need the full source defs first.
]

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _query_mssql(sql: str):
    """Run a query against the source MSSQL DW and return a DataFrame."""
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    conn = BaseHook.get_connection(SOURCE_CONN_ID)
    db = SQLConnection(
        db_host=conn.host, db_port=conn.port or 1433,
        db_database=conn.schema, db_username=conn.login,
        db_password=conn.password, dialect="mssql", driver="pymssql",
    )
    with db:
        return db.fech_dataframe(sql)


def _load_snowflake(df, table: str, colmap: dict) -> int:
    """Rename source columns to the Snowflake column names, truncate the target table and
    load the DataFrame. Returns the row count loaded."""
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from airflow.hooks.base import BaseHook

    # map source columns → target columns (case-insensitive on the source side), keep only those
    lower = {c.lower(): c for c in df.columns}
    rename, keep = {}, []
    for src, tgt in colmap.items():
        actual = lower.get(src.lower())
        if actual is None:
            raise KeyError(f"[{table}] source column '{src}' not in query result; got {list(df.columns)}")
        keep.append(actual)
        rename[actual] = tgt
    out = df[keep].rename(columns=rename)

    # datetime columns → 'YYYY-MM-DD' strings: the target columns are DATE, and write_pandas
    # would otherwise send datetime64 as nanosecond-epoch ints that Snowflake can't cast to DATE.
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            mask = out[col].notna()
            out[col] = out[col].dt.strftime("%Y-%m-%d")
            out.loc[~mask, col] = None

    conn  = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson
    cx = snowflake.connector.connect(
        account=extra["account"], user=conn.login, password=conn.password,
        warehouse=extra.get("warehouse"), database=extra.get("database"),
        schema=extra.get("schema"), role=extra.get("role") or None, login_timeout=30,
    )
    try:
        cur = cx.cursor()
        # Truncate string values to each target column's VARCHAR length (Talend did this
        # implicitly; Snowflake otherwise rejects over-length strings).
        cur.execute(
            "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH "
            f"FROM {extra.get('database')}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{extra.get('schema')}' AND TABLE_NAME = '{table}'"
        )
        for col, maxlen in cur.fetchall():
            if maxlen and col in out.columns:
                s = out[col]
                out[col] = s.where(s.isna(), s.astype(str).str.slice(0, maxlen))

        cur.execute(f'TRUNCATE TABLE IF EXISTS "{table}"')
        ok, _nchunks, nrows, _ = write_pandas(cx, out, table, quote_identifiers=True, auto_create_table=False)
        if not ok:
            raise RuntimeError(f"write_pandas failed for {table}")
        return nrows
    finally:
        cx.close()

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='snowflake_ETL',
    description='Incremental load BifarmaCentral DW → Snowflake DEV (one task per entity)',
    schedule=Variable.get("snowflake_etl_schedule", default_var="0 4 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=4,  # limit concurrent source/Snowflake load
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)
def snowflake_etl():

    def _make_task(entity: dict):
        @task(task_id=f"load_{entity['name']}")
        def _load(query=entity["query"], table=entity["table"],
                  columns=entity["columns"], name=entity["name"]):
            df = _query_mssql(query)
            print(f"[{name}] read {len(df)} rows from source")
            n = _load_snowflake(df, table, columns)
            print(f"[{name}] loaded {n} rows into Snowflake \"{table}\"")
        return _load()

    for entity in ENTITIES:
        _make_task(entity)

snowflake_etl()