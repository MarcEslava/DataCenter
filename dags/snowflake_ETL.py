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
    # ── transactional facts: INCREMENTAL (last 3 days, append, streamed in chunks) ──
    {"name": "lignes_vente", "table": "LIGNES_VENTE", "incremental": True,
     "query": f"""SELECT dv.identidad, dv.iddelegacion, dv.fecha, dv.idProducto,
                     dv.idVendedor, dv.tipoVenta, dv.idAportacion, dv.cantidad, dv.importe, dv.costePUC
                 FROM [dbo].[dwVentas] AS dv
                 INNER JOIN tme_delegaciones AS td ON dv.identidad = td.identidad
                 WHERE {_DELEG_FILTER} AND dv.fcarga >= CAST(DATEADD(DAY, -3, GETDATE()) AS DATE)""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "fecha": "DATE_VENTE",
                 "idProducto": "ID_PRODUIT", "idVendedor": "ID_VENDEUR", "tipoVenta": "TYPE_VENTE",
                 "idAportacion": "ID_SAISIR", "cantidad": "MONTANT", "importe": "PVP",
                 "costePUC": "MONTANT_COUT"}},
    {"name": "lignes_reception", "table": "LIGNES_RECEPTION", "incremental": True,
     "query": f"""SELECT dv.identidad, dv.iddelegacion, dv.fecha, dv.idProveedor,
                     dv.idProducto, dv.pedidas, dv.recibidas, dv.bonificadas, dv.importePvp,
                     dv.importePuc, dv.importe, dv.importeNeto
                 FROM [dbo].[dwRecepciones] AS dv
                 INNER JOIN tme_delegaciones AS td ON dv.identidad = td.identidad
                 WHERE {_DELEG_FILTER} AND dv.fcarga >= CAST(DATEADD(DAY, -3, GETDATE()) AS DATE)""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "fecha": "DATE_RECEPTION",
                 "idProducto": "ID_PRODUIT", "idProveedor": "ID_FOURNISSEUR", "pedidas": "U_COMMANDEES",
                 "recibidas": "U_RECUES", "bonificadas": "U_BONUS", "importePvp": "MONTANT_PVP",
                 "importePuc": "MONTANT_PUC", "importe": "MONTANT", "importeNeto": "MONTANT_NET"}},
    {"name": "inventaires", "table": "INVENTAIRES", "incremental": True,
     "query": f"""SELECT dv.identidad, dv.iddelegacion, dv.idProducto, dv.fecha, dv.estoc, dv.pvp
                 FROM [dbo].[tbi_invenID] AS dv
                 INNER JOIN tme_delegaciones AS td
                     ON dv.identidad = td.identidad AND dv.iddelegacion = td.iddelegacion
                 WHERE {_DELEG_FILTER} AND dv.fecha >= CAST(DATEADD(DAY, -3, GETDATE()) AS DATE)""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "fecha": "DATE",
                 "idProducto": "ID_PRODUIT", "estoc": "STOCK", "pvp": "PVP"}},
    {"name": "commandes_au_fournisseur", "table": "COMMANDES_AU_FOURNISSEUR", "incremental": True,
     "query": """SELECT identidad, iddelegacion, idMov, idProducto, fechaEntrada, tipomov
                 FROM dbo.tfa_desabasinfo
                 WHERE fechaEntrada >= DATEADD(DAY, -3, GETDATE())
                   AND CAST(fcarga AS DATE) >= CAST(DATEADD(DAY, -3, GETDATE()) AS DATE)""",
     "columns": {"identidad": "IDENTITE", "iddelegacion": "ID_PHARMACIE", "idMov": "ID_MOUVEMENT",
                 "idProducto": "ID_PRODUIT", "fechaEntrada": "DATE_ENTREE", "tipomov": "TYPE_DE_MOUVEMENT"}},

    # ── from the `bi` MySQL database (10.20.10.4) ────────────
    {"name": "laboratories_ECO", "table": "LABORATORIES_ECO",
     "source": "bi_db", "dialect": "mysql",
     "query": "SELECT id, lab_master_id, name FROM SnowFlakeLaboratoriesECO",
     "columns": {"id": "ID_LAB_ECO", "lab_master_id": "ID_LABORATOIRE", "name": "NOM"}},

    # ── NOT YET TRANSLATED ───────────────────────────────────
    # produits   → PRODUITS:  multi-source (vtme_productos_homogeneo + a secondary "Products"
    #              source with CN6/book-units + computed ACCORD/NOSTOCKABLE).
    # pharmacies → PHARMACIES: multi-source (tme_delegaciones + secondary lookups for
    #              TRANSFORME/METRES/BOOK_*/ecostock*/LABORATOIRE_PREFERE_* — incl. MySQL ecofam/ecoextract).
    # Excluded for now: a truncate+load with only the primary columns would NULL out the
    # enrichment columns these tables already hold in DEV. Need the full source defs first.
]

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

_DRIVERS = {"mssql": ("pymssql", 1433), "mysql": ("pymysql", 3306)}

def _query_source(sql: str, conn_id: str = SOURCE_CONN_ID, dialect: str = "mssql"):
    """Run a query against a source DB (mssql/mysql) and return a DataFrame."""
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    conn = BaseHook.get_connection(conn_id)
    driver, default_port = _DRIVERS[dialect]
    db = SQLConnection(
        db_host=conn.host, db_port=conn.port or default_port,
        db_database=conn.schema, db_username=conn.login,
        db_password=conn.password, dialect=dialect, driver=driver,
    )
    with db:
        return db.fech_dataframe(sql)


def _load_df(out, table: str) -> int:
    """Load an already target-mapped DataFrame into the Snowflake table:
    datetime → 'YYYY-MM-DD', truncate strings to column lengths, TRUNCATE + write_pandas."""
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from airflow.hooks.base import BaseHook

    out = out.reset_index(drop=True)   # write_pandas requires a clean RangeIndex
    # datetime columns → 'YYYY-MM-DD' strings (target columns are DATE; write_pandas would
    # otherwise send datetime64 as nanosecond-epoch ints that Snowflake can't cast to DATE).
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


def _load_snowflake(df, table: str, colmap: dict) -> int:
    """Rename source columns to the Snowflake column names, then load via _load_df."""
    lower = {c.lower(): c for c in df.columns}
    rename, keep = {}, []
    for src, tgt in colmap.items():
        actual = lower.get(src.lower())
        if actual is None:
            raise KeyError(f"[{table}] source column '{src}' not in query result; got {list(df.columns)}")
        keep.append(actual)
        rename[actual] = tgt
    return _load_df(df[keep].rename(columns=rename), table)


def _stream_incremental(query: str, conn_id: str, dialect: str, table: str,
                        colmap: dict, chunksize: int = 200_000) -> int:
    """Incremental APPEND: stream the source query in chunks and append each to the
    Snowflake table (no TRUNCATE). Keeps memory bounded for the large fact tables."""
    import datetime as _dt
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from airflow.hooks.base import BaseHook

    sconn = BaseHook.get_connection(conn_id)
    sf    = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    e     = sf.extra_dejson
    cx = snowflake.connector.connect(
        account=e["account"], user=sf.login, password=sf.password,
        warehouse=e.get("warehouse"), database=e.get("database"),
        schema=e.get("schema"), role=e.get("role") or None, login_timeout=30,
    )
    cur = cx.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH "
        f"FROM {e.get('database')}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{e.get('schema')}' AND TABLE_NAME = '{table}'"
    )
    lens = {c: m for c, m in cur.fetchall() if m}

    # streaming source cursor (FreeTDS streams for mssql; SSCursor for mysql)
    if dialect == "mssql":
        import pymssql
        sc = pymssql.connect(server=str(sconn.host), port=str(sconn.port or 1433),
                             user=str(sconn.login), password=str(sconn.password),
                             database=str(sconn.schema))
        scur = sc.cursor()
    else:
        import pymysql
        sc = pymysql.connect(host=str(sconn.host), port=int(sconn.port or 3306),
                             user=str(sconn.login), password=str(sconn.password),
                             database=str(sconn.schema), cursorclass=pymysql.cursors.SSCursor)
        scur = sc.cursor()

    total = 0
    try:
        scur.execute(query)
        src_cols = [d[0] for d in scur.description]
        lower = {c.lower(): c for c in src_cols}
        keep  = [lower[s.lower()] for s in colmap]
        rename = {lower[s.lower()]: t for s, t in colmap.items()}
        while True:
            rows = scur.fetchmany(chunksize)
            if not rows:
                break
            out = pd.DataFrame.from_records(list(rows), columns=src_cols)[keep].rename(columns=rename)
            # dates (datetime64 or python date objects) → 'YYYY-MM-DD'
            for col in out.columns:
                s = out[col]
                if pd.api.types.is_datetime64_any_dtype(s):
                    out[col] = s.dt.strftime("%Y-%m-%d").where(s.notna(), None)
                elif s.map(lambda v: isinstance(v, (_dt.date, _dt.datetime))).any():
                    out[col] = s.map(lambda v: v.strftime("%Y-%m-%d") if isinstance(v, (_dt.date, _dt.datetime)) else v)
            for col, mx in lens.items():
                if col in out.columns:
                    s = out[col]
                    out[col] = s.where(s.isna(), s.astype(str).str.slice(0, mx))
            ok, _nc, nr, _ = write_pandas(cx, out.reset_index(drop=True), table,
                                          quote_identifiers=True, auto_create_table=False)
            if not ok:
                raise RuntimeError(f"write_pandas append failed for {table}")
            total += nr
            print(f"[{table}] appended chunk: {nr} (running total {total})")
        return total
    finally:
        scur.close(); sc.close(); cx.close()

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='snowflake_ETL',
    tags=["snowflake", "warehouse", "analytics"],
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
                  columns=entity["columns"], name=entity["name"],
                  source=entity.get("source", SOURCE_CONN_ID),
                  dialect=entity.get("dialect", "mssql"),
                  incremental=entity.get("incremental", False)):
            if incremental:
                # last-3-days window, streamed in chunks and APPENDED (no truncate)
                n = _stream_incremental(query, source, dialect, table, columns)
                print(f"[{name}] appended {n} rows into Snowflake \"{table}\" (incremental)")
            else:
                df = _query_source(query, conn_id=source, dialect=dialect)
                print(f"[{name}] read {len(df)} rows from source")
                n = _load_snowflake(df, table, columns)
                print(f"[{name}] loaded {n} rows into Snowflake \"{table}\" (full reload)")
        return _load()

    for entity in ENTITIES:
        _make_task(entity)

    # ── produits → PRODUITS (multi-source: BifarmaCentral MSSQL ⋈ bi MySQL) ──
    @task(task_id="load_produits")
    def load_produits():
        import pandas as pd
        # primary: product master (MSSQL BifarmaCentral)
        prim = _query_source(
            "SELECT DISTINCT codproducto, desproducto, idFamiliaEco, idLab, idAgrupHomo "
            "FROM [BifarmaCentral].[dbo].[vtme_productos_homogeneo]",
            conn_id="bifarma_origen", dialect="mssql")
        # secondary: EAN / CN6 / book-units (MySQL bi.Products)
        sec = _query_source(
            "SELECT ean, CN6, minimum_units_book_1, minimum_units_book_2, "
            "minimum_units_book_3, minimum_units_book_4 FROM Products",
            conn_id="bi_db", dialect="mysql")
        # join key on the secondary: CN6 if present, else ean (Talend "join" expression)
        cn6 = sec["CN6"].astype(str).str.strip()
        sec["joinkey"] = cn6.where(cn6.ne("") & cn6.ne("None") & sec["CN6"].notna(),
                                   sec["ean"].astype(str).str.strip())
        prim["codkey"] = prim["codproducto"].astype(str).str.strip()
        m = prim.merge(sec, left_on="codkey", right_on="joinkey", how="left")
        out = pd.DataFrame({
            "ID_PRODUIT":              m["codproducto"],
            "DESCRIPTION":             m["desproducto"],
            "ID_FAMILLE_ECO":          m["idFamiliaEco"],
            "EAN":                     m["ean"],
            "CODI_JOINT_HOMOGENI":     m["idAgrupHomo"],
            "ACCORD":                  (m["codkey"] == m["joinkey"]).astype(int),
            "ID_LABORATORIE":          m["idLab"],
            "UNITES_MINIMALES_BOOK_1": m["minimum_units_book_1"],
            "UNITES_MINIMALES_BOOK_2": m["minimum_units_book_2"],
            "UNITES_MINIMALES_BOOK_3": m["minimum_units_book_3"],
            "UNITES_MINIMALES_BOOK_4": m["minimum_units_book_4"],
        }).drop_duplicates(subset=["ID_PRODUIT"])
        n = _load_df(out, "PRODUITS")
        print(f"[produits] loaded {n} rows into PRODUITS")
    load_produits()

    # ── pharmacies → PHARMACIES (3-source: Bifarma + BifarmaAgreg MSSQL ⋈ bi MySQL) ──
    @task(task_id="load_pharmacies")
    def load_pharmacies():
        import pandas as pd
        prim = _query_source(
            "SELECT identidad, iddelegacion, delegacion, LEFT(nif, 9) AS nif, date1, faltacliente "
            "FROM [Bifarma].[dbo].[tme_delegaciones] "
            "WHERE activo = '1' AND identidad < '3900' AND swBench <> '0' AND identidad NOT LIKE '34%'",
            conn_id="bifarma_origen", dialect="mssql")
        ram = _query_source(
            "SELECT identidad, iddelegacion, metrosLinealesParaf, fecTransformada, "
            "CASE WHEN swTransformada = 'SI' THEN 1 ELSE 0 END AS transformada "
            "FROM [BifarmaAgreg].[dbo].[tme_delegacionesRAMTransformadas]",
            conn_id="bifarma_origen", dialect="mssql")
        far = _query_source(
            "SELECT u.nif, f.laboratorio_preferido_1, f.laboratorio_preferido_2, "
            "f.laboratorio_preferido_3, f.laboratorio_preferido_4, f.book_gen, f.book_sol, "
            "f.stockable, f.ecostockEFG, f.ecostockServiceA, f.ecostockServiceB, "
            "f.ecostockServiceC, f.transition_date "
            "FROM farmacias f JOIN Unit u ON f.unit_id = u.id",
            conn_id="bi_db", dialect="mysql")
        for d in (prim, ram):
            for k in ("identidad", "iddelegacion"):
                d[k] = d[k].astype(str).str.strip()
        prim["nif"] = prim["nif"].astype(str).str.strip()
        far["nif"]  = far["nif"].astype(str).str.strip()
        m = (prim.merge(ram, on=["identidad", "iddelegacion"], how="left")
                 .merge(far, on="nif", how="left")
                 .drop_duplicates(subset=["identidad", "iddelegacion"]))
        out = pd.DataFrame({
            "IDENTITE":               m["identidad"],
            "ID_PHARMACIE":           m["iddelegacion"],
            "NOM":                    m["delegacion"],
            "NIF":                    m["nif"],
            "DATE_RADIATION":         m["date1"],
            "DATE_ENREGISTREMENT":    m["faltacliente"],
            "TRANSFORME":             m["transformada"],
            "DATE_DE_TRANSFORMATION": m["fecTransformada"],
            "METRES_LINEAIRES":       m["metrosLinealesParaf"],
            "LABORATOIRE_PREFERE_1":  m["laboratorio_preferido_1"],
            "LABORATOIRE_PREFERE_2":  m["laboratorio_preferido_2"],
            "LABORATOIRE_PREFERE_3":  m["laboratorio_preferido_3"],
            "LABORATOIRE_PREFERE_4":  m["laboratorio_preferido_4"],
            "DATE_DE_TRANSITION":     m["transition_date"],
            "BOOK_PARAPHARMACIE":     m["book_gen"],
            "BOOK_SOLAIRE":           m["book_sol"],
            "STOCKABLE":              m["stockable"],
            "ECOSTOCKEFG":            m["ecostockEFG"],
            "ECOSTOCKSERVICEA":       m["ecostockServiceA"],
            "ECOSTOCKSERVICEB":       m["ecostockServiceB"],
            "ECOSTOCKSERVICEC":       m["ecostockServiceC"],
        })
        n = _load_df(out, "PHARMACIES")
        print(f"[pharmacies] loaded {n} rows into PHARMACIES")
    load_pharmacies()

snowflake_etl()