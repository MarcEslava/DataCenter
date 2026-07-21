r"""
Listado Referencias ETL  ·  PROTOTYPE

Builds the product reference master ('Listado Acuerdos') from the Zoho CRM *Products*
module and uploads it to the Power BI source DB, so it lives alongside the sales/stock
data the report already reads from there.

Destination: table DEST_TABLE (dbo.maestros) on `powerbi_dest_db` — the same DB that
powerBI_maestros_ETL loads. Power BI's 'Listado Acuerdos' table gets repointed from the
Excel workbook to this table.

Flow:
  1. extract_products         — page the Zoho Products module → JSON
  2. extract_vendor_names     — {vendor_id: name} from Zoho Vendors (for 'Laboratorio')
  3. extract_families         — CN → Familia/Superfamilia from BIFarma (vteco_familias)
  4. build_maestros           — map/compute the 41 columns and to_sql-replace dbo.maestros.

⚠️ PROTOTYPE — the Zoho field names in FIELD / DTO_FIELD / BOOK_FIELD below are best
guesses from the other DAGs (Decimal_1=PVL, IVA2=IVA, Product_Code, EAN, Vendor_Name,
Marca, and the Dto/Unid "Book 1..N" concept). Confirm them and adjust. Columns fed by
the sales DB (SO Uds) or not yet located in Zoho are emitted blank and flagged BLANK_COLS.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID          = "zoho_crm"
SQL_FAMILIES_CONN_ID  = "BIFarma_db"                  # mssql — Familia/Superfamilia (as in calculo_fee)
DEST_CONN_ID          = "powerbi_dest_db"             # Power BI source DB (as in powerBI_maestros_ETL)
DEST_TABLE            = "dbo.maestros"                # target table
DEST_IF_EXISTS        = "replace"                     # full reload each run
BASE_TMP     = "/tmp/listado_referencias"
LAB_USE_SHORTNAME = False                             # 'Laboratorio' = Shortname if True else legal name

# Familia/Superfamilia by product code — same source as calculo_fee_ETL
# (tbi_productosERS joined to vteco_familias on idfamiliaeco).
FAMILIES_SQL = """
    SELECT pr.codproducto            AS CN,
           f.nombrefamiliaeco        AS Familia,
           f.nombresuperfamiliaeco   AS Superfamilia
    FROM dbo.tbi_productosERS pr
    INNER JOIN dbo.vteco_familias f ON f.idfamiliaeco = pr.idfamilia
    GROUP BY pr.codproducto, f.nombrefamiliaeco, f.nombresuperfamiliaeco
"""

# Exact output header order — must match the Power BI query. Keep "Plataforma "
# (trailing space) and the accents/casing verbatim.
OUT_COLUMNS = [
    "CN", "CN6", "EAN", "Dto Especial", "Producto", "Solares", "SO Uds",
    "IVA", "Cod.IVA", "PVL",
    "%Dto 1", "%Dto 2", "%Dto 3", "%Dto 4",
    "PUC1", "PUC2", "PUC3", "PUC4",
    "PVPr", "Marca", "Laboratorio",
    "BOOK1", "BOOK2", "BOOK3", "BOOK4",
    "BOOK1 Sol", "BOOK2 Sol", "BOOK3 Sol", "BOOK4 Sol",
    "Plataforma ",
    "Coste PUC Book1", "Coste PUC Book2", "Coste PUC Book3", "Coste PUC Book4",
    "Coste PUC Book1 Sol", "Coste PUC Book2 Sol", "Coste PUC Book3 Sol", "Coste PUC Book4 Sol",
    "Unidades Descuento", "Superfamilia", "Familia",
]

# Zoho Products field → output column (verify against the real Products schema).
FIELD = {
    "CN":            "Product_Code",   # CN
    "CN6":           "Product_Code",   # CN6 — 6-digit national code (may be a distinct field)
    "EAN":           "EAN",
    "Producto":      "Product_Name",
    "Solares":       "Solares",        # solar-range flag/name (verify field)
    "IVA":           "IVA2",           # VAT %
    "Cod.IVA":       "Cod_IVA",        # VAT category code
    "PVL":           "Decimal_1",
    "Marca":         "Marca",
    "Dto Especial":  "Dto_Especial",
    "Plataforma":    "Plataforma",
    "Unidades Descuento": "Unidades_Descuento",
    "LAB":           "Vendor_Name",    # lookup → name/id
    # Superfamilia / Familia are NOT from Zoho — they come from BIFarma (FAMILIES_SQL).
}

# Per-book discount and unit fields (1..4).  %Dto n → PUCn, BOOKn = units.
DTO_FIELD      = {1: "Dto_Book_1", 2: "Dto_Book_2", 3: "Dto_Book_3", 4: "Dto_Book_4"}
BOOK_FIELD     = {1: "Unid_Book_1", 2: "Unid_Book_2", 3: "Unid_Book_3", 4: "Unid_Book_4"}
BOOK_SOL_FIELD = {1: "Unid_Book_1_Sol", 2: "Unid_Book_2_Sol", 3: "Unid_Book_3_Sol", 4: "Unid_Book_4_Sol"}

DTO_SCALE = 100.0   # discounts stored as percent (12 → 0.12). Set to 1.0 if already a fraction.

# Columns with no confirmed Zoho source yet — emitted blank (fill in when the source is known).
BLANK_COLS = ["SO Uds", "PVPr"]

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _lab_name(value) -> str:
    return value.get("name", "") if isinstance(value, dict) else ("" if value is None else str(value))

def _lab_id(value) -> str:
    return str(value.get("id", "")) if isinstance(value, dict) else ""

def _dest_conn():
    """Destination SQLConnection for powerbi_dest_db (pyodbc, ODBC 18) — as in powerBI_maestros_ETL."""
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    c = BaseHook.get_connection(DEST_CONN_ID)
    return SQLConnection(
        db_host=c.host, db_port=c.port or 1433, db_database=c.schema,
        db_username=c.login, db_password=c.password,
        dialect="mssql", driver="pyodbc",
        odbc_driver="ODBC Driver 18 for SQL Server",
        odbc_trust_server_cert=True, odbc_encrypt=False,
    )

def _query_mssql(conn_id: str, sql: str):
    """Run a query against an mssql Airflow connection (pymssql)."""
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    conn = BaseHook.get_connection(conn_id)
    db = SQLConnection(
        db_host=conn.host, db_port=conn.port or 1433,
        db_database=conn.schema, db_username=conn.login,
        db_password=conn.password, dialect="mssql", driver="pymssql",
    )
    with db:
        return db.fech_dataframe(sql)

def _fetch_all(module: str):
    """Page through a Zoho CRM module (shared auth pattern)."""
    from time import sleep
    from airflow.hooks.base import BaseHook
    from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
    conn = BaseHook.get_connection(ZOHO_CONN_ID)
    extra = conn.extra_dejson
    token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
    access_token = token_mgr.get_refresh_token(extra["refresh_token"])
    crm = ZohoCRMConnector(access_token)
    rows, page = [], 0
    while True:
        try:
            batch = crm.fetch_module_data(module, params={"page": page, "per_page": 200})
        except Exception:
            break
        if not batch:
            break
        rows.extend(batch)
        page += 1
        sleep(0.3)
    return rows

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="listado_referencias_ETL",
    description="Excel 'Listado Acuerdos' for Power BI, from Zoho Products (prototype)",
    schedule=Variable.get("listado_referencias_schedule", default_var="0 5 1 * *"),  # 05:00 on the 1st
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=5)},
)
def listado_referencias_etl():

    @task
    def extract_products() -> str:
        import os, json
        products = _fetch_all("Products")
        print(f"Extracted {len(products)} products from Zoho CRM")
        os.makedirs(BASE_TMP, exist_ok=True)
        out = f"{BASE_TMP}/products.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(products, f, default=str)
        return out

    @task
    def extract_vendor_names() -> dict:
        """{vendor_id: display name} for 'Laboratorio' — Shortname or legal name per LAB_USE_SHORTNAME."""
        field = "Shortname" if LAB_USE_SHORTNAME else "Vendor_Name"
        vendors = _fetch_all("Vendors")
        names = {}
        for v in vendors:
            val = (str(v.get(field) or "")).strip()
            if v.get("id") and val:
                names[str(v["id"])] = val
        print(f"Built {len(names)} vendor name(s) from field '{field}'")
        return names

    @task
    def extract_families() -> str:
        """CN → Familia/Superfamilia from BIFarma (vteco_familias); write CSV; return path."""
        import os
        df = _query_mssql(SQL_FAMILIES_CONN_ID, FAMILIES_SQL)
        df = df.drop_duplicates(subset=["CN"])
        print(f"Extracted {len(df)} CN→familia rows from BIFarma")
        os.makedirs(BASE_TMP, exist_ok=True)
        out = f"{BASE_TMP}/families.csv"
        df.to_csv(out, index=False)
        return out

    @task
    def build_maestros(products_file: str, vendor_names: dict, families_file: str) -> int:
        import json
        import pandas as pd

        with open(products_file, encoding="utf-8") as f:
            products = json.load(f)
        if not products:
            print("No products to process")
            return 0

        df = pd.DataFrame(products)
        n = len(df)

        def col(field: str) -> "pd.Series":
            return df[field] if field in df.columns else pd.Series([""] * n, index=df.index)

        def num(field: str) -> "pd.Series":
            return pd.to_numeric(col(field), errors="coerce")

        out = pd.DataFrame(index=df.index)

        # Direct product fields
        out["CN"]           = col(FIELD["CN"])
        out["CN6"]          = col(FIELD["CN6"])
        out["EAN"]          = num(FIELD["EAN"])
        out["Dto Especial"] = col(FIELD["Dto Especial"])
        out["Producto"]     = col(FIELD["Producto"])
        out["Solares"]      = col(FIELD["Solares"])
        out["IVA"]          = num(FIELD["IVA"])
        out["Cod.IVA"]      = col(FIELD["Cod.IVA"])
        out["PVL"]          = num(FIELD["PVL"]).fillna(0.0)
        out["Marca"]        = col(FIELD["Marca"])
        out["Plataforma "]  = col(FIELD["Plataforma"])
        out["Unidades Descuento"] = col(FIELD["Unidades Descuento"])

        # Familia / Superfamilia from BIFarma, matched by CN. round() before Int64 so a
        # non-integer/float-precision value doesn't fail the "safe" cast.
        fam = pd.read_csv(families_file)
        fam["_cnkey"] = pd.to_numeric(fam["CN"], errors="coerce").round().astype("Int64")
        fam = fam.dropna(subset=["_cnkey"]).drop_duplicates(subset=["_cnkey"])
        cnkey = pd.to_numeric(out["CN"], errors="coerce").round().astype("Int64")
        fam_by_cn = fam.set_index("_cnkey")
        out["Familia"]      = cnkey.map(fam_by_cn["Familia"]).fillna("")
        out["Superfamilia"] = cnkey.map(fam_by_cn["Superfamilia"]).fillna("")
        print(f"Matched {int(cnkey.isin(fam['_cnkey']).sum())}/{n} products to a BIFarma familia")

        # Laboratorio (from vendor lookup id → name, fallback to inline name)
        lab_lookup = col(FIELD["LAB"])
        out["Laboratorio"] = lab_lookup.map(lambda v: vendor_names.get(_lab_id(v)) or _lab_name(v))

        # Per-book discounts, units, and derived unit purchase cost (PUCn = PVL·(1−%Dto)).
        for i in (1, 2, 3, 4):
            dto = num(DTO_FIELD[i]).fillna(0.0)
            out[f"%Dto {i}"] = dto
            net = dto.div(DTO_SCALE).rsub(1.0)          # 1 − (%Dto / scale)
            out[f"PUC{i}"]   = (out["PVL"] * net).round(4)
            out[f"BOOK{i}"]      = num(BOOK_FIELD[i]).fillna(0).astype("int64")
            out[f"BOOK{i} Sol"]  = num(BOOK_SOL_FIELD[i]).fillna(0).astype("int64")

        # Book cost = unit PUC × book units.
        for i in (1, 2, 3, 4):
            out[f"Coste PUC Book{i}"]     = (out[f"PUC{i}"] * out[f"BOOK{i}"].astype(float)).round(2)
            out[f"Coste PUC Book{i} Sol"] = (out[f"PUC{i}"] * out[f"BOOK{i} Sol"].astype(float)).round(2)

        # Columns with no confirmed Zoho source yet → blank.
        for c in BLANK_COLS:
            out[c] = ""

        out = out[OUT_COLUMNS]

        # Upload to the Power BI source DB (full reload), alongside the sales/stock data.
        schema, tbl = DEST_TABLE.split(".", 1) if "." in DEST_TABLE else (None, DEST_TABLE)
        dst = _dest_conn()
        dst.connect()
        try:
            with dst.engine.begin() as conn:
                out.to_sql(name=tbl, schema=schema, con=conn,
                           if_exists=DEST_IF_EXISTS, index=False, chunksize=10_000)
        finally:
            dst.close()
        print(f"Uploaded {len(out)} rows -> {DEST_TABLE} on {DEST_CONN_ID}")
        return len(out)

    # ── Wire ──
    products     = extract_products()
    vendor_names = extract_vendor_names()
    families     = extract_families()
    build_maestros(products, vendor_names, families)


listado_referencias_etl()
