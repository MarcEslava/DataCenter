"""
EcoLabs ETL  ·  PROTOTYPE

Builds one product file per BOOK × LAB from the Zoho CRM *Products* module,
matching the legacy format of docs/ECO_ZAMBON_B1.csv:

    CN;EAN;DESC;PVL;IVA;tipo de iva;cantidad;DESC BOOK;NOM LAB
    656473;;ESPIDIFEN 600 mg 40 SOBRES...;5,64;4;1;1;12;ZAMBON

Flow:
  1. extract_products  — page through Zoho CRM Products, dump to a JSON file
  2. build_lab_books   — normalise fields, and for each Book (1/2/3) select the
                         participating products, group by laboratory, and write
                         ECO_{LAB}_B{n}.csv to OUTPUT_DIR (one file per book/lab).

⚠️ PROTOTYPE — the Zoho field names in FIELD and BOOK_DTO_FIELD below are best
guesses based on the other DAGs (calculo_fee/novedades used Decimal_1=PVL,
IVA2=IVA, Product_Code, EAN, Vendor_Name, and the "Dto. Book 1/2/3" concept).
Confirm them against the real Products module schema and adjust.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID        = "zoho_crm"
SQL_BIFARMA_CONN_ID = "BIFarma_db"                           # mssql — Bifarma DB, has tbi_sinonimos (EAN↔CN)

BASE_TMP   = "/tmp/ecolabs"                                   # scratch for the raw dump
FILE_PREFIX = "ECO"                                           # ECO_{LAB}_B{n}.csv
ENCODING    = "latin-1"                                       # legacy pharma files are ISO-8859-1

# Upload the per-book/lab files to the FTP server (utils.ftp), using an Airflow connection.
# Set ENABLE_FTP_UPLOAD=False to write them to OUTPUT_DIR locally instead (for testing).
ENABLE_FTP_UPLOAD = True
FTP_CONN_ID    = Variable.get("ecolabs_ftp_conn", default_var="ecolabs_ftp")   # Airflow connection id
FTP_REMOTE_DIR = Variable.get("ecolabs_ftp_dir",  default_var=".")             # remote folder (login lands in the home root)
OUTPUT_DIR     = Variable.get("ecolabs_output_dir", default_var="/opt/airflow/docs")  # local fallback

# Books to emit. A product goes into book N's file when it "participates" in
# that book (see _participates below); DESC BOOK is filled from its book field.
# Book 4 is a duplicate of Book 3 (same data, filename B4) — see BOOK_DTO_FIELD.
BOOKS = [1, 2, 3, 4]

# 'tipo de iva' is derived from the IVA % via this map (legacy sample: IVA 4 → 1).
# Products whose IVA % is not a key here (or is blank) get a BLANK 'tipo de iva'
# and are listed in a warning at the end of the run so they can be fixed in Zoho.
IVA_TIPO_MAP = {0: 0, 4: 1, 10: 2, 21: 3}
CANTIDAD_DEFAULT = 1

# ── Zoho Products field → output meaning ─────────────────────────────────────
# ⚠️ Verify these against the real Products module and rename as needed.
FIELD = {
    "CN":   "Product_Code",   # CN   — código nacional
    "EAN":  "EAN",            # EAN  — barcode (often blank)
    "DESC": "Product_Name",   # DESC — product description  (maybe 'Producto'/'Descripcion')
    "PVL":  "Decimal_1",      # PVL  — price (Decimal_1 in calculo_fee)
    "IVA":  "IVA2",           # IVA  — VAT %, e.g. 4        (IVA2 in calculo_fee)
    "LAB":  "Vendor_Name",    # lookup → {'name': 'ESSITY SPAIN S.L.', 'id': '...'}
}

# EFG (generic) products use a different filename: ECO_B{n}_{LAB}_EFG.csv (regular products
# stay ECO_{LAB}_B{n}.csv). A product is EFG when its VENDOR's agreement type is EFG — the
# same Vendors field novedades_SKU uses for Obligatorio/Opcional.
VENDOR_ACUERDO_FIELD = "Tipo_Acuerdo"
EFG_ACUERDO_VALUES   = {"EFG"}

# Labs (NOM LAB) to skip entirely — no files generated for these (matched case-insensitively).
EXCLUDED_LABS = {"MARCA_EXCLUSIVA", "MERCADO_ALTERNATIVO"}

# NOM LAB / filename use the short lab name (ECO_ZAMBON_B1 → 'ZAMBON'). A Product's
# Vendor_Name only carries the full legal name, so we resolve the short name from the
# Vendors module by id (same 'Shortname' field used in novedades_SKU).
VENDOR_SHORTNAME_FIELD = "Shortname"

# Per-book field that supplies the "DESC BOOK" value (the book discount).
# A product is included in book N when this field has a value.
BOOK_DTO_FIELD = {
    1: "Dto_Book_1",
    2: "Dto_Book_2",
    3: "Dto_Book_3",
}

# DIAGNOSTIC: set to a Product_Code (CN) to dump that product's raw Zoho fields
# to the log (handy for verifying API field names). Leave "" in normal runs.
DEBUG_DUMP_CN = ""

# Output header, in order — must match the legacy file exactly.
OUT_COLUMNS = ["CN", "EAN", "DESC", "PVL", "IVA", "tipo de iva", "cantidad", "DESC BOOK", "NOM LAB"]

# EAN → 6-digit CN, from the complete synonyms table dbo.tbi_sinonimos
# (columns: identidad, iddelegacion, idproducto, idsinonimo, fcarga):
#   idproducto = 6-digit CN (padded, e.g. '000045'), idsinonimo = 13-digit EAN.
# Generic "no real CN" buckets (999996-999999) are excluded.
BIFARMA_DB      = "Bifarma"      # database that hosts tbi_sinonimos (cross-DB from the conn)
BIFARMA_EAN_COL = "idsinonimo"   # tbi_sinonimos column with the 13-digit EAN
BIFARMA_CN_COL  = "idproducto"   # tbi_sinonimos column with the 6-digit CN
BIFARMA_CN_SQL = f"""
    SELECT {BIFARMA_EAN_COL} AS EAN, {BIFARMA_CN_COL} AS CN
    FROM {BIFARMA_DB}.dbo.tbi_sinonimos
    WHERE {BIFARMA_EAN_COL} IS NOT NULL AND {BIFARMA_CN_COL} IS NOT NULL
      AND {BIFARMA_CN_COL} NOT IN ('999996', '999997', '999998', '999999')
"""

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

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

def _safe(name: str) -> str:
    """Filename-safe, upper-cased lab token (ECO_ZAMBON_B1)."""
    return "".join(c if c.isalnum() else "_" for c in str(name).strip()).upper().strip("_")

def _lab_name(value) -> str:
    """Vendor_Name may be a lookup dict {'name': ...} or a plain string."""
    if isinstance(value, dict):
        return value.get("name", "")
    return "" if value is None else str(value)

def _lab_id(value) -> str:
    """Vendor_Name lookup id, or '' if not a lookup dict."""
    return str(value.get("id", "")) if isinstance(value, dict) else ""

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="ecoLabs_ETL",
    description="Per-book/per-lab product files from Zoho CRM Products (prototype)",
    schedule=Variable.get("ecolabs_schedule", default_var="0 4 1 * *"),  # 04:00 on the 1st
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def ecolabs_etl():

    @task
    def extract_products() -> str:
        """Page through the Zoho CRM Products module; dump raw records to JSON; return the path."""
        import os, json
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_products, page = [], 0
        while True:
            try:
                batch = crm.fetch_module_data("Products", params={"page": page, "per_page": 200})
            except Exception:
                break
            if not batch:
                break
            all_products.extend(batch)
            page += 1
            sleep(0.3)
        print(f"Extracted {len(all_products)} products from Zoho CRM")

        os.makedirs(BASE_TMP, exist_ok=True)
        out = f"{BASE_TMP}/products.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(all_products, f, default=str)
        return out

    @task
    def extract_vendor_info() -> dict:
        """From Zoho Vendors: {vendor_id: Shortname} (for NOM LAB) + the set of EFG vendor ids."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        efg_wanted = {x.upper() for x in EFG_ACUERDO_VALUES}
        crm = ZohoCRMConnector(access_token)
        shortnames, efg_ids, page = {}, [], 0
        while True:
            try:
                batch = crm.fetch_module_data("Vendors", params={"page": page, "per_page": 200})
            except Exception:
                break
            if not batch:
                break
            for v in batch:
                vid = str(v.get("id") or "")
                if not vid:
                    continue
                short = (v.get(VENDOR_SHORTNAME_FIELD) or "").strip()
                if short:
                    shortnames[vid] = short
                if (str(v.get(VENDOR_ACUERDO_FIELD) or "")).strip().upper() in efg_wanted:
                    efg_ids.append(vid)
            page += 1
            sleep(0.3)
        print(f"Built {len(shortnames)} vendor shortname(s), {len(efg_ids)} EFG vendor(s)")
        return {"shortnames": shortnames, "efg_ids": efg_ids}

    @task
    def extract_bifarma_cn() -> str:
        """Build {EAN → CN} from BIFarma (calculo_fee source); write JSON; return path."""
        import os, json
        import pandas as pd
        df = _query_mssql(SQL_BIFARMA_CONN_ID, BIFARMA_CN_SQL)
        ean = pd.to_numeric(df["EAN"], errors="coerce")
        df = df[ean.notna()].copy()
        df["_eankey"] = ean[ean.notna()].astype("int64").astype(str)
        df = df.dropna(subset=["CN"]).drop_duplicates(subset=["_eankey"])
        mapping = dict(zip(df["_eankey"], df["CN"].astype(str)))
        print(f"Built {len(mapping)} EAN→CN entries from BIFarma")
        os.makedirs(BASE_TMP, exist_ok=True)
        out = f"{BASE_TMP}/bifarma_cn.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(mapping, f, default=str)
        return out

    @task
    def build_lab_books(products_file: str, vendor_info: dict, bifarma_cn_file: str) -> list[dict]:
        """Normalise, then upload one ECO_{LAB}_B{n}.csv per book × lab (FTP). Returns a summary."""
        import os, json, contextlib
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from utils.ftp import FTPConn

        shortnames = vendor_info.get("shortnames", {})
        efg_ids    = set(vendor_info.get("efg_ids", []))

        with open(products_file, encoding="utf-8") as f:
            products = json.load(f)
        if not products:
            print("No products to process")
            return []
        with open(bifarma_cn_file, encoding="utf-8") as f:
            bifarma_cn = json.load(f)

        df = pd.DataFrame(products)

        def col(name: str) -> "pd.Series":
            """Column `name` as a Series aligned to df, or an empty-string Series if absent."""
            if name in df.columns:
                return df[name]
            return pd.Series([""] * len(df), index=df.index)

        # ── TEMP DIAGNOSTIC: dump one product's raw Zoho fields to find the real
        #    API name of the "Dto. Book" discount fields. Remove once resolved. ──
        if DEBUG_DUMP_CN:
            code_col = FIELD["CN"]
            hit = df[df.get(code_col, pd.Series([""] * len(df), index=df.index))
                       .astype(str).str.strip() == str(DEBUG_DUMP_CN)]
            if hit.empty:
                print(f"[DEBUG] CN {DEBUG_DUMP_CN} not found among {len(df)} products "
                      f"(matched on '{code_col}')")
            for i, rec in hit.iterrows():
                print(f"[DEBUG] raw Zoho fields for CN {DEBUG_DUMP_CN}:")
                for k, v in rec.items():
                    print(f"    {k} = {v!r}")

        # CN (first column) = the 6-digit código nacional. A Zoho Product_Code that is really
        # a 13-digit EAN (anything longer than 7 digits) is NOT a CN — resolve those, and any
        # missing one, from BIFarma by matching the EAN. Still-unresolved → blank + printed.
        def _clean_cn(v) -> str:
            s = str(v).strip()
            return "" if s.lower() in ("", "nan", "none") else s
        cn_raw = col(FIELD["CN"]).map(_clean_cn)
        is_cn  = cn_raw.str.fullmatch(r"\d{1,7}").fillna(False)   # real CN = up to 7 digits
        cn = cn_raw.where(is_cn, "")

        # EAN key: the EAN field, or the code itself when it is EAN-shaped (>7 digits).
        ean_zoho = pd.to_numeric(col(FIELD["EAN"]), errors="coerce")
        code_num = pd.to_numeric(cn_raw, errors="coerce")
        ean_all  = ean_zoho.fillna(code_num.where(code_num > 9_999_999))
        eankey   = ean_all.astype("Int64").astype(str)

        need = cn == ""
        cn = cn.mask(need, eankey.map(lambda k: bifarma_cn.get(k, "")))
        # A CN below 150000 is an internal (non-national) code → treat as no CN.
        cn = cn.where(pd.to_numeric(cn, errors="coerce") >= 150000, "")
        # Canonical CN form: 6-digit zero-padded (as stored in tbi_sinonimos, e.g. 000045).
        cn = cn.map(lambda s: str(s).zfill(6) if str(s) else "")

        still_missing = cn == ""
        if still_missing.any():
            desc_s, lab_s, ean_s = col(FIELD["DESC"]), col(FIELD["LAB"]).map(_lab_name), col(FIELD["EAN"])
            print(f"⚠️ {int(still_missing.sum())} product(s) without CN (not in Zoho nor matched in BIFarma):")
            for i in df.index[still_missing]:
                print(f"    - [{lab_s[i]}] {desc_s[i]}  (EAN: {ean_s[i]})")

        # NOM LAB: prefer the vendor Shortname (by id), else fall back to the legal name.
        def nom_lab(v):
            return shortnames.get(_lab_id(v)) or _lab_name(v)

        # 'tipo de iva' from the IVA % via IVA_TIPO_MAP. Normalize the raw IVA
        # (strip '%', comma decimals, round) for the lookup; the output IVA column
        # itself stays the raw passthrough. Unmapped/blank IVA → <NA> (blank + flagged).
        iva_norm = pd.to_numeric(
            col(FIELD["IVA"]).astype(str).str.strip()
               .str.replace("%", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce",
        ).round()
        tipo_iva = iva_norm.map(IVA_TIPO_MAP).astype("Int64")

        # Base columns shared by every book (constant + product-level fields).
        base = pd.DataFrame({
            "CN":          cn,
            "EAN":         col(FIELD["EAN"]),
            "DESC":        col(FIELD["DESC"]),
            "PVL":         pd.to_numeric(col(FIELD["PVL"]), errors="coerce"),
            "IVA":         col(FIELD["IVA"]),
            "tipo de iva": tipo_iva,
            "cantidad":    CANTIDAD_DEFAULT,
            "NOM LAB":     col(FIELD["LAB"]).map(nom_lab),
            "_EFG":        col(FIELD["LAB"]).map(lambda v: _lab_id(v) in efg_ids),
        })

        summary = []
        unmapped_iva = {}   # CN → (lab, desc, raw IVA) for shipped rows with no tipo de iva
        excluded_labs = {_safe(x) for x in EXCLUDED_LABS}
        remote_dir = FTP_REMOTE_DIR.rstrip("/")
        with contextlib.ExitStack() as stack:
            if ENABLE_FTP_UPLOAD:
                conn = BaseHook.get_connection(FTP_CONN_ID)
                # ftp.ecoceutics.com requires explicit TLS → default to ftps (override via Extra).
                protocol = conn.extra_dejson.get("protocol", "ftps")
                print(f"[ecoLabs] FTP conn={FTP_CONN_ID} host={conn.host} user={conn.login} protocol={protocol}")
                ftp = stack.enter_context(FTPConn(
                    host=conn.host, user=conn.login, password=conn.password,
                    port=conn.port or 21, protocol=protocol,
                ))
            else:
                ftp = None
                os.makedirs(OUTPUT_DIR, exist_ok=True)

            def emit(fname: str, content: bytes) -> str:
                """Upload to FTP, or write locally when uploads are disabled. Returns the destination."""
                if ftp is not None:
                    remote = f"{remote_dir}/{fname}" if remote_dir and remote_dir != "." else fname
                    ftp.upload_bytes(content, remote)
                    return f"ftp:{remote}"
                path = os.path.join(OUTPUT_DIR, fname)
                with open(path, "wb") as fh:
                    fh.write(content)
                return path

            for n in BOOKS:
                dto_field = BOOK_DTO_FIELD.get(n)
                if not dto_field or dto_field not in df.columns:
                    print(f"[B{n}] field '{dto_field}' not present — skipping book {n}")
                    continue

                book = base.copy()
                # DESC BOOK = the book discount as a plain number: strip any '%', and turn a
                # fraction (0.33) into a percentage (33) — the legacy files use e.g. 12, 33, 42.
                disc = (col(dto_field).astype(str).str.strip()
                        .str.replace("%", "", regex=False).str.replace(",", ".", regex=False))
                disc = pd.to_numeric(disc, errors="coerce")
                disc = disc.mask((disc > 0) & (disc <= 1), disc * 100).round()
                book["DESC BOOK"] = disc.astype("Int64")

                # Keep only products that participate in this book (has a discount value).
                participates = book["DESC BOOK"].notna()
                book = book[participates & book["NOM LAB"].astype(str).str.strip().ne("")]
                if book.empty:
                    print(f"[B{n}] no participating products")
                    continue

                # One file per laboratory in this book, split by EFG (different naming).
                for (lab, is_efg), grp in book.groupby(["NOM LAB", "_EFG"]):
                    safe_lab = _safe(str(lab))
                    if safe_lab in excluded_labs:
                        print(f"[B{n}] {lab}: excluded — skipping")
                        continue
                    g = grp[OUT_COLUMNS]
                    # Flag shipped products whose IVA % didn't map to a tipo de iva.
                    miss = g["tipo de iva"].isna()
                    for i in g.index[miss]:
                        unmapped_iva[str(g.at[i, "CN"])] = (lab, g.at[i, "DESC"], g.at[i, "IVA"])
                    if is_efg:
                        fname = f"{FILE_PREFIX}_B{n}_{safe_lab}_EFG.csv"   # ECO_B1_CINFA_EFG.csv
                    else:
                        fname = f"{FILE_PREFIX}_{safe_lab}_B{n}.csv"       # ECO_ZAMBON_B1.csv
                    # semicolon-separated, comma decimals, no header, ISO-8859-1 — matching the legacy files.
                    csv_str = g.to_csv(sep=";", index=False, decimal=",", na_rep="", header=False)
                    content = csv_str.encode(ENCODING, errors="replace")
                    dest = emit(fname, content)
                    print(f"[B{n}] {lab}{' EFG' if is_efg else ''}: {len(g)} rows -> {dest}")
                    summary.append({"book": n, "lab": lab, "efg": bool(is_efg), "rows": len(g), "file": dest})

        if unmapped_iva:
            print(f"⚠️ {len(unmapped_iva)} shipped product(s) with unmapped/blank IVA — "
                  f"'tipo de iva' left blank (fix IVA in Zoho; valid: {sorted(IVA_TIPO_MAP)}):")
            for cnk, (lab, desc, iva) in unmapped_iva.items():
                print(f"    - [{lab}] CN {cnk} {desc}  (IVA: {iva!r})")

        print(f"Done — {'uploaded' if ENABLE_FTP_UPLOAD else 'wrote'} {len(summary)} book/lab file(s)")
        return summary

    # ── Wire ──
    products    = extract_products()
    vendor_info = extract_vendor_info()
    bifarma_cn  = extract_bifarma_cn()
    build_lab_books(products, vendor_info, bifarma_cn)


ecolabs_etl()
