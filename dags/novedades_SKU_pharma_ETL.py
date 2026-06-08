"""
Novedades SKU Pharma ETL

Extracts vendors from Zoho CRM, maps them against acordsEcos (SQL conn 1),
then pulls products/sales/purchases from SQL conn 2 for matched labs.

Runs the full pipeline IN PARALLEL for each client (L'Oreal, Haleon, Almirall, etc.).
Uses Airflow dynamic task mapping: extract_vendors produces a list of clients,
and process_client.expand() fans out one parallel branch per client.

Bulk data is written to disk under BASE_TMP and only file paths + small metadata
are passed via XCom to avoid serialisation limits.
"""
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration (from Airflow connections)
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID = "zoho_crm"
SQL_ACORDS_CONN_ID = "biOps_db"      # SQL connection 1 (acordsEcos)
SQL_PRODUCTS_CONN_ID = "BIFarma_db"  # SQL connection 2 (products/sales)

MAIL_RECIPIENTS = ["afochez@ecoceutics.com", "phojas@ecoceutics.com", "apirretas@ecoceutics.com"]

BASE_TMP = "/tmp/novedades_sku"

# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────

_DIALECT_DEFAULTS = {
    "mssql": {"driver": "pymssql", "port": 1433},
    "mysql": {"driver": "pymysql",  "port": 3306},
}

def _query_sql(conn_id: str, sql: str, dialect: str):
    """Run a query using the given dialect (mssql/mysql)."""
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    conn = BaseHook.get_connection(conn_id)
    defaults = _DIALECT_DEFAULTS[dialect]
    db = SQLConnection(
        db_host=conn.host,
        db_port=conn.port or defaults["port"],
        db_database=conn.schema,
        db_username=conn.login,
        db_password=conn.password,
        dialect=dialect,
        driver=defaults["driver"],
    )
    with db:
        return db.fech_dataframe(sql)

def _run_dir(ctx) -> str:
    import os
    dag_run = ctx.get("dag_run")
    run_id = str(dag_run.run_id) if dag_run is not None else "default"
    safe = run_id.replace(":", "_").replace("+", "_")
    d = os.path.join(BASE_TMP, safe)
    os.makedirs(d, exist_ok=True)
    return d

def _safe(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name)

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='novedades_SKU_pharma_ETL',
    description='ETL for Pharma SKU updates from Zoho per client',
    schedule=Variable.get("novedades_sku_pharma_schedule", default_var="0 2 1 * *"),  # default: 2am on 1st of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=2,  # limit parallelism to avoid overloading sources
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
)
def novedades_sku_pharma_etl():

    # ── 1. Extract all vendors and split by client ────────────
    @task
    def extract_vendors() -> list[dict]:
        """Extract vendors from Zoho; write per-client JSON files; return [{Vendor_Name, vendor_file}]."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        import json
        import pandas as pd

        ctx = get_current_context()
        run_dir = _run_dir(ctx)

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_vendors = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Vendors", params={"page": page, "per_page": 200})
            except Exception:
                break
            if not batch:
                break
            all_vendors.extend(batch)
            page += 1
            sleep(0.3)
        print(f"Extracted {len(all_vendors)} total vendors from Zoho")
        df = pd.DataFrame(all_vendors)
        if df.empty:
            print("No vendors found in Zoho.")
            return []
        else:
            print("Sample extracted vendors data:", df.head())
        df = df[df['Tipo_Acuerdo'].str.strip().isin(['Obligatorio', 'Opcional'])]
        df_owners = pd.json_normalize(df['Owner'].apply(lambda x: x if isinstance(x, dict) else {}).tolist())
        df['category_manager_name'] = df_owners['name'].values
        df['category_manager_email'] = df_owners['email'].values
        clients = []
        for Vendor_Name, group in df.groupby('Shortname'):
            vendors = group.to_dict('records')
            vendor_file = f"{run_dir}/vendors_{_safe(str(Vendor_Name))}.json"
            with open(vendor_file, 'w', encoding='utf-8') as f:
                json.dump(vendors, f, default=str)
            clients.append({"Vendor_Name": Vendor_Name, "vendor_file": vendor_file})
            print(f"Prepared client '{Vendor_Name}' with {len(vendors)} vendors")
        return clients

    # ── 2. Extract Contacts of the Vendors ───────────────────
    @task
    def extract_vendor_contacts() -> str:
        """Extract contacts from Zoho CRM; write to JSON file; return file path."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        import json
        import pandas as pd

        ctx = get_current_context()
        run_dir = _run_dir(ctx)

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_contacts = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Contacts", params={"page": page, "per_page": 200})
            except Exception:
                break
            if not batch:
                break
            all_contacts.extend(batch)
            page += 1
            sleep(0.3)
        print(f"Extracted {len(all_contacts)} total Contacts from Zoho CRM")
        df_CRM_contacts = pd.DataFrame(all_contacts)[['First_Name', 'Full_Name', 'Email', 'Vendor_Name', 'Idioma_Comunicaciones']]
        contacts_file = f"{run_dir}/contacts.json"
        with open(contacts_file, 'w', encoding='utf-8') as f:
            json.dump(df_CRM_contacts.to_dict('records'), f, default=str)
        return contacts_file

    # ── 1.5 Extract products already on CRM ──────────────────
    @task
    def extract_crm_products() -> str:
        """Extract products from Zoho CRM; write to CSV; return file path."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        import pandas as pd

        ctx = get_current_context()
        run_dir = _run_dir(ctx)

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_products = []
        page = 0
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
        print(f"Extracted {len(all_products)} total products from Zoho CRM")
        df_CRM_products = pd.DataFrame(all_products)[['Product_Code', 'EAN', 'Vendor_Name', 'Marca', 'Gama']]
        products_file = f"{run_dir}/crm_products.csv"
        df_CRM_products.to_csv(products_file, index=False)
        return products_file

    # ── 3. Extract vendor/lab mapping table from BI (once for all clients) ──
    @task
    def extract_acords() -> str:
        """Extract the vendor-to-lab mapping table from BI; write to CSV; return file path."""
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        run_dir = _run_dir(ctx)

        df = _query_sql(SQL_ACORDS_CONN_ID, "SELECT * FROM Vendors", dialect="mysql")
        for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        print(f"Extracted {len(df)} rows from Vendors with columns: {df.columns.tolist()}")
        acords_file = f"{run_dir}/acords.csv"
        df.to_csv(acords_file, index=False)
        return acords_file

    # ── 4. Per-client pipeline (runs in parallel) ─────────────
    @task_group(group_id="process_client")
    def process_client(client_data: dict, all_acords: str, all_crm_products: str, all_contacts: str):
        """Full ETL pipeline for a single client. Mapped dynamically."""

        @task
        def map_acords(client_data: dict, all_acords: str) -> dict:
            """Map client vendors against the acordsEcos table; return metadata + BIF_ids."""
            import json
            import pandas as pd

            Vendor_Name = client_data["Vendor_Name"]
            with open(client_data["vendor_file"], encoding='utf-8') as f:
                vendors = json.load(f)

            acords_df = pd.read_csv(all_acords, dtype=str)
            acords_df['lab_desc_lower'] = acords_df['Laboratori'].str.strip().str.lower()
            match = acords_df[acords_df['lab_desc_lower'] == Vendor_Name.strip().lower()]

            if match.empty:
                print(f"[{Vendor_Name}] No match found in acords lab_description")
                return {
                    "Vendor_Name": Vendor_Name, "laboratory_id": [],
                    "vendor_id": "", "cm_email": "", "cm_name": "", "NIF": "",
                }

            lab_groups  = match['Laboratori'].unique().tolist()
            all_matched = acords_df[acords_df['Laboratori'].isin(lab_groups)].drop(columns=['lab_desc_lower'])
            bif_ids     = all_matched['BIF_id'].unique().tolist()
            first_vendor = vendors[0] if vendors else {}
            vendor_id    = first_vendor.get("id", "")
            cm_email     = first_vendor.get("category_manager_email", "")
            cm_name      = first_vendor.get("category_manager_name", "")
            print(f"[{Vendor_Name}] -> group(s): {lab_groups} -> BIF_ids: {bif_ids}")
            return {
                "Vendor_Name":   Vendor_Name,
                "vendor_id":     vendor_id,
                "cm_email":      cm_email,
                "cm_name":       cm_name,
                "NIF":           first_vendor.get("NIF", ""),
                "laboratory_id": bif_ids,
            }

        @task
        def filter_products(mapped_result: dict) -> dict:
            """Query products for this vendor's BIF_ids; write to CSV; return metadata + products_file path."""
            import pandas as pd
            from airflow.operators.python import get_current_context
            from utils.clsDate import DateHelper
            from airflow.hooks.base import BaseHook
            from utils.clsSQL import SQLConnection

            ctx = get_current_context()
            run_dir = _run_dir(ctx)

            Vendor_Name   = mapped_result["Vendor_Name"]
            bif_ids       = mapped_result["laboratory_id"]
            products_file = f"{run_dir}/products_{_safe(Vendor_Name)}.csv"

            meta = {
                "Vendor_Name":   Vendor_Name,
                "vendor_id":     mapped_result.get("vendor_id", ""),
                "cm_email":      mapped_result.get("cm_email", ""),
                "cm_name":       mapped_result.get("cm_name", ""),
                "NIF":           mapped_result.get("NIF", ""),
                "products_file": "",
            }

            if not bif_ids:
                print(f"[{Vendor_Name}] No BIF_ids — skipping product query")
                return meta

            ids_str   = ", ".join(f"'{x.strip()}'" for x in bif_ids)
            d         = DateHelper()
            curr_yy   = d.anyo
            prev_yy   = d.offset(years=-1).anyo
            fin_month = str(d.mes).zfill(2)

            GROUP_BY = """GROUP BY pr.codproducto, pr.desproducto, pr.codlab, pr.deslab,
                de.identidad, de.iddelegacion, de.delegacion, pr.idproducto,
                f.nombresubgrupoproducto, pr.idsuperfamilia, f.nombresuperfamiliaeco,
                pr.idfamilia, f.nombrefamiliaeco"""
            BASE_FROM = """FROM dbo.bench_dwComprasVentasMesS T1
                INNER JOIN dbo.tme_delegaciones de ON T1.idendeS = de.idendeS
                INNER JOIN dbo.tbi_productosERS pr ON T1.idendeS = pr.idendeS AND T1.idproducto = pr.idproducto
                INNER JOIN dbo.vteco_familias f    ON f.idfamiliaeco = pr.idfamilia"""
            BASE_COLS = """pr.codproducto AS CodProducto, pr.desproducto AS Producto,
                pr.codlab AS IdLaboratorio, pr.deslab AS Laboratorio,
                de.identidad AS IdEntidad, de.iddelegacion AS IdDelegacion,
                pr.idproducto AS IdProducto"""
            ECO_FILTER = "(T1.idendes IN (SELECT idendes FROM tme_delegaciones WHERE grupoCompras = 'ECO'))"
            LAB_FILTER = f"pr.codLab IN ({ids_str})"

            _conn = BaseHook.get_connection(SQL_PRODUCTS_CONN_ID)
            db = SQLConnection(
                db_host=_conn.host, db_port=_conn.port or 1433,
                db_database=_conn.schema, db_username=_conn.login,
                db_password=_conn.password, dialect="mssql", driver="pymssql",
            )
            with db:
                act_df = db.fech_dataframe(f"""
                    SELECT {BASE_COLS}, MIN(pr.stockActual) AS Estoc,
                        SUM(ISNULL(T1.cantidad,0)) AS CantidadAct, SUM(ISNULL(T1.importe,0)) AS ImporteAct,
                        SUM(ISNULL(T1.cantidadcompra,0)) AS CantidadCompraAct, SUM(ISNULL(T1.importecompra,0)) AS ImporteCompraAct
                    {BASE_FROM}
                    WHERE T1.anyomes >= {curr_yy}01 AND T1.anyomes <= {curr_yy}{fin_month}
                        AND {ECO_FILTER} AND {LAB_FILTER} {GROUP_BY}""")
                ant_df = db.fech_dataframe(f"""
                    SELECT {BASE_COLS},
                        SUM(ISNULL(T1.cantidad,0)) AS CantidadAnt, SUM(ISNULL(T1.importe,0)) AS ImporteAnt,
                        SUM(ISNULL(T1.cantidadcompra,0)) AS CantidadCompraAnt, SUM(ISNULL(T1.importecompra,0)) AS ImporteCompraAnt
                    {BASE_FROM}
                    WHERE T1.anyomes >= {prev_yy}01 AND T1.anyomes <= {prev_yy}{fin_month}
                        AND {ECO_FILTER} AND {LAB_FILTER} {GROUP_BY}""")

            for df_ in (act_df, ant_df):
                df_['CodProducto'] = pd.to_numeric(df_['CodProducto'], errors='coerce').astype('Int64')

            MERGE_KEYS  = ['CodProducto', 'IdLaboratorio', 'IdEntidad', 'IdDelegacion', 'IdProducto']
            products_df = pd.merge(act_df, ant_df, on=MERGE_KEYS, how='left')
            for col in ['Producto', 'Laboratorio']:
                if f'{col}_x' in products_df.columns:
                    products_df.rename(columns={f'{col}_x': col}, inplace=True)
                    products_df.drop(columns=[f'{col}_y'], errors='ignore', inplace=True)
            for col in ['CantidadAnt', 'ImporteAnt', 'CantidadCompraAnt', 'ImporteCompraAnt']:
                products_df[col] = products_df.get(col, pd.Series(dtype=float)).fillna(0.0)
            print(f"[{Vendor_Name}] {len(products_df)} products for BIF_ids {bif_ids}")
            products_df.to_csv(products_file, index=False)
            meta["products_file"] = products_file
            return meta

        @task
        def new_products(all_crm_products: str, filtered_products: dict) -> dict:
            """SQL products not already in CRM; write to CSV; return metadata + new_products_file path."""
            import pandas as pd
            from airflow.operators.python import get_current_context

            ctx = get_current_context()
            run_dir = _run_dir(ctx)

            Vendor_Name       = filtered_products["Vendor_Name"]
            new_products_file = f"{run_dir}/new_{_safe(Vendor_Name)}.csv"

            meta = {
                "Vendor_Name":       Vendor_Name,
                "vendor_id":         filtered_products.get("vendor_id", ""),
                "cm_email":          filtered_products.get("cm_email", ""),
                "cm_name":           filtered_products.get("cm_name", ""),
                "NIF":               filtered_products.get("NIF", ""),
                "new_products_file": "",
            }

            if not filtered_products.get("products_file"):
                print(f"[{Vendor_Name}] No products file — skipping.")
                return meta

            try:
                client_prods_df = pd.read_csv(filtered_products["products_file"])
            except pd.errors.EmptyDataError:
                print(f"[{Vendor_Name}] Products file is empty — skipping.")
                return meta

            try:
                crm_prods_df = pd.read_csv(all_crm_products)
            except pd.errors.EmptyDataError:
                crm_prods_df = pd.DataFrame()

            print(f"[{Vendor_Name}] {len(client_prods_df)} SQL products, {len(crm_prods_df)} CRM products")

            if client_prods_df.empty:
                print(f"[{Vendor_Name}] No SQL products — skipping.")
                return meta

            client_prods_df['CodProducto'] = pd.to_numeric(client_prods_df['CodProducto'], errors='coerce').astype('Int64')

            if crm_prods_df.empty or 'Product_Code' not in crm_prods_df.columns:
                new_prods_df = client_prods_df.copy()
                new_prods_df['EAN'] = pd.NA
            else:
                crm_prods_df['Product_Code'] = pd.to_numeric(crm_prods_df['Product_Code'], errors='coerce').astype('Int64')
                crm_prods_df['EAN']          = pd.to_numeric(crm_prods_df['EAN'],          errors='coerce').astype('Int64')
                matched_by_code = set(pd.merge(client_prods_df, crm_prods_df, left_on='CodProducto', right_on='Product_Code', how='inner')['CodProducto'])
                matched_by_ean  = set(pd.merge(client_prods_df, crm_prods_df, left_on='CodProducto', right_on='EAN',          how='inner')['CodProducto'])
                already_in_crm  = matched_by_code | matched_by_ean
                new_prods_df    = client_prods_df[~client_prods_df['CodProducto'].isin(already_in_crm)]
                ean_lookup      = crm_prods_df[['Product_Code', 'EAN']].rename(columns={'Product_Code': 'CodProducto'})
                new_prods_df    = pd.merge(new_prods_df, ean_lookup, on='CodProducto', how='left').drop_duplicates(subset=['CodProducto'])

            print(f"[{Vendor_Name}] {len(new_prods_df)} products not in CRM")
            new_prods_df.to_csv(new_products_file, index=False)
            meta["new_products_file"] = new_products_file
            return meta

        @task
        def make_category_file(result: dict, all_contacts: str) -> dict:
            """Write per-vendor CSV to disk; return metadata + file_path for CM aggregation."""
            import csv, json, os, traceback
            import fcntl  # type: ignore[import]
            import pandas as pd
            from airflow.operators.python import get_current_context

            ctx = get_current_context()
            run_dir = _run_dir(ctx)

            Vendor_Name = result.get("Vendor_Name", "Unknown")
            vendor_id   = result.get("vendor_id", "")
            cm_email    = result.get("cm_email", "") or "meslava@ecoceutics.com"
            cm_name     = result.get("cm_name", "")
            nif         = result.get("NIF", "")

            print(f"[{Vendor_Name}] make_category_file start — cm={cm_email}")

            meta = {
                "Vendor_Name":       Vendor_Name,
                "cm_email":          cm_email,
                "cm_name":           cm_name,
                "first_name":        "",
                "new_products_file": result.get("new_products_file") or "",
                "file_path":         "",
            }

            if not result.get("new_products_file"):
                print(f"[{Vendor_Name}] No new_products file — skipping.")
                return meta

            try:
                new_prods_df = pd.read_csv(result["new_products_file"])
            except pd.errors.EmptyDataError:
                print(f"[{Vendor_Name}] new_products file is empty — skipping.")
                return meta
            except Exception as e:
                print(f"[{Vendor_Name}] ERROR reading new_products file: {e}\n{traceback.format_exc()}")
                raise

            new_prods = new_prods_df.to_dict('records')
            print(f"[{Vendor_Name}] {len(new_prods)} products loaded")

            first_name = ""
            if vendor_id:
                try:
                    with open(all_contacts, encoding='utf-8') as f:
                        contacts_list = json.load(f)
                    contact = next(
                        (c for c in contacts_list if isinstance(c.get("Vendor_Name"), dict) and c["Vendor_Name"].get("id") == vendor_id),
                        None,
                    )
                    if contact:
                        first_name = contact.get("First_Name", "")
                except Exception as e:
                    print(f"[{Vendor_Name}] WARNING reading contacts: {e}")
            meta["first_name"] = first_name

            new_prods = [row for row in new_prods if float(row.get('ImporteCompraAct') or 0) > 0]
            print(f"[{Vendor_Name}] {len(new_prods)} products with purchases")

            if not new_prods:
                print(f"[{Vendor_Name}] No products with purchases — skipping file write.")
                return meta

            DEFAULTS = {
                'Estado': 'Inactivo', 'Novedad': 'Si', 'Opcional': 'Si',
                'Dto. Book 1': 0, 'Dto. Book 2': 0, 'Dto. Book 3': 0,
                'Unid. BOOK 1': 0, 'Unid. BOOK 2': 0, 'Unid. BOOK 3': 0,
            }
            FIELDNAMES = [
                'CodProducto', 'EAN', 'Producto', 'Laboratorio', 'NIF', 'Gama', 'Marca',
                'PVL', 'IVA', 'Dto. Book 1', 'Dto. Book 2', 'Dto. Book 3',
                'Unid. BOOK 1', 'Unid. BOOK 2', 'Unid. BOOK 3',
                'Pack', 'Novedad', 'Opcional', 'Estado', 'Precio Unitario Compra',
                'ImporteCompraAct', 'CantidadCompraAct',
            ]
            for row in new_prods:
                row['NIF'] = nif
                for field, default in DEFAULTS.items():
                    row.setdefault(field, default)
                try:
                    row['Precio Unitario Compra'] = round(float(row['ImporteCompraAct']) / float(row['CantidadCompraAct']), 2)
                except (ZeroDivisionError, TypeError, ValueError):
                    row['Precio Unitario Compra'] = ''

            try:
                safe_cm      = _safe(cm_email)
                file_path    = f"{run_dir}/notify_{safe_cm}.csv"
                write_header = not os.path.exists(file_path)
                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    fcntl.flock(f, fcntl.LOCK_EX)  # type: ignore[attr-defined]
                    try:
                        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore', restval='', delimiter=';')
                        if write_header:
                            writer.writeheader()
                        writer.writerows([{str(k): v for k, v in row.items()} for row in new_prods])
                    finally:
                        fcntl.flock(f, fcntl.LOCK_UN)  # type: ignore[attr-defined]
                print(f"[{Vendor_Name}] Appended {len(new_prods)} products to {file_path}")
            except Exception as e:
                print(f"[{Vendor_Name}] ERROR writing file: {e}\n{traceback.format_exc()}")
                raise

            meta["file_path"] = file_path
            return meta

        # Wire the per-client pipeline
        mapped            = map_acords(client_data, all_acords)
        filtered_products = filter_products(mapped)
        result            = new_products(all_crm_products, filtered_products)
        return make_category_file(result, all_contacts)

    @task(trigger_rule="all_done")
    def notify_by_sender() -> None:
        """One email per CM: scan run dir for notify_*.csv files and send each one."""
        import os, glob, traceback
        import fcntl  # type: ignore[import]
        from airflow.operators.python import get_current_context
        from utils.clsZohoMailing import ZohoMailer

        ctx      = get_current_context()
        run_dir  = _run_dir(ctx)
        files    = glob.glob(f"{run_dir}/notify_*.csv")
        print(f"notify_by_sender: found {len(files)} CM file(s) in {run_dir}")
        try:
            mailer = ZohoMailer()
        except Exception as e:
            print(f"ERROR initialising ZohoMailer: {e}\n{traceback.format_exc()}")
            raise

        for fp in files:
            cm_email = os.path.basename(fp).replace("notify_", "").replace(".csv", "").replace("_", "@", 1).replace("_", ".")
            try:
                with open(fp, encoding="utf-8") as f:
                    fcntl.flock(f, fcntl.LOCK_SH)  # type: ignore[attr-defined]
                    try:
                        content = f.read()
                    finally:
                        fcntl.flock(f, fcntl.LOCK_UN)  # type: ignore[attr-defined]

                total_rows = content.count("\n") - 1
                print(f"[{cm_email}] Sending {total_rows} rows")
                html_body = (
                    f"<p>El proceso Novedades SKU ha encontrado {total_rows} productos nuevos.</p>"
                    f"<p>Se adjunta el listado en formato CSV.</p>"
                    f"<p>Por favor tu ayuda para rellenar los datos de PVL, Iva, Marca, Gamma.</p>"
                    f"<p>Lo necesitamos con urgencia, para actualizar los datos de SO y SI correctamente.</p>"
                    f"<p>Saludos.</p>"
                )
                subject = f"[Novedades SKU] {total_rows} producto(s) nuevo(s)"
                mailer.send(
                    to=[{"address": cm_email, "name": ""}],
                    subject=subject,
                    html_body=html_body,
                    attachments=[{"content": content, "name": "novedades_SKU.csv", "mime_type": "text/csv"}],
                )
                print(f"[{cm_email}] Sent OK")
                os.remove(fp)
            except Exception as e:
                print(f"[{cm_email}] ERROR: {e}\n{traceback.format_exc()}")
                raise
            print(f"Sent to {cm_email}: {total_rows} rows")
            try:
                os.remove(fp)
            except OSError:
                pass

    @task(trigger_rule="all_done")
    def notify_summary(results: list[dict]) -> None:
        """Send summary email with SI/SO CSV for all labs after all clients are processed."""
        import csv, io, os
        import pandas as pd
        from utils.clsZohoMailing import ZohoMailer

        if not isinstance(results, list):
            results = [results] if results else []

        all_rows       = []
        count_per_vendor = {}
        for value in results:
            if not isinstance(value, dict):
                continue
            vendor = value.get("Vendor_Name", "Unknown")
            nf     = value.get("new_products_file")
            if not nf or not os.path.exists(nf):
                count_per_vendor[vendor] = 0
                continue
            try:
                df = pd.read_csv(nf)
            except pd.errors.EmptyDataError:
                count_per_vendor[vendor] = 0
                continue
            if df.empty:
                count_per_vendor[vendor] = 0
                continue
            if 'Laboratorio' not in df.columns:
                df['Laboratorio'] = vendor
            else:
                df['Laboratorio'] = df['Laboratorio'].fillna(vendor)
            count_per_vendor[vendor] = len(df)
            all_rows.extend(df.to_dict('records'))

        total = len(all_rows)
        print(f"notify_summary: {total} total new products across all vendors")

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=[
            'Laboratorio', 'CodProducto', 'EAN', 'Producto',
            'CantidadAct', 'ImporteAct', 'CantidadAnt', 'ImporteAnt',
            'CantidadCompraAct', 'ImporteCompraAct', 'CantidadCompraAnt', 'ImporteCompraAnt',
        ], extrasaction='ignore', restval='', delimiter=';')
        writer.writeheader()
        writer.writerows([{str(k): v for k, v in row.items()} for row in all_rows])
        csv_content = buf.getvalue()

        table_rows = "".join(
            f"<tr><td>{vendor}</td><td style='text-align:center'>{count or '—'}</td></tr>"
            for vendor, count in count_per_vendor.items()
        )
        vendors_with_prods = [v for v, n in count_per_vendor.items() if n > 0]
        html_body = (
            f"<p>Resumen del proceso <b>Novedades SKU</b>:</p>"
            f"<table border='1' cellpadding='4' cellspacing='0'>"
            f"<tr><th>Proveedor</th><th>Nuevos productos</th></tr>"
            f"{table_rows}"
            f"</table>"
            f"<p>Se adjunta CSV con detalle SI/SO de todos los productos nuevos.</p>"
        )
        subject     = f"[Novedades SKU] Resumen — {total} producto(s) nuevo(s) en {len(vendors_with_prods)} proveedor(es)"
        attachments = [{"content": csv_content, "name": "novedades_summary_SISO.csv", "mime_type": "text/csv"}] if all_rows else []
        ZohoMailer().send(
            to=[{"address": "meslava@ecoceutics.com", "name": "Marc Eslava"}],
            subject=subject,
            html_body=html_body,
            attachments=attachments,
        )
        print(f"Summary sent: {len(count_per_vendor)} vendors processed, {total} new products total")

    # ── Wire it all together ──────────────────────────────────
    clients       = extract_vendors()
    crm_products  = extract_crm_products()
    contacts      = extract_vendor_contacts()
    acords        = extract_acords()
    all_summaries = process_client.partial(all_acords=acords, all_crm_products=crm_products, all_contacts=contacts).expand(client_data=clients)
    sender_task = notify_by_sender()
    all_summaries >> sender_task
    notify_summary(all_summaries)

# Instantiate the DAG
novedades_sku_pharma_etl()