"""
Novedades SKU Pharma ETL

Extracts vendors from Zoho CRM, maps them against acordsEcos (SQL conn 1),
then pulls products/sales/purchases from SQL conn 2 for matched labs.

Runs the full pipeline IN PARALLEL for each client (L'Oreal, Haleon, Almirall, etc.).
Uses Airflow dynamic task mapping: extract_vendors produces a list of clients,
and process_client.expand() fans out one parallel branch per client.
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

MAIL_RECIPIENTS = ["afochez@ecoceutics.com", "phojas@ecoceutcis.com", "apirretas@ecoceutics.com"]

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

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────
#   test rama funcionar
@dag(
    dag_id='novedades_SKU_pharma_ETL',
    description='ETL for Pharma SKU updates from Zoho per client',
    schedule= Variable.get("novedades_sku_pharma_schedule", default_var="0 2 * * *"),  # default: daily at 2am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,  # limit parallelism to avoid overloading sources
    default_args={
        'owner': 'data-team',
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
    },
)
def novedades_sku_pharma_etl():

    # ── 1. Extract all vendors and split by client ────────────
    @task
    def extract_vendors() -> list[dict]:
        """Extract vendors from Zoho and return one dict per client group."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        import pandas as pd

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_vendors = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Vendors", params={"page": page, "per_page": 200})
                all_vendors.extend(batch)
            except Exception:
                break
            page += 1
            sleep(0.3)
        print(f"Extracted {len(all_vendors)} total vendors from Zoho")
        df = pd.DataFrame(all_vendors)
        if df.empty:
            print("No vendors found in Zoho.")
            return []
        else:
            print("Sample extracted vendors data:", df.head())
        # Group vendors by client (assuming 'Vendor_Name' field exists)
        df = df[df['Tipo_Acuerdo'].str.strip().isin(['Obligatorio', 'Opcional'])]
        df_owners = pd.json_normalize(df['Owner'].apply(lambda x: x if isinstance(x, dict) else {}))
        df['category_manager_name'] = df_owners['name'].values
        df['category_manager_email'] = df_owners['email'].values
        clients = []
        for Vendor_Name, group in df.groupby('Shortname'):
            vendors = group.to_dict('records')
            clients.append({
                "Vendor_Name": Vendor_Name,
                "vendor": vendors,
            })
            print(f"Prepared client '{Vendor_Name}' with {len(vendors)} vendors")
        return clients
    #   2 Extract Contacts of the Vendors
    @task
    def extract_vendor_contacts() -> list[dict]:
        """Extract products already on CRM to compare with SQL data."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        import pandas as pd

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_contacts = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Contacts", params={"page": page, "per_page": 200})
                all_contacts.extend(batch)
            except Exception:
                break
            sleep(0.3)
            page += 1
        print(f"Extracted {len(all_contacts)} total Contacts from Zoho CRM")
        df_CRM_contacts = pd.DataFrame(all_contacts)[['First_Name', 'Full_Name', 'Email', 'Vendor_Name', 'Idioma_Comunicaciones']]
        return df_CRM_contacts.to_dict('records')
    #   1.5 Extract products already on CRM
    @task
    def extract_crm_products() -> list[dict]:
        """Extract products already on CRM to compare with SQL data."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        import pandas as pd

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_products = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Products", params={"page": page, "per_page": 200})
                all_products.extend(batch)
            except Exception:
                break
            sleep(0.3)
            page += 1
        print(f"Extracted {len(all_products)} total products from Zoho CRM")
        df_CRM_products = pd.DataFrame(all_products)[['Product_Code', 'EAN', 'Vendor_Name']]
        return df_CRM_products.to_dict('records')


    # ── 3. Extract vendor/lab mapping table from BI (once for all clients) ──
    @task
    def extract_acords() -> list[dict]:
        """Extract the vendor-to-lab mapping table from BI. Runs ONCE."""
        df = _query_sql(SQL_ACORDS_CONN_ID, "SELECT * FROM VendorMapping", dialect="mysql")
        # Convert datetime columns to ISO strings so XCom can serialize them
        for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        print(f"Extracted {len(df)} rows from VendorMapping")
        print("Extracted acordes data with columns:", df.columns.tolist())
        return df.to_dict('records')

    # ── 4. Per-client pipeline (runs in parallel) ─────────────
    @task_group(group_id="process_client")
    def process_client(client_data: dict, all_acords: list[dict], all_crm_products: list[dict], all_contacts: list[dict]):
        """Full ETL pipeline for a single client. Mapped dynamically."""

        @task
        def map_acords(client_data: dict, all_acords: list[dict]) -> dict:
            """Map client vendors against the acordsEcos table."""
            import pandas as pd
            pd.set_option('display.max_columns', None)

            Vendor_Name = client_data["Vendor_Name"]
            acords_df   = pd.DataFrame(all_acords)

            # Step 1: find which Laboratori group this vendor belongs to via lab_description
            acords_df['lab_desc_lower'] = acords_df['Laboratori'].str.strip().str.lower()
            match = acords_df[acords_df['lab_desc_lower'] == Vendor_Name.strip().lower()]

            if match.empty:
                print(f"[{Vendor_Name}] No match found in acords lab_description")
                return {"Vendor_Name": Vendor_Name, "laboratory_id": [], "mapped": []}

            # Step 2: collect ALL BIF_ids sharing the same Laboratori group (1:n)
            lab_groups = match['Laboratori'].unique().tolist()
            all_matched = acords_df[acords_df['Laboratori'].isin(lab_groups)].drop(columns=['lab_desc_lower'])
            bif_ids = all_matched['BIF_id'].unique().tolist()
            vendor_id = client_data["vendor"][0].get("id", "") if client_data.get("vendor") else ""
            print(f"[{Vendor_Name}] -> group(s): {lab_groups} -> BIF_ids: {bif_ids}")
            return {
                "Vendor_Name": Vendor_Name,
                "vendor_id": vendor_id,
                "laboratory_id": bif_ids,
                "mapped": all_matched.to_dict('records'),
            }

        @task
        def filter_products(mapped_result: dict) -> dict:
            """Query products directly for this vendor's BIF_ids."""
            import pandas as pd
            from utils.clsDate import DateHelper
            from airflow.hooks.base import BaseHook
            from utils.clsSQL import SQLConnection

            Vendor_Name = mapped_result["Vendor_Name"]
            bif_ids     = mapped_result["laboratory_id"]

            if not bif_ids:
                print(f"[{Vendor_Name}] No BIF_ids — skipping product query")
                return {"Vendor_Name": Vendor_Name, "products": [], "mapped": mapped_result.get("mapped", [])}

            ids_str = ", ".join(f"'{x.strip()}'" for x in bif_ids)
            d = DateHelper()
            curr_yy   = d.anyo
            prev_yy   = d.offset(years=-1).anyo
            curr_year = d.offset(years=-2).anyo
            prev_year = d.offset(years=-2).anyo
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

            MERGE_KEYS = ['CodProducto', 'IdLaboratorio', 'IdEntidad', 'IdDelegacion', 'IdProducto']
            products_df = pd.merge(act_df, ant_df, on=MERGE_KEYS, how='left')
            for col in ['CantidadAnt', 'ImporteAnt', 'CantidadCompraAnt', 'ImporteCompraAnt']:
                products_df[col] = products_df.get(col, pd.Series(dtype=float)).fillna(0.0)
            print(f"[{Vendor_Name}] {len(products_df)} products for BIF_ids {bif_ids}")
            return {
                "Vendor_Name": Vendor_Name,
                "products": products_df.to_dict('records'),
                "mapped": mapped_result["mapped"],
            }

        @task
        def new_products(all_crm_products : dict, filtered_products: dict) -> dict:
            """Compare the client's products against CRM and keep only new ones."""
            import pandas as pd
            pd.set_option('display.max_columns', None)

            Vendor_Name = filtered_products["Vendor_Name"]
            client_prods_df = pd.DataFrame(filtered_products["products"])
            crm_prods_df    = pd.DataFrame(all_crm_products)
            print(f"[{Vendor_Name}] Comparing {len(client_prods_df)} client products against {len(crm_prods_df)} CRM products")
            
            client_prods_df['CodProducto'] = pd.to_numeric(client_prods_df['CodProducto'], errors='coerce').round(0)
            crm_prods_df['Product_Code'] = pd.to_numeric(crm_prods_df['Product_Code'], errors='coerce').round(0)
            crm_prods_df['EAN'] = pd.to_numeric(crm_prods_df['EAN']).fillna(0).round(0) 
            
            matched_by_code = set(pd.merge(client_prods_df, crm_prods_df, left_on='CodProducto', right_on='Product_Code', how='inner')['CodProducto'])
            matched_by_ean  = set(pd.merge(client_prods_df, crm_prods_df, left_on='CodProducto', right_on='EAN',          how='inner')['CodProducto'])
            already_in_crm  = matched_by_code | matched_by_ean
            new_prods_df    = client_prods_df[~client_prods_df['CodProducto'].isin(already_in_crm)]
            new_prods_df = new_prods_df.drop_duplicates(subset=['CodProducto','EAN'])
            print(f"[{Vendor_Name}] Found {len(new_prods_df)} new products not in CRM")
            return {
                "Vendor_Name": Vendor_Name,
                "vendor_id": filtered_products.get("vendor_id", ""),
                "new_products": new_prods_df.to_dict('records'),
                "mapped": filtered_products["mapped"],
            }


        @task
        def notify_categories(result: dict, all_contacts: list[dict]) -> None:
            """Send a notification email for this vendor's new products."""
            from utils.clsZohoMailing import ZohoMailer
            from utils.clsDate import DateHelper

            Vendor_Name = result.get("Vendor_Name", "Unknown")
            vendor_id   = result.get("vendor_id", "")
            new_prods   = result.get("new_products", [])

            first_name = ""
            if vendor_id and all_contacts:
                contact = next((c for c in all_contacts if isinstance(c.get("Vendor_Name"), dict) and c["Vendor_Name"].get("id") == vendor_id), None)
                if contact:
                    first_name = contact.get("First_Name", "")
            print(f"[{Vendor_Name}] Contact First_Name: '{first_name}'")


            if not new_prods:
                print(f"[{Vendor_Name}] No new products — skipping notification.")
                return

            import csv, io
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=[
                'CodProducto', 'EAN', 'Producto_x', 'Laboratorio_x', 'Marca', 'GAMMA',
                'PVL', 'IVA', 'Dto. Book 1', 'Dto. Book 2', 'Dto. Book 3',
                'Unid. BOOK 1', 'Unid. BOOK 2', 'Unid. BOOK 3',
                'Pack', 'Novedad', 'Opcional', 'Estado', 'Precio Unitario Compra', 'ImporteCompraAct', 'CantidadCompraAct'
            ], extrasaction='ignore', restval='', delimiter=';')
            writer.writeheader()
            DEFAULTS = {
                'Estado': 'Inactivo',
                'Novedad': 'Si',
                'Opcional': 'Si',
                'Dto. Book 1':0, 
                'Dto. Book 2':0, 
                'Dto. Book 3':0,
                'Unid. BOOK 1':0, 
                'Unid. BOOK 2':0, 
                'Unid. BOOK 3':0
                
            }

            for row in new_prods:
                for field, default in DEFAULTS.items():
                    row.setdefault(field, default)

            for row in new_prods:
                try:
                    row['Precio Unitario Compra'] = round(float(row['ImporteCompraAct']) / float(row['CantidadCompraAct']), 2)
                except (ZeroDivisionError, TypeError, ValueError):
                    row['Precio Unitario Compra'] = ''
                writer.writerow(row)
                
            for row in new_prods:
                row['Producto'] = row.pop('Producto_x', '')
                row['Laboratorio'] = row.pop('Laboratorio_x', '')
                writer.writerow(row)

            csv_content = buf.getvalue()
            
            d =DateHelper()
            
            greeting = f"<p>Hola {first_name},</p>" if first_name else ""
            html_body = (
                f"{greeting}"
                f"<p>El proceso <b>Novedades SKU</b> ha encontrado <b>{len(new_prods)}</b> productos nuevos para <b>{Vendor_Name}</b>.</p>"
                f"<p>Se adjunta el listado en formato CSV.</p>"
                f"<p>PUC calculado en base a YTD actual -> {d.anyomes}</p>"
            )
            subject = f"[Novedades SKU] {Vendor_Name} — {len(new_prods)} producto(s) nuevo(s)"
            mailer = ZohoMailer()
            mailer.send(
                to=[{"address": "meslava@ecoceutics.com", "name": "Marc Eslava"}],
                subject=subject,
                html_body=html_body,
                attachments=[{"content": csv_content, "name": f"novedades_{Vendor_Name}.csv", "mime_type": "text/csv"}],
            )
            print(f"[{Vendor_Name}] Notification sent ({len(new_prods)} new products)")

        # Wire the per-client pipeline
        mapped            = map_acords(client_data, all_acords)
        filtered_products = filter_products(mapped)
        result            = new_products(all_crm_products, filtered_products)
        notify_categories(result, all_contacts)

    # ── Wire it all together ──────────────────────────────────
    clients      = extract_vendors()
    crm_products = extract_crm_products()
    contacts     = extract_vendor_contacts()
    acords       = extract_acords()
    process_client.partial(all_acords=acords, all_crm_products=crm_products, all_contacts=contacts).expand(client_data=clients)

# Instantiate the DAG
novedades_sku_pharma_etl()