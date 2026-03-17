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
    max_active_tasks=3,  # limit parallelism to avoid overloading sources
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
        for Vendor_Name, group in df.groupby('Vendor_Name'):
            vendors = group.to_dict('records')
            clients.append({
                "Vendor_Name": Vendor_Name,
                "vendor": vendors,
            })
            print(f"Prepared client '{Vendor_Name}' with {len(vendors)} vendors")
        return clients
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
        df_CRM_products = pd.DataFrame(all_products)
        df_CRM_products = df_CRM_products[['Product_Code', 'EAN', 'Vendor_Name']]
        return all_products
    # ── 2. Extract products (once for all clients) ──────────────
    @task
    def extract_products() -> list[dict]:
        """Extract products/sales for current and previous year. Runs once."""
        import pandas as pd
        from utils.clsDate import DateHelper
        try:
            d = DateHelper()
            curr_yy = d.offset(years=-1).anyo_short
            prev_yy = d.offset(years=-2).anyo_short
            curr_year = d.offset(years=-1).anyo
            prev_year = d.offset(years=-2).anyo

            GROUP_BY = """
                GROUP BY
                    pr.codproducto, pr.desproducto, pr.codlab, pr.deslab,
                    de.identidad, de.iddelegacion, de.delegacion,
                    pr.idproducto, f.nombresubgrupoproducto,
                    pr.idsuperfamilia, f.nombresuperfamiliaeco,
                    pr.idfamilia, f.nombrefamiliaeco"""

            BASE_FROM = """
                FROM dbo.bench_dwComprasVentasMesSNew T1
                INNER JOIN dbo.tme_delegaciones de ON T1.idendeS = de.idendeS
                INNER JOIN dbo.tbi_productosERS pr ON T1.idendeS = pr.idendeS AND T1.idproducto = pr.idproducto
                INNER JOIN dbo.vteco_familias f    ON f.idfamiliaeco = pr.idfamilia"""

            BASE_COLS = """
                    pr.codproducto AS CodProducto,
                    pr.desproducto AS Producto,
                    pr.codlab AS IdLaboratorio,
                    pr.deslab AS Laboratorio,
                    de.identidad AS IdEntidad,
                    de.iddelegacion AS IdDelegacion,
                    de.delegacion AS Delegacion,
                    pr.idproducto AS IdProducto,
                    f.nombresubgrupoproducto AS SubGrupoProducto,
                    pr.idsuperfamilia AS IdSuperFamilia,
                    f.nombresuperfamiliaeco AS SuperFamilia,
                    pr.idfamilia AS IdFamilia,
                    f.nombrefamiliaeco AS Familia"""

            ECO_FILTER = "(T1.idendes IN (SELECT idendes FROM tme_delegaciones WHERE grupoCompras = 'ECO'))"

            from airflow.hooks.base import BaseHook
            from utils.clsSQL import SQLConnection
            _conn = BaseHook.get_connection(SQL_PRODUCTS_CONN_ID)
            db = SQLConnection(
                db_host=_conn.host,
                db_port=_conn.port or 1433,
                db_database=_conn.schema,
                db_username=_conn.login,
                db_password=_conn.password,
                dialect="mssql",
                driver="pymssql",
            )
            with db:
                # ── Current year ──
                act_df = db.fech_dataframe(f"""
                    SELECT {BASE_COLS},
                        MIN(pr.stockActual) AS Estoc,
                        SUM(ISNULL(T1.cantidad, 0))       AS CantidadAct,
                        SUM(ISNULL(T1.importe, 0))        AS ImporteAct,
                        SUM(ISNULL(T1.cantidadcompra, 0)) AS CantidadCompraAct,
                        SUM(ISNULL(T1.importecompra, 0))  AS ImporteCompraAct
                    {BASE_FROM}
                    WHERE T1.anyomes >= {curr_yy}01 AND T1.anyomes <= {curr_yy}12
                        AND {ECO_FILTER}
                        AND pr.codLab IN (SELECT idLab FROM BifarmaCentral.dbo.labAcuerdos WHERE anyo = {curr_year})
                    {GROUP_BY}""")

                # ── Previous year ──
                ant_df = db.fech_dataframe(f"""
                    SELECT {BASE_COLS},
                        SUM(ISNULL(T1.cantidad, 0))       AS CantidadAnt,
                        SUM(ISNULL(T1.importe, 0))        AS ImporteAnt,
                        SUM(ISNULL(T1.cantidadcompra, 0)) AS CantidadCompraAnt,
                        SUM(ISNULL(T1.importecompra, 0))  AS ImporteCompraAnt
                    {BASE_FROM}
                    WHERE T1.anyomes >= {prev_yy}01 AND T1.anyomes <= {prev_yy}12
                        AND {ECO_FILTER}
                        AND pr.codLab IN (SELECT idLab FROM BifarmaCentral.dbo.labAcuerdos WHERE anyo = {prev_year})
                    {GROUP_BY}""")

            # ── Merge current + previous ──
            MERGE_KEYS = [
                'CodProducto', 'IdLaboratorio', 'IdEntidad',
                'IdDelegacion', 'IdProducto',
            ]
            products_df = pd.merge(act_df, ant_df, on=MERGE_KEYS, how='left', suffixes=('', '_ant'))

            for col in ['CantidadAnt', 'ImporteAnt', 'CantidadCompraAnt', 'ImporteCompraAnt']:
                if col not in products_df.columns:
                    products_df[col] = 0.0
                products_df[col] = products_df[col].fillna(0.0)

            products_df = products_df[['IdProducto', 'CodProducto','Producto', 'IdLaboratorio', 'Laboratorio']]

            print(f"Extracted {len(act_df)} current + {len(ant_df)} previous year rows -> {len(products_df)} merged")
            print("Extracted products data with columns:", products_df.columns.tolist())
            return products_df.to_dict('records')  # codProducto, idProducto, IdLaboratorio, 
        except Exception as e:
            print(f"Error extracting products: {e}")
            raise e  # Re-raise to mark the run as failed in Airflow

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
    def process_client(client_data: dict, all_products: list[dict], all_acords: list[dict]):
        """Full ETL pipeline for a single client. Mapped dynamically."""

        @task
        def map_acords(client_data: dict, all_acords: list[dict]) -> dict:
            """Map client vendors against the acordsEcos table."""
            import pandas as pd
            pd.set_option('display.max_columns', None)

            Vendor_Name = client_data["Vendor_Name"]
            acords_df   = pd.DataFrame(all_acords)

            # Step 1: find which Laboratori group this vendor belongs to via lab_description
            acords_df['lab_desc_lower'] = acords_df['lab_description'].str.strip().str.lower()
            match = acords_df[acords_df['lab_desc_lower'] == Vendor_Name.strip().lower()]

            if match.empty:
                print(f"[{Vendor_Name}] No match found in acords lab_description")
                return {"Vendor_Name": Vendor_Name, "laboratory_id": [], "mapped": []}

            # Step 2: collect ALL BIF_ids sharing the same Laboratori group (1:n)
            lab_groups = match['Laboratori'].unique().tolist()
            all_matched = acords_df[acords_df['Laboratori'].isin(lab_groups)].drop(columns=['lab_desc_lower'])
            bif_ids = all_matched['BIF_id'].unique().tolist()
            print(f"[{Vendor_Name}] -> group(s): {lab_groups} -> BIF_ids: {bif_ids}")
            return {
                "Vendor_Name": Vendor_Name,
                "laboratory_id": bif_ids,
                "mapped": all_matched.to_dict('records'),
            }

        @task
        def filter_products(mapped_result: dict, all_products: list[dict]) -> dict:
            """Filter the full products dataset to this client's labs."""
            import pandas as pd

            Vendor_Name  = mapped_result["Vendor_Name"]
            bif_ids      = mapped_result["laboratory_id"]
            products_df  = pd.DataFrame(all_products)
            print(f"Columns in products_df: {products_df.columns.tolist()}")
            print(f"Data in products_df:\n{products_df.head()}")
            client_products = products_df[products_df['IdLaboratorio'].str.strip().isin(bif_ids)]
            print(f"[{Vendor_Name}] Filtered {len(client_products)} product rows from {len(products_df)} total (BIF_ids: {bif_ids})")
            return {
                "Vendor_Name": Vendor_Name,
                "products": client_products.to_dict('records'),
                "mapped": mapped_result["mapped"],
            }

        @task
        def load(data: dict, client_data: dict) -> dict:
            """Load filtered data to CSV."""
            import pandas as pd
            import os

            Vendor_Name = data["Vendor_Name"]
            owners = client_data.get("owners", [])
            df = pd.DataFrame(data["products"])

            if df.empty:
                print(f"[{Vendor_Name}] No products to load")
                return {"Vendor_Name": Vendor_Name, "row_count": 0, "output_path": None, "owners": owners}

            safe_name = Vendor_Name.replace("'", "").replace(" ", "_").lower()
            output_path = f"/opt/airflow/dags/output/novedades_SKU_{safe_name}.csv"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)

            print(f"[{Vendor_Name}] Loaded {len(df)} rows to {output_path}")
            return {"Vendor_Name": Vendor_Name, "row_count": len(df), "output_path": output_path, "owners": owners}

        # Wire the per-client pipeline
        mapped   = map_acords(client_data, all_acords)
        filtered = filter_products(mapped, all_products)
        load(filtered, client_data)

    @task(trigger_rule="all_done")
    def notify_categories(**context) -> None:
        """Send one email per owner (in MAIL_RECIPIENTS) summarising their clients with new data."""
        from utils.clsZohoMailing import ZohoMailer

        ti = context["ti"]
        # Pull every load result produced by the mapped task group
        all_results = ti.xcom_pull(task_ids="process_client.load") or []
        if isinstance(all_results, dict):
            all_results = [all_results]

        # Keep only clients that actually produced rows
        results_with_data = [r for r in all_results if r and r.get("row_count", 0) > 0]
        if not results_with_data:
            print("No clients with new data — skipping notifications.")
            return

        # Group by owner email  →  {email: {"owner": {...}, "clients": [...]}}
        owner_map: dict = {}
        for r in results_with_data:
            for owner in r.get("owners", []):
                email = owner.get("email", "")
                if email not in MAIL_RECIPIENTS:
                    continue
                if email not in owner_map:
                    owner_map[email] = {"owner": owner, "clients": []}
                owner_map[email]["clients"].append(r)

        if not owner_map:
            print("No matching owners in MAIL_RECIPIENTS — skipping notifications.")
            return

        mailer = ZohoMailer()
        for email, data in owner_map.items():
            owner = data["owner"]
            clients = data["clients"]
            rows_html = "".join(
                f"<tr><td>{c['Vendor_Name']}</td><td>{c['row_count']}</td><td>{c['output_path']}</td></tr>"
                for c in clients
            )
            html_body = (
                f"<p>Hola {owner.get('name', '')}.</p>"
                f"<p>El proceso <b>Novedades SKU</b> ha finalizado con los siguientes resultados:</p>"
                f"<table border='1' cellpadding='4'>"
                f"<tr><th>Cliente</th><th>Filas</th><th>Archivo</th></tr>"
                f"{rows_html}"
                f"</table>"
            )
            subject = f"[Novedades SKU] {len(clients)} cliente(s) con nuevos datos"
            mailer.send(
                to=[{"address": "meslava@ecoceutics.com", "name": owner.get("name", "")}],
                subject=subject,
                html_body=html_body,
            )
            print(f"Notification sent to {email} for {len(clients)} client(s)")

    # ── Wire it all together ──────────────────────────────────
    clients = extract_vendors()
    category_manager = set()
    products = extract_products()
    acords = extract_acords()
    expanded = process_client.partial(all_products=products, all_acords=acords).expand(client_data=clients)
    expanded >> notify_categories()

# Instantiate the DAG
novedades_sku_pharma_etl()