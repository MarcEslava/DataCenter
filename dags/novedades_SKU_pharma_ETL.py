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

# ─────────────────────────────────────────────────────────────
# Configuration (from Airflow connections)
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID = "zoho_crm"
SQL_ACORDS_CONN_ID = "BIOps_db"     # SQL connection 1
SQL_PRODUCTS_CONN_ID = "BIFarma_db"  # SQL connection 2

# TODO: Adjust this to match the Zoho field that identifies the client/lab
CLIENT_KEY = "Vendor_Name"  # field in Zoho vendor data that holds client name


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────
#   test rama funcionar
@dag(
    dag_id='novedades_SKU_pharma_ETL',
    description='ETL for Pharma SKU updates from Zoho per client',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=3,  # limit parallelism to avoid overloading sources
    default_args={
        'owner': 'data-team',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
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

        print(f"Extracted {len(all_vendors)} vendors from Zoho")

        grouped = {}
        for v in all_vendors:
            client = v.get(CLIENT_KEY, 'unknown')
            grouped.setdefault(client, []).append(v)

        clients = [
            {"client_name": name, "vendors": vendors}
            for name, vendors in grouped.items()
        ]
        print(f"Split into {len(clients)} clients: {[c['client_name'] for c in clients]}")
        return clients

    # ── 2. Extract products (once for all clients) ──────────────
    @task
    def extract_products() -> list[dict]:
        """Extract products/sales for current and previous year. Runs ONCE."""
        import pandas as pd
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
        from utils.clsDate import DateHelper
        try:
            d = DateHelper()
            curr_yy = d.anyo_short
            prev_yy = d.offset(years=-1).anyo_short
            curr_year = d.anyo
            prev_year = d.offset(years=-1).anyo

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

            hook = MsSqlHook(mssql_conn_id=SQL_PRODUCTS_CONN_ID)

            # ── Current year ──
            act_df = hook.get_pandas_df(f"""
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
            ant_df = hook.get_pandas_df(f"""
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

            drop_cols = [c for c in products_df.columns if c.endswith('_ant')]
            products_df = products_df.drop(columns=drop_cols)

            print(f"Extracted {len(act_df)} current + {len(ant_df)} previous year rows -> {len(products_df)} merged")
            return products_df.to_dict('records')
        except Exception as e:
            print(f"Error extracting products: {e}")
            raise e  # Re-raise to mark the run as failed in Airflow

    # ── 3. Per-client pipeline (runs in parallel) ─────────────
    @task_group(group_id="process_client")
    def process_client(client_data: dict, all_products: list[dict]):
        """Full ETL pipeline for a single client. Mapped dynamically."""

        @task
        def map_acords(client_data: dict) -> dict:
            
            """Map client vendors with acordsEcos table."""
            import pandas as pd
            from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

            client_name = client_data["client_name"]
            vendors_df = pd.json_normalize(client_data["vendors"])

            # TODO [DATA-46]: Adjust join key to match your schema
            hook = MsSqlHook(mssql_conn_id=SQL_ACORDS_CONN_ID)
            acords_df = hook.get_pandas_df("SELECT * FROM acordsEcos")

            mapped = pd.merge(vendors_df, acords_df, on='key_column', how='inner')
            print(f"[{client_name}] Mapped {len(mapped)} vendors with acords")
            return {
                "client_name": client_name,
                "mapped": mapped.to_dict('records'),
            }

        @task
        def filter_products(mapped_result: dict, all_products: list[dict]) -> dict:
            """Filter the full products dataset to this client's labs."""
            import pandas as pd

            client_name = mapped_result["client_name"]
            mapped_df = pd.DataFrame(mapped_result["mapped"])
            products_df = pd.DataFrame(all_products)

            if mapped_df.empty or products_df.empty:
                print(f"[{client_name}] No data to filter")
                return {"client_name": client_name, "products": [], "mapped": []}

            # TODO [DATA-47]: Adjust filter — match labs from mapped vendors to products
            # lab_ids = mapped_df['codLab'].unique().tolist()
            # client_products = products_df[products_df['IdLaboratorio'].isin(lab_ids)]
            client_products = products_df  # placeholder — filter by lab above

            print(f"[{client_name}] Filtered {len(client_products)} product rows from {len(products_df)} total")
            return {
                "client_name": client_name,
                "products": client_products.to_dict('records'),
                "mapped": mapped_result["mapped"],
            }

        @task
        def transform(data: dict) -> dict:
            """Transform and enrich extracted data."""
            import pandas as pd

            client_name = data["client_name"]
            products_df = pd.DataFrame(data["products"]) if data["products"] else pd.DataFrame()
            mapped_df = pd.DataFrame(data["mapped"]) if data["mapped"] else pd.DataFrame()

            if products_df.empty or mapped_df.empty:
                print(f"[{client_name}] No data to transform")
                return {"client_name": client_name, "rows": []}

            # TODO [DATA-48]: Adjust merge key
            df = pd.merge(products_df, mapped_df, on='key_column', how='left')

            # ── Clean ──
            df = df.drop_duplicates()
            df = df.dropna(subset=['key_column'])

            # ── Rename columns ──
            # TODO [DATA-49]: Adjust column mapping
            # column_mapping = {'source_col': 'Target Name'}
            # df = df.rename(columns=column_mapping)

            # ── Derived columns ──
            # TODO [DATA-50]: Add calculated fields
            # df['margin'] = df['sell_price'] - df['buy_price']

            # ── Final column selection ──
            # TODO [DATA-51]: Adjust
            # final_columns = ['col_a', 'col_b', 'col_c']
            # df = df[[c for c in final_columns if c in df.columns]]

            print(f"[{client_name}] Transformed {len(df)} rows")
            return {"client_name": client_name, "rows": df.to_dict('records')}

        @task
        def load(data: dict):
            """Load transformed data to destination."""
            import pandas as pd
            import os

            client_name = data["client_name"]
            rows = data["rows"]
            if not rows:
                print(f"[{client_name}] No data to load")
                return

            df = pd.DataFrame(rows)

            # ── Option A: Write to SQL ──
            # TODO [DATA-52]: Uncomment and adjust
            # from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
            # hook = MsSqlHook(mssql_conn_id=SQL_PRODUCTS_CONN_ID)
            # engine = hook.get_sqlalchemy_engine()
            # df.to_sql(f'novedades_{safe_name}', con=engine, if_exists='replace', index=False)

            # ── Option B: Write to CSV (one per client) ──
            safe_name = client_name.replace("'", "").replace(" ", "_").lower()
            output_path = f"/opt/airflow/dags/output/novedades_SKU_{safe_name}.csv"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)

            print(f"[{client_name}] Loaded {len(df)} rows to {output_path}")

        # Wire the per-client pipeline
        mapped = map_acords(client_data)
        filtered = filter_products(mapped, all_products)
        transformed = transform(filtered)
        load(transformed)

    # ── Wire it all together ──────────────────────────────────
    clients = extract_vendors()
    products = extract_products()
    process_client.partial(all_products=products).expand(client_data=clients)


# Instantiate the DAG
novedades_sku_pharma_etl()
