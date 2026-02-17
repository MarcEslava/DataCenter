"""
Novedades SKU Pharma ETL

Extracts vendors from Zoho CRM, maps them against acordsEcos (SQL conn 1),
then pulls products/sales/purchases from SQL conn 2 for matched labs.

Runs the full pipeline IN PARALLEL for each client (L'Oreal, Haleon, Almirall, etc.).
Uses Airflow dynamic task mapping: extract_vendors produces a list of clients,
and process_client.expand() fans out one parallel branch per client.
"""

import os
from airflow.decorators import dag, task, task_group
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd  # noqa: F401 — used inside @task functions
from utils.clsSQL import SQLConnection
from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector


# ─────────────────────────────────────────────────────────────
# Configuration (from Airflow connections)
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID = "zoho_crm"
SQL_ACORDS_CONN_ID = "novedades_acords_db"     # SQL connection 1
SQL_PRODUCTS_CONN_ID = "novedades_products_db"  # SQL connection 2

# TODO: Adjust this to match the Zoho field that identifies the client/lab
CLIENT_KEY = "Vendor_Name"  # field in Zoho vendor data that holds client name


def _make_sql(conn_id):
    """Build a SQLConnection from an Airflow connection."""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return SQLConnection(
        db_host=conn.host,
        db_port=conn.port,
        db_database=conn.schema,
        db_username=conn.login,
        db_password=conn.password,
        dialect=extra.get("dialect", "mssql"),
        driver=extra.get("driver", "pyodbc"),
    )


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='novedades_SKU_pharma_ETL',
    description='ETL for Pharma SKU updates from Zoho — parallel per client',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)
def novedades_sku_pharma_etl():

    # ── 1. Extract all vendors and split by client ────────────
    @task
    def extract_vendors() -> list[dict]:
        """Extract vendors from Zoho and return one dict per client group."""
        from time import sleep

        # Authenticate — credentials from Airflow connection 'zoho_crm'
        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        # Fetch all vendors (paginated)
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

        # Group vendors by client name
        grouped = {}
        for v in all_vendors:
            client = v.get(CLIENT_KEY, 'unknown')
            grouped.setdefault(client, []).append(v)

        # Return one dict per client — this drives dynamic mapping
        clients = [
            {"client_name": name, "vendors": vendors}
            for name, vendors in grouped.items()
        ]
        print(f"Split into {len(clients)} clients: {[c['client_name'] for c in clients]}")
        return clients

    # ── 2. Per-client pipeline (runs in parallel) ─────────────
    @task_group(group_id="process_client")
    def process_client(client_data: dict):
        """Full ETL pipeline for a single client. Mapped dynamically."""

        @task
        def map_acords(client_data: dict) -> dict:
            """Map client vendors with acordsEcos table."""
            client_name = client_data["client_name"]
            vendors_df = pd.json_normalize(client_data["vendors"])

            # TODO [DATA-46]: Adjust join key to match your schema
            with _make_sql(SQL_ACORDS_CONN_ID) as db:
                acords_df = db.get_table_info(table_name='acordsEcos')

            mapped = pd.merge(vendors_df, acords_df, on='key_column', how='inner')
            print(f"[{client_name}] Mapped {len(mapped)} vendors with acords")
            return {
                "client_name": client_name,
                "mapped": mapped.to_dict('records'),
            }

        @task
        def extract_products(mapped_result: dict) -> dict:
            """Extract products/sales/purchases for this client's labs."""
            client_name = mapped_result["client_name"]
            mapped_df = pd.DataFrame(mapped_result["mapped"])

            if mapped_df.empty:
                print(f"[{client_name}] No mapped vendors — skipping product extraction")
                return {"client_name": client_name, "products": [], "mapped": []}

            # TODO [DATA-47]: Adjust query — filter by lab_id from mapped vendors
            # lab_ids = mapped_df['lab_id'].unique().tolist()
            # where_clause = f"p.lab_id IN ({', '.join(repr(x) for x in lab_ids)})"
            with _make_sql(SQL_PRODUCTS_CONN_ID) as db:
                products_df = db.get_table_info(
                    table_name='product p JOIN sell s ON p.id = s.product_id JOIN buy b ON p.id = b.product_id',
                    cols='p.*, s.sell_qty, s.sell_price, b.buy_qty, b.buy_price',
                    # where=where_clause,
                )

            print(f"[{client_name}] Extracted {len(products_df)} product rows")
            return {
                "client_name": client_name,
                "products": products_df.to_dict('records'),
                "mapped": mapped_result["mapped"],
            }

        @task
        def transform(data: dict) -> dict:
            """Transform and enrich extracted data."""
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
            client_name = data["client_name"]
            rows = data["rows"]
            if not rows:
                print(f"[{client_name}] No data to load")
                return

            df = pd.DataFrame(rows)

            # ── Option A: Write to SQL ──
            # TODO [DATA-52]: Uncomment and adjust
            # with _make_sql(SQL_PRODUCTS_CONN_ID) as db:
            #     db.bulk_upload_to_staging(df, table_name=f'novedades_{client_name}')
            #     db.upsert_products_from_staging()

            # ── Option B: Write to CSV (one per client) ──
            import os
            safe_name = client_name.replace("'", "").replace(" ", "_").lower()
            output_path = f"/opt/airflow/dags/output/novedades_SKU_{safe_name}.csv"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)

            print(f"[{client_name}] Loaded {len(df)} rows to {output_path}")

        # Wire the per-client pipeline
        mapped = map_acords(client_data)
        products = extract_products(mapped)
        transformed = transform(products)
        load(transformed)

    # ── Wire it all together ──────────────────────────────────
    clients = extract_vendors()
    process_client.expand(client_data=clients)


# Instantiate the DAG
novedades_sku_pharma_etl()
