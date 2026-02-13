"""
EcoVital Orders ETL Pipeline

Extracts orders from LogiCommerce, enriches with customer data from Ecoceutics,
and produces a fact table for pharmacy order analysis.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import hashlib
import base64
import requests
import pandas as pd
from time import sleep
import json
import ast
from utils.ftp import FTPConn

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

LOGICOMMERCE_API_BASE = "https://api.logicommerce.net/v1"
LOGICOMMERCE_APP_ID = "pK3c76MsxY"
LOGICOMMERCE_SECRET = "pK3c76MsxY73eS9F2ke9gAvfBb2x84"

ECOCEUTICS_API_BASE = "https://apifidfarma.ecoceutics.com/v1"
ECOCEUTICS_API_KEY = "657A8288P7156"

API_RATE_LIMIT_DELAY = 0.3
OUTPUT_PATH = "/opt/airflow/dags/output/EcoVital_FactTable.xlsx"

FTP_CONN_ID = "alloga_ftp"
FTP_REMOTE_PATH = "/EcoVital_FactTable.csv"

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ecovital_etl',
    default_args=default_args,
    description='EcoVital Orders ETL Pipeline',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
)


# ─────────────────────────────────────────────────────────────
# Utility Functions (Pure - No Side Effects)
# ─────────────────────────────────────────────────────────────

def create_sha256_token(secret: str) -> str:
    """Create Base64-encoded SHA256 hash from secret."""
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("utf-8")


def build_logicommerce_headers(token: str) -> dict:
    """Build headers for LogiCommerce API requests."""
    return {
        "Accept": "application/json",
        "Authorization": f"Basic {token}",
        "countryCode": "ES",
        "appid": LOGICOMMERCE_APP_ID
    }


def build_ecoceutics_headers() -> dict:
    """Build headers for Ecoceutics API requests."""
    return {
        "Accept": "application/json",
        "countryCode": "ES",
    }


def extract_discount_value(discounts: list) -> float:
    """Extract discount value from DISCOUNTS array."""
    if isinstance(discounts, list) and discounts and isinstance(discounts[0], dict):
        return discounts[0].get("DISCOUNTVALUE", 0)
    return 0


def parse_json_cell(value) -> dict | list | None:
    """Parse a cell that may contain JSON string, dict, or list."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        try:
            return ast.literal_eval(str(value))
        except Exception:
            return None


def normalize_billing_item(item) -> dict:
    """Normalize a single billing address item to dict."""
    if isinstance(item, list):
        if len(item) == 0:
            return {}
        if isinstance(item[0], dict):
            return item[0]
        return {"value": item}
    if isinstance(item, dict):
        return item
    return {}


# ─────────────────────────────────────────────────────────────
# API Functions (Single Responsibility: Make HTTP Request)
# ─────────────────────────────────────────────────────────────

def fetch_logicommerce_orders(token: str) -> dict:
    """Fetch orders list from LogiCommerce API."""
    url = f"{LOGICOMMERCE_API_BASE}/orders"
    response = requests.get(url, headers=build_logicommerce_headers(token))
    response.raise_for_status()
    return response.json()


def fetch_logicommerce_order_detail(token: str, order_number: str) -> dict:
    """Fetch single order detail from LogiCommerce API."""
    url = f"{LOGICOMMERCE_API_BASE}/orders/{order_number}"
    response = requests.get(url, headers=build_logicommerce_headers(token))
    response.raise_for_status()
    return response.json()


def fetch_logicommerce_users(token: str) -> dict:
    """Fetch users list from LogiCommerce API."""
    url = f"{LOGICOMMERCE_API_BASE}/users"
    response = requests.get(url, headers=build_logicommerce_headers(token))
    response.raise_for_status()
    return response.json()


def fetch_ecoceutics_fid(nif: str) -> dict:
    """Fetch FID data for a NIF from Ecoceutics API."""
    url = f"{ECOCEUTICS_API_BASE}/unit/{nif}/fid/?api_key={ECOCEUTICS_API_KEY}"
    response = requests.get(url, headers=build_ecoceutics_headers())
    response.raise_for_status()
    return response.json()


# ─────────────────────────────────────────────────────────────
# Transform Functions (Single Responsibility: Transform Data)
# ─────────────────────────────────────────────────────────────

def extract_order_numbers(orders_data: dict) -> list:
    """Extract document numbers from orders response."""
    df = pd.json_normalize(orders_data.get("ORDERS", []))
    return df['ID'].tolist()


def normalize_order_detail(order_data: dict) -> pd.DataFrame:
    """Normalize single order detail into DataFrame rows."""
    df = pd.json_normalize(
        order_data,
        record_path=["ORDERS", "DETAILS"],
        meta=[
            ["ORDERS", "DATE"],
            ["ORDERS", "ORDERID"],
            ["ORDERS", "ORDERUSERS", "NIF"]
        ],
        errors="ignore"
    )
    df = df.rename(columns={
        "ORDERS.ORDERID": "PEDIDO",
        "ORDERS.DATE": "DATE",
        "ORDERS.ORDERUSERS.NIF": "NIF"
    })
    df["DISCOUNTVALUE"] = df["DISCOUNTS"].apply(extract_discount_value)
    return df[["PEDIDO", "DATE", "SKU", "QUANTITY", "PRICE", "DISCOUNTVALUE", "NIF", "TAXES"]]


def extract_unique_nifs(pedidos: list) -> list:
    """Extract unique NIF values from order details."""
    df = pd.DataFrame(pedidos)
    return df['NIF'].drop_duplicates().tolist()


def normalize_billing_addresses(users_data: dict) -> pd.DataFrame:
    """Extract and normalize billing addresses from users data."""
    users = pd.json_normalize(users_data.get("USERS", []))

    col = "ADDRESSBOOK.BILLINGADDRESS"
    if col not in users.columns:
        return pd.DataFrame()

    parsed = users[col].apply(parse_json_cell)
    normalized = [normalize_billing_item(item) for item in parsed]
    return pd.json_normalize(normalized)


def extract_fid_from_response(nif_data: list) -> pd.DataFrame:
    """Extract FID from API responses."""
    df = pd.DataFrame(nif_data, columns=['raw', 'nif'])
    df['id'] = df['raw'].apply(
        lambda x: x[0].get('id') if isinstance(x, list) and x else None
    )
    return df[['id', 'nif']].drop_duplicates(['id'])


def merge_orders_with_nif(pedidos_df: pd.DataFrame, nif_df: pd.DataFrame) -> pd.DataFrame:
    """Merge order details with NIF/FID mapping."""
    df = nif_df.merge(pedidos_df, left_on='nif', right_on='NIF', how='right')
    df = df.drop(columns=['nif'])
    return df.rename(columns={
        "id": "NIF2",
        "SKU": "PRODUCTO",
        "PRICE": "PRECIO",
        "QUANTITY": "UNIDADES",
        "DISCOUNTVALUE": "DTO",
        "DATE": "FECHA",
    })


def merge_with_billing(orders_df: pd.DataFrame, billing_df: pd.DataFrame) -> pd.DataFrame:
    """Merge orders with billing address data."""
    return orders_df.merge(billing_df, left_on='NIF', right_on='NIF', how='left')


def apply_final_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Apply final column renaming for output."""
    mapping = {
        "PEDIDO": "Pedido",
        "FECHA": "F.Pedido",
        "COMPANY": "Farmacia",
        "ADDRESS": "Direcion",
        "CITY": "Poblacion",
        "ZIP": "Codigo Postal",
        "STATE": "Provincia",
        "PRODUCTO": "Codigo Producto",
        "UNIDADES": "C.Pedida",
        "PRECIO": "Precio",
        "DTO": "Descuento",
        "NIF": "CustomerCifId",
        "TAXES": "TAXES"
    }
    df = df.rename(columns=mapping)
    if 'Precio' in df.columns:
        df['Precio'] = df['Precio'].round(2)
    return df


def save_to_excel(df: pd.DataFrame, path: str) -> str:
    """Save DataFrame to Excel file."""
    df.to_excel(path, index=False, sheet_name="in")
    return path

def save_to_csv(df: pd.DataFrame, path: str) -> str:
    """Save DataFrame to CSV file."""
    df.to_csv(path, index=False)
    return path


# ─────────────────────────────────────────────────────────────
# Airflow Task Functions (Orchestration Layer)
# ─────────────────────────────────────────────────────────────

def task_generate_token(**context):
    """Task: Generate authentication token."""
    return create_sha256_token(LOGICOMMERCE_SECRET)


def task_extract_orders(**context):
    """Task: Fetch orders from LogiCommerce."""
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    return fetch_logicommerce_orders(token)


def task_extract_users(**context):
    """Task: Fetch users from LogiCommerce."""
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    return fetch_logicommerce_users(token)


def task_transform_orders(**context):
    """Task: Extract order numbers from orders data."""
    orders_data = context['task_instance'].xcom_pull(task_ids='extract_orders')
    return extract_order_numbers(orders_data)


def task_extract_order_details(**context):
    """Task: Fetch and normalize details for each order."""
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    order_numbers = context['task_instance'].xcom_pull(task_ids='transform_orders')

    all_details = []
    for order_num in order_numbers:
        print(f"Processing order: {order_num}")
        order_data = fetch_logicommerce_order_detail(token, order_num)
        detail_df = normalize_order_detail(order_data)
        all_details.append(detail_df)
        sleep(API_RATE_LIMIT_DELAY)

    results = pd.concat(all_details, ignore_index=True)
    print(f"Fetched {len(results)} order detail rows")
    return results.to_dict('records')


def task_extract_nif_data(**context):
    """Task: Fetch FID for each unique NIF."""
    pedidos = context['task_instance'].xcom_pull(task_ids='extract_order_details')
    nif_list = extract_unique_nifs(pedidos)

    results = []
    for nif in nif_list:
        print(f"Processing NIF: {nif}")
        fid_data = fetch_ecoceutics_fid(nif)
        results.append([fid_data, nif])
        sleep(API_RATE_LIMIT_DELAY)

    print(f"Fetched FID for {len(results)} customers")
    return results


def task_transform_users(**context):
    """Task: Extract billing addresses from users."""
    users_data = context['task_instance'].xcom_pull(task_ids='extract_users')
    billing_df = normalize_billing_addresses(users_data)
    print(f"Normalized {len(billing_df)} billing addresses")
    return billing_df.to_dict('records')


def task_load_fact_table(**context):
    """Task: Merge all data and save fact table."""
    ti = context['task_instance']

    # Pull data from upstream tasks
    pedidos = ti.xcom_pull(task_ids='extract_order_details')
    nif_data = ti.xcom_pull(task_ids='extract_nif_data')
    billing_data = ti.xcom_pull(task_ids='transform_users')

    # Convert to DataFrames
    pedidos_df = pd.DataFrame(pedidos)
    billing_df = pd.DataFrame(billing_data)
    nif_df = extract_fid_from_response(nif_data)

    # Merge pipeline
    df = merge_orders_with_nif(pedidos_df, nif_df)
    df = merge_with_billing(df, billing_df)
    df = apply_final_column_mapping(df)

    # Save output
    output_path = save_to_csv(df, OUTPUT_PATH)
    print(f"Saved {len(df)} rows to {output_path}")
    print(df.head())

    return output_path


def task_upload_to_ftp(**context):
    """Task: Upload CSV to FTP server."""
    output_path = context['task_instance'].xcom_pull(task_ids='load_fact_table')
    with FTPConn.from_airflow(FTP_CONN_ID) as ftp:
        ftp.upload(output_path, FTP_REMOTE_PATH)
    print(f"Uploaded {output_path} to FTP: {FTP_REMOTE_PATH}")


# ─────────────────────────────────────────────────────────────
# Task Definitions
# ─────────────────────────────────────────────────────────────

generate_token_task = PythonOperator(
    task_id='generate_token',
    python_callable=task_generate_token,
    dag=dag,
)

extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=task_extract_orders,
    dag=dag,
)

extract_users_task = PythonOperator(
    task_id='extract_users',
    python_callable=task_extract_users,
    dag=dag,
)

transform_orders_task = PythonOperator(
    task_id='transform_orders',
    python_callable=task_transform_orders,
    dag=dag,
)

extract_order_details_task = PythonOperator(
    task_id='extract_order_details',
    python_callable=task_extract_order_details,
    dag=dag,
)

extract_nif_task = PythonOperator(
    task_id='extract_nif_data',
    python_callable=task_extract_nif_data,
    dag=dag,
)

transform_users_task = PythonOperator(
    task_id='transform_users',
    python_callable=task_transform_users,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_fact_table',
    python_callable=task_load_fact_table,
    dag=dag,
)

upload_ftp_task = PythonOperator(
    task_id='upload_to_ftp',
    python_callable=task_upload_to_ftp,
    dag=dag,
)

# ─────────────────────────────────────────────────────────────
# Task Dependencies
# ─────────────────────────────────────────────────────────────
#
#                    ┌─► extract_orders ─► transform_orders ─► extract_order_details ─► extract_nif ─┐
# generate_token ───►│                                                                                ├─► load_fact_table
#                    └─► extract_users ─► transform_users ───────────────────────────────────────────┘
#

generate_token_task >> [extract_orders_task, extract_users_task]

extract_orders_task >> transform_orders_task >> extract_order_details_task >> extract_nif_task
extract_users_task >> transform_users_task

[extract_nif_task, transform_users_task] >> load_task >> upload_ftp_task
