"""
EcoVital Orders ETL Pipeline

Extracts orders from LogiCommerce, enriches with customer data from Ecoceutics,
looks up alliance client IDs via SSH/DB, and produces a fact table for pharmacy
order analysis. Uploads result to FTP.
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
from utils.clsSSHTunnel import SSHTunnel
from utils.clsSQL import SQLConnection


# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

LOGICOMMERCE_API_BASE = "https://api.logicommerce.net/v1"
LOGICOMMERCE_APP_ID = "pK3c76MsxY"
LOGICOMMERCE_SECRET = "pK3c76MsxY73eS9F2ke9gAvfBb2x84"

ECOCEUTICS_API_BASE = "https://apifidfarma.ecoceutics.com/v1"
ECOCEUTICS_API_KEY = "657A8288P7156"

API_RATE_LIMIT_DELAY = 0.3
OUTPUT_PATH = "/opt/airflow/dags/output/EcoVital_FactTable.csv"

FTP_CONN_ID = "alloga_ftp"
FTP_REMOTE_PATH = "fichero/EcoVital_FactTable.csv"

SSH_HOST = "cecobd1.ecoceutics.com"
SSH_USER = "U4NsrvTwqF"
SSH_PASSWORD = ""
SSH_PORT = 12984
SSH_KEY = "/root/.ssh/id_ed25519"
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "wED2iQTl"
DB_PASSWORD = "BS0jIbTe"
DB_NAME = "fidfarma"

TAX_MAPPING = {
    "1": 21,
    "2": 10,
    "3": 4,
}

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
# Utility Functions
# ─────────────────────────────────────────────────────────────

def create_sha256_token(secret: str) -> str:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("utf-8")


def build_logicommerce_headers(token: str) -> dict:
    return {
        "Accept": "application/json",
        "Authorization": f"Basic {token}",
        "countryCode": "ES",
        "appid": LOGICOMMERCE_APP_ID
    }


def build_ecoceutics_headers() -> dict:
    return {"Accept": "application/json", "countryCode": "ES"}


def extract_tax_value(taxes) -> float:
    if isinstance(taxes, list) and taxes and isinstance(taxes[0], dict):
        tax_id = str(taxes[0].get("TAX", {}).get("ID", ""))
        return TAX_MAPPING.get(tax_id, 0)
    return 0


def extract_re_value(taxes) -> float:
    if isinstance(taxes, list) and taxes and isinstance(taxes[0], dict):
        return taxes[0].get("RERATE", 0)
    return 0


def extract_discount_value(discounts: list) -> float:
    if isinstance(discounts, list) and discounts and isinstance(discounts[0], dict):
        return discounts[0].get("DISCOUNTVALUE", 0)
    return 0


def parse_json_cell(value) -> dict | list | None:
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
# API Functions
# ─────────────────────────────────────────────────────────────

def fetch_logicommerce_paginated(token: str, endpoint: str, key: str) -> dict:
    headers = build_logicommerce_headers(token)
    url = f"{LOGICOMMERCE_API_BASE}/{endpoint}"
    all_items = []
    page = 1

    while True:
        response = requests.get(url, headers=headers, params={"page": page})
        response.raise_for_status()
        data = response.json()
        items = data.get(key, [])
        all_items.extend(items)
        total = data.get("ITEMS", 0)
        pager = data.get("PAGERPARAMETERS", {})
        per_page = pager.get("PERPAGE", "?")
        pages_total = (total + int(per_page) - 1) // int(per_page) if str(per_page).isdigit() and int(per_page) > 0 else "?"
        print(f"  page {page}/{pages_total}: +{len(items)} rows  [{len(all_items)}/{total} fetched]")
        if len(all_items) >= total:
            break
        page += 1
        sleep(API_RATE_LIMIT_DELAY)

    data[key] = all_items
    return data


def fetch_logicommerce_orders(token: str) -> dict:
    return fetch_logicommerce_paginated(token, "orders", "ORDERS")


def fetch_logicommerce_order_detail(token: str, order_number: str) -> dict:
    url = f"{LOGICOMMERCE_API_BASE}/orders/{order_number}"
    response = requests.get(url, headers=build_logicommerce_headers(token))
    response.raise_for_status()
    return response.json()


def fetch_logicommerce_users(token: str) -> dict:
    return fetch_logicommerce_paginated(token, "users", "USERS")


def fetch_ecoceutics_fid(nif: str) -> dict:
    url = f"{ECOCEUTICS_API_BASE}/unit/{nif}/fid/?api_key={ECOCEUTICS_API_KEY}"
    response = requests.get(url, headers=build_ecoceutics_headers())
    response.raise_for_status()
    return response.json()


# ─────────────────────────────────────────────────────────────
# DB / SSH
# ─────────────────────────────────────────────────────────────

def make_tunnel():
    key_content = open(SSH_KEY).read() if SSH_KEY else None
    return SSHTunnel(
        ssh_host=SSH_HOST, ssh_port=SSH_PORT, ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD or None, ssh_private_key=key_content,
        remote_host=DB_HOST, remote_port=DB_PORT,
    )


def make_db(tunnel):
    return SQLConnection(
        db_host=DB_HOST, db_port=DB_PORT, db_database=DB_NAME,
        db_username=DB_USER, db_password=DB_PASSWORD,
        dialect="mysql", driver="pymysql",
        ssh_tunnel=tunnel,
    )


def query_units_by_nifs(nif_list: list) -> pd.DataFrame:
    if not nif_list:
        return pd.DataFrame(columns=['id', 'nif', 'id_unit_izaro'])
    with make_tunnel() as tunnel, make_db(tunnel) as db:
        df = db.get_table_info(
            table_name='Unit',
            cols='id, nif, id_unit_izaro',
            where=f"nif IN ({', '.join(repr(n) for n in nif_list)})",
        )
        return df if not df.empty else pd.DataFrame(columns=['id', 'nif', 'id_unit_izaro'])


# ─────────────────────────────────────────────────────────────
# Transform Functions
# ─────────────────────────────────────────────────────────────

def extract_order_numbers(orders_data: dict) -> list:
    df = pd.json_normalize(orders_data.get("ORDERS", []))
    return df['ID'].tolist()


def normalize_order_detail(order_data: dict) -> pd.DataFrame:
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
    df["RE"] = df["TAXES"].apply(extract_re_value)
    df["TAXES"] = df["TAXES"].apply(extract_tax_value)
    return df[["PEDIDO", "DATE", "SKU", "QUANTITY", "PRICE", "DISCOUNTVALUE", "NIF", "TAXES", "RE"]]


def extract_unique_nifs(pedidos_df: pd.DataFrame) -> list:
    return pedidos_df['NIF'].drop_duplicates().tolist()


def normalize_billing_addresses(users_data: dict) -> pd.DataFrame:
    users = pd.json_normalize(users_data.get("USERS", []))
    col = "ADDRESSBOOK.BILLINGADDRESS"
    if col not in users.columns:
        return pd.DataFrame()
    parsed = users[col].apply(parse_json_cell)
    normalized = [normalize_billing_item(item) for item in parsed]
    return pd.json_normalize(normalized)


def extract_fid_from_response(nif_data: list) -> pd.DataFrame:
    df = pd.DataFrame(nif_data, columns=['raw', 'nif'])
    df['id'] = df['raw'].apply(
        lambda x: x[0].get('id') if isinstance(x, list) and x else None
    )
    return df[['id', 'nif']].drop_duplicates(['id'])


def merge_orders_with_nif(pedidos_df: pd.DataFrame, nif_df: pd.DataFrame) -> pd.DataFrame:
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
    return orders_df.merge(billing_df, left_on='NIF', right_on='NIF', how='left')


def apply_final_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
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
        "TAXES": "Iva",
    }
    df = df.rename(columns=mapping)
    if 'Precio' in df.columns:
        df['Precio'] = df['Precio'].round(2)

    final_columns = [
        "F.Pedido", "Pedido", "Farmacia", "Direcion", "Codigo Postal",
        "Poblacion", "Provincia", "Codigo Producto", "C.Pedida",
        "Precio", "Descuento", "Iva", "RE", "CustomerCifId", "Cliente Alliance",
    ]
    return df[[c for c in final_columns if c in df.columns]]


# ─────────────────────────────────────────────────────────────
# Airflow Task Functions
# ─────────────────────────────────────────────────────────────

def task_generate_token(**context):
    return create_sha256_token(LOGICOMMERCE_SECRET)


def task_extract_orders(**context):
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    return fetch_logicommerce_orders(token)


def task_extract_users(**context):
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    return fetch_logicommerce_users(token)


def task_transform_orders(**context):
    orders_data = context['task_instance'].xcom_pull(task_ids='extract_orders')
    return extract_order_numbers(orders_data)


def task_extract_order_details(**context):
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    order_numbers = context['task_instance'].xcom_pull(task_ids='transform_orders')

    all_details = []
    for order_num in order_numbers:
        try:
            order_data = fetch_logicommerce_order_detail(token, order_num)
            detail_df = normalize_order_detail(order_data)
            all_details.append(detail_df)
            print(f"  Order {order_num}: {len(detail_df)} rows")
        except requests.HTTPError as e:
            print(f"  Order {order_num}: SKIP ({e.response.status_code} - {e.response.json().get('msg', '')})")
        sleep(API_RATE_LIMIT_DELAY)

    if not all_details:
        return []
    results = pd.concat(all_details, ignore_index=True)
    print(f"Total: {len(results)} order detail rows")
    return results.to_dict('records')


def task_extract_nif_data(**context):
    pedidos = context['task_instance'].xcom_pull(task_ids='extract_order_details')
    if not pedidos:
        return []
    pedidos_df = pd.DataFrame(pedidos)
    nif_list = extract_unique_nifs(pedidos_df)

    results = []
    for nif in nif_list:
        try:
            fid_data = fetch_ecoceutics_fid(nif)
            results.append([fid_data, nif])
            print(f"  NIF {nif}: OK")
        except requests.HTTPError as e:
            print(f"  NIF {nif}: SKIP ({e.response.status_code})")
        sleep(API_RATE_LIMIT_DELAY)

    print(f"Fetched FID for {len(results)} customers")
    return results


def task_transform_users(**context):
    users_data = context['task_instance'].xcom_pull(task_ids='extract_users')
    billing_df = normalize_billing_addresses(users_data)
    print(f"Normalized {len(billing_df)} billing addresses")
    return billing_df.to_dict('records')


def task_alliance_clients(**context):
    pedidos = context['task_instance'].xcom_pull(task_ids='extract_order_details')
    if not pedidos:
        print("No orders to query")
        return []
    if not SSH_HOST:
        print("SKIP - SSH_HOST not configured")
        return []

    pedidos_df = pd.DataFrame(pedidos)
    nif_list = extract_unique_nifs(pedidos_df)
    print(f"Querying Unit table for {len(nif_list)} NIFs...")
    units_df = query_units_by_nifs(nif_list)
    print(f"Found {len(units_df)} matches")
    if units_df.empty:
        return []
    return units_df[['nif', 'id_unit_izaro']].to_dict('records')


def task_load_fact_table(**context):
    ti = context['task_instance']

    pedidos = ti.xcom_pull(task_ids='extract_order_details')
    nif_data = ti.xcom_pull(task_ids='extract_nif_data')
    billing_data = ti.xcom_pull(task_ids='transform_users')
    alliance_data = ti.xcom_pull(task_ids='alliance_clients')

    if not pedidos:
        print("No data to merge")
        return None

    pedidos_df = pd.DataFrame(pedidos)
    billing_df = pd.DataFrame(billing_data) if billing_data else pd.DataFrame()
    nif_df = extract_fid_from_response(nif_data) if nif_data else pd.DataFrame(columns=['id', 'nif'])
    alliance_df = pd.DataFrame(alliance_data) if alliance_data else pd.DataFrame(columns=['nif', 'id_unit_izaro'])

    df = merge_orders_with_nif(pedidos_df, nif_df)
    df = merge_with_billing(df, billing_df)

    if not alliance_df.empty:
        df = df.merge(
            alliance_df.rename(columns={'id_unit_izaro': 'Cliente Alliance'}),
            left_on='NIF', right_on='nif', how='left'
        )
        df = df.drop(columns=['nif'], errors='ignore')

    df = apply_final_column_mapping(df)

    import os
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_PATH}")
    return OUTPUT_PATH


def task_upload_to_ftp(**context):
    output_path = context['task_instance'].xcom_pull(task_ids='load_fact_table')
    if not output_path:
        print("No file to upload")
        return
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

alliance_clients_task = PythonOperator(
    task_id='alliance_clients',
    python_callable=task_alliance_clients,
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
#                    ┌─► extract_orders ─► transform_orders ─► extract_order_details ─► extract_nif ──────┐
# generate_token ───►│                                              │                                      ├─► load_fact_table ─► upload_to_ftp
#                    └─► extract_users ─► transform_users ──────────┼─► alliance_clients ─────────────────┘
#

generate_token_task >> [extract_orders_task, extract_users_task]

extract_orders_task >> transform_orders_task >> extract_order_details_task >> extract_nif_task
extract_users_task >> transform_users_task
extract_order_details_task >> alliance_clients_task

[extract_nif_task, transform_users_task, alliance_clients_task] >> load_task >> upload_ftp_task
