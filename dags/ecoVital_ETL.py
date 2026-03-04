"""
EcoVital Orders ETL Pipeline

Extracts orders from LogiCommerce, enriches with customer data from Ecoceutics,
looks up alliance client IDs via SSH/DB, and produces a fact table for pharmacy
order analysis. Uploads result to FTP.
"""

from airflow import DAG
from airflow.models import Variable
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

# Variables (to be set in Airflow UI or environment)
LOGICOMMERCE_API_BASE = Variable.get("logicommerce_api_base")
LOGICOMMERCE_APP_ID = Variable.get("logicommerce_app_id")
LOGICOMMERCE_SECRET = Variable.get("logicommerce_secret")  

ECOCEUTICS_API_BASE = Variable.get("ecoceutics_api_base")
ECOCEUTICS_API_KEY = Variable.get("ecoceutics_api_key")

API_RATE_LIMIT_DELAY = 0.3

# Connections
FTP_CONN_ID = "aqua_ftp"
FTP_REMOTE_PATH = Variable.get("ftp_remote_path", default_var="/")

SSH_CONN_ID = "ssh_tunnel"
DB_CONN_ID = "fidfarma_db"

TAX_MAPPING = Variable.get("ecovital_tax_mapping", deserialize_json=True, default_var={})

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ecovital_etl',
    default_args=default_args,
    description='EcoVital Orders ETL Pipeline',
    schedule=Variable.get("ecovital_schedule", default_var="30 11 * * *"),
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
            raise


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
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(SSH_CONN_ID)
        extra = conn.extra_dejson
        key_file = extra.get('key_file')
        key_content = open(key_file).read() if key_file else None
        return SSHTunnel(
            ssh_host=conn.host,
            ssh_port=conn.port or 22,
            ssh_username=conn.login,
            ssh_password=conn.password or None,
            ssh_private_key=key_content,
            remote_host=extra.get('remote_host', '127.0.0.1'),
            remote_port=int(extra.get('remote_port', 3306)),
        )
    except Exception as e:
        print(f"SSH Tunnel error: {e}")
        raise


def make_db(tunnel):
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(DB_CONN_ID)
        return SQLConnection(
            db_host=conn.host,
            db_port=conn.port or 3306,
            db_database=conn.schema,
            db_username=conn.login,
            db_password=conn.password,
            dialect="mysql", driver="pymysql",
            ssh_tunnel=tunnel,
        )
    except Exception as e:
        print(f"DB connection error: {e}")
        raise


def query_units_by_nifs(nif_list: list) -> pd.DataFrame:
    try:
        if not nif_list:
            return pd.DataFrame(columns=['id', 'nif', 'id_unit_izaro'])

        where = f"nif IN ({', '.join(repr(n) for n in nif_list)})"
        try:
            tunnel = make_tunnel()
        except Exception as e:
            print(f"SSH tunnel unavailable, trying direct connection: {e}")
            tunnel = None
        with make_db(tunnel) as db:
            df = db.get_table_info(
                table_name='Unit',
                cols='id, nif, id_unit_izaro',
                where=where,
            )
        return df if not df.empty else pd.DataFrame(columns=['id', 'nif', 'id_unit_izaro'])
    except Exception as e:
        print(f"DB query error: {e}")
        raise


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
        "PEDIDO": "N Pedido",
        "FECHA": "Fecha Pedido",
        "PRODUCTO": "Codigo Producto",
        "UNIDADES": "Unidades",
        "PRECIO": "Precio",
        "DTO": "Descuento",
        "Cliente Alliance": "Codigo Farmacia",
    }
    df = df.rename(columns=mapping)
    if 'Precio' in df.columns:
        df['Precio'] = df['Precio'].round(2).astype(str).str.replace('.', ',', regex=False)
    if 'Fecha Pedido' in df.columns:
        df['Fecha Pedido'] = pd.to_datetime(df['Fecha Pedido'], utc=True).dt.strftime('%d/%m/%Y')

    final_columns = [
        "N Pedido", "Codigo Farmacia", "Codigo Producto",
        "Unidades", "Precio", "Descuento", "Fecha Pedido",
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
    if not SSH_CONN_ID:
        print("SKIP - SSH_CONN_ID not configured")
        return []

    pedidos_df = pd.DataFrame(pedidos)
    nif_list = extract_unique_nifs(pedidos_df)
    print(f"Querying Unit table for {len(nif_list)} NIFs...")
    units_df = query_units_by_nifs(nif_list)
    if units_df is None:
        print("No matching units found")
        return []
    else:
        print(f"Found {units_df.shape} matches")
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
    print(f"Orders DataFrame: {pedidos_df.head(2)}")
    billing_df = pd.DataFrame(billing_data) if billing_data else pd.DataFrame()
    print(f"Billing DataFrame: {billing_df.head(2)}")
    nif_df = extract_fid_from_response(nif_data) if nif_data else pd.DataFrame(columns=['id', 'nif'])
    print(f"NIF DataFrame: {nif_df.head(2)}")
    alliance_df = pd.DataFrame(alliance_data) if alliance_data else pd.DataFrame(columns=['nif', 'id_unit_izaro'])

    df = merge_orders_with_nif(pedidos_df, nif_df)
    df = merge_with_billing(df, billing_df)

    if not alliance_df.empty:
        df = df.merge(
            alliance_df.rename(columns={'id_unit_izaro': 'Cliente Alliance'}),
            left_on='NIF', right_on='nif', how='left'
        )
        df = df.drop(columns=['nif'], errors='ignore')
    else:
        df['Cliente Alliance'] = None

    print(f"Columns before mapping: {list(df.columns)}")
    df = apply_final_column_mapping(df)
    print(f"Fact table ready: {len(df)} rows, columns: {list(df.columns)}")
    return df.to_dict('records')


def task_upload_to_ftp(**context):
    records = context['task_instance'].xcom_pull(task_ids='load_fact_table')
    if not records:
        print("No data to upload")
        return []
    df = pd.DataFrame(records)
    col_order = ["N Pedido", "Codigo Farmacia", "Codigo Producto", "Unidades", "Precio", "Descuento", "Fecha Pedido"]
    df = df[[c for c in col_order if c in df.columns]]

    max_pedido = context['task_instance'].xcom_pull(task_ids='cleanup_ftp') or 0
    df = df[df['N Pedido'].astype(int) > int(max_pedido)]
    print(f"Uploading pedidos > {max_pedido}: {df['N Pedido'].nunique()} pedidos")
    if df.empty:
        print("No new pedidos to upload")
        return []

    uploaded = []
    with FTPConn.from_airflow(FTP_CONN_ID) as ftp:
        for pedido in df['N Pedido'].unique():
            df_pedido = df[df['N Pedido'] == pedido]
            remote_file = f"{FTP_REMOTE_PATH}Pedidos_AP_{pedido}.csv"
            ftp.upload_df(df_pedido, remote_file, sep=";", sheet_name=f"Pedidos_AP_{pedido}")
            uploaded.append(str(pedido))
    print(f"Uploaded {len(df)} rows to FTP ({len(uploaded)} pedidos)")
    return uploaded

def notify_logicommerce_order_status(token: str, order_number: str, state: int = 2) -> None:
    """Change a LogiCommerce order state via /orders/{orderNumber}/changeState.
    state=2 → in_process
    """
    url = f"{LOGICOMMERCE_API_BASE}/orders/{order_number}/changeState"
    headers = build_logicommerce_headers(token)
    payload = {"changeState": {"state": state}}
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    print(f"  Order {order_number} → state {state} (HTTP {response.status_code})")


def task_notify_logicommerce(**context):
    """Notify LogiCommerce that uploaded pedidos are now in_process."""
    ti = context['task_instance']
    uploaded = ti.xcom_pull(task_ids='upload_to_ftp') or []
    if not uploaded:
        print("No pedidos to notify")
        return

    token = ti.xcom_pull(task_ids='generate_token')
    print(f"Notifying LogiCommerce for {len(uploaded)} pedidos: {uploaded}")
    failed = []
    for order_number in uploaded:
        try:
            # state=2 (in_process) is hardcoded — only state used in this pipeline for now
            notify_logicommerce_order_status(token, order_number, state=2)
            sleep(API_RATE_LIMIT_DELAY)
        except requests.HTTPError as e:
            print(f"  Order {order_number}: FAILED ({e.response.status_code} - {e.response.text})")
            failed.append(order_number)

    if failed:
        raise RuntimeError(f"Failed to notify LogiCommerce for orders: {failed}")
    print(f"All {len(uploaded)} pedidos marked as in_process")


def task_cleanup(**context):
    try:
        with FTPConn.from_airflow(FTP_CONN_ID) as ftp:
            files = ftp.list(FTP_REMOTE_PATH)
            pedido_files = [f for f in files if f.rsplit('/', 1)[-1].startswith("Pedidos_AP_")]

            max_pedido = 0
            for file in pedido_files:
                try:
                    num = int(file.rsplit('/', 1)[-1].replace("Pedidos_AP_", "").replace(".csv", ""))
                    if num > max_pedido:
                        max_pedido = num
                except ValueError:
                    pass

            print(f"Max pedido on FTP before cleanup: {max_pedido}")
            for file in pedido_files:
                ftp.remove(file)
            print(f"Cleanup done, deleted {len(pedido_files)} files")

        return max_pedido
    except Exception as e:
        print(f"Error during cleanup: {e}")
        raise
        
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

cleanup_task = PythonOperator(
    task_id='cleanup_ftp',
    python_callable=task_cleanup,
    dag=dag,
)

change_state_logicommerce_task = PythonOperator(
    task_id='change_state_logicommerce',
    python_callable=task_notify_logicommerce,
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

[extract_nif_task, transform_users_task, alliance_clients_task] >> load_task >> cleanup_task >> upload_ftp_task >> change_state_logicommerce_task
