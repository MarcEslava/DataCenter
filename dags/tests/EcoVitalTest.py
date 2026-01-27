from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import hashlib, base64, requests, pandas as pd
from time import sleep
import json, ast

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecovital_etl',
    default_args=default_args,
    description='EcoVital Orders ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# ─────────────────────────────────────────────────────────────
# Extract Functions
# ─────────────────────────────────────────────────────────────

def generate_token(**context):
    """
    Generate SHA256 authentication token for LogiCommerce API.

    Creates a Base64-encoded SHA256 hash from the API secret key.
    This token is used for Basic authentication in subsequent API calls.

    Returns:
        str: Base64-encoded authentication token
    """
    data = "pK3c76MsxY73eS9F2ke9gAvfBb2x84"
    digest = hashlib.sha256(data.encode("utf-8")).digest()
    token = base64.b64encode(digest).decode("utf-8")
    return token


def extract_orders(**context):
    """
    Fetch all orders from LogiCommerce API.

    Pulls the authentication token from XCom and makes a GET request
    to the orders endpoint. Returns the raw JSON response containing
    all orders for the ES country.

    Returns:
        dict: JSON response with ORDERS array containing order data
    """
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    url = "https://api.logicommerce.net/v1/orders"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {token}",
        "countryCode": "ES",
        "appid": "pK3c76MsxY"
    }
    response = requests.get(url, headers=headers)
    print("API Response:", response.status_code)
    response.raise_for_status()

    try:
        data = response.json()
        print(f"Fetched {len(data)} records")
    except Exception as e:
        print("Error parsing JSON:", e)
        data = []
    return data


def extract_users(**context):
    """
    Fetch all users from LogiCommerce API.

    Retrieves user data including billing addresses for later
    enrichment of the fact table. Runs in parallel with order extraction.

    Returns:
        dict: JSON response with USERS array containing user profiles
    """
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    url = "https://api.logicommerce.net/v1/users"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {token}",
        "countryCode": "ES",
        "appid": "pK3c76MsxY"
    }
    response = requests.get(url, headers=headers)
    print("API Response:", response.status_code)
    response.raise_for_status()

    try:
        data = response.json()
        print(f"Fetched {len(data)} records")
    except Exception as e:
        print("Error parsing JSON:", e)
        data = []
    return data


# ─────────────────────────────────────────────────────────────
# Transform Functions
# ─────────────────────────────────────────────────────────────

def transform_orders(**context):
    """
    Extract document numbers from raw orders data.

    Normalizes the ORDERS array and extracts DOCUMENTNUMBER field
    to create a list of order IDs for detailed lookup.

    Returns:
        list: List of order document numbers (e.g., ['ORD001', 'ORD002'])
    """
    data = context['task_instance'].xcom_pull(task_ids='extract_orders')
    df = pd.json_normalize(data.get("ORDERS", []))
    return df['DOCUMENTNUMBER'].tolist()


def extract_order_details(**context):
    """
    Fetch detailed order data for each order number.

    Iterates through order document numbers and fetches full details
    including line items (DETAILS), dates, and customer NIF.
    Extracts discount values from nested DISCOUNTS array.

    Rate limited: 0.3s delay between API calls to avoid throttling.

    Returns:
        list[dict]: List of order detail records with columns:
            - PEDIDO: Order ID
            - DATE: Order date
            - SKU: Product code
            - QUANTITY: Units ordered
            - PRICE: Unit price
            - DISCOUNTVALUE: Applied discount
            - NIF: Customer tax ID
            - TAXES: Tax information
    """
    token = context['task_instance'].xcom_pull(task_ids='generate_token')
    order_numbers = context['task_instance'].xcom_pull(task_ids='transform_orders')

    results = pd.DataFrame()
    try:
        for x in order_numbers:
            print(f"Processing order Number: {x}")
            url = f"https://api.logicommerce.net/v1/orders/{x}"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {token}",
                "countryCode": "ES",
                "appid": "pK3c76MsxY"
            }
            response = requests.get(url, headers=headers)
            print("API Response:", response.status_code)
            response.raise_for_status()
            response_data = response.json()

            response_df = pd.json_normalize(
                response_data,
                record_path=["ORDERS", "DETAILS"],
                meta=[["ORDERS", "DATE"], ["ORDERS", "ORDERID"], ["ORDERS", "ORDERUSERS", "NIF"]],
                errors="ignore"
            ).rename(columns={
                "ORDERS.ORDERID": "PEDIDO",
                "ORDERS.DATE": "DATE",
                "ORDERS.ORDERUSERS.NIF": "NIF"
            })

            response_df["DISCOUNTVALUE"] = response_df["DISCOUNTS"].apply(
                lambda d: (
                    d[0].get("DISCOUNTVALUE")
                    if isinstance(d, list) and d and isinstance(d[0], dict)
                    else 0
                )
            )

            pedido_df = response_df[[
                "PEDIDO", "DATE", "SKU", "QUANTITY",
                "PRICE", "DISCOUNTVALUE", "NIF", "TAXES"
            ]]

            results = pd.concat([results, pedido_df], ignore_index=True)
            sleep(0.3)

    except Exception as e:
        print("Error during API call:", e)
        raise

    print("Fetched order details:", results.shape)
    return results.to_dict('records')


def extract_nif_data(**context):
    """
    Fetch customer FID (fidelity ID) from Ecoceutics API using NIF.

    Extracts unique NIF values from order details and queries the
    Ecoceutics FID API to get internal customer identifiers.
    Used to enrich orders with pharmacy customer IDs.

    Rate limited: 0.3s delay between API calls.

    Returns:
        list: List of [api_response, nif] pairs for each customer
    """
    pedidos = context['task_instance'].xcom_pull(task_ids='extract_order_details')
    pedidos_df = pd.DataFrame(pedidos)
    nif_list = pedidos_df['NIF'].drop_duplicates().tolist()

    results = []
    for unit in nif_list:
        print(f"Processing NIF: {unit}")
        url = f"https://apifidfarma.ecoceutics.com/v1/unit/{unit}/fid/?api_key=657A8288P7156"
        headers = {
            "Accept": "application/json",
            "countryCode": "ES",
        }
        response = requests.get(url, headers=headers)
        print("API Response:", response.text)
        payload = response.json()
        response.raise_for_status()
        results.append([payload, unit])
        sleep(0.3)

    return results


def transform_users(**context):
    """
    Transform users data and extract billing addresses.

    Parses the nested ADDRESSBOOK.BILLINGADDRESS field which may contain
    JSON strings, dicts, or lists. Normalizes the billing address data
    into a flat DataFrame with fields like COMPANY, ADDRESS, CITY, ZIP, STATE.

    Handles edge cases:
        - None values
        - JSON strings requiring parsing
        - Lists with dict elements
        - Already parsed dicts

    Returns:
        list[dict]: Normalized billing address records with NIF as key
    """
    users_data = context['task_instance'].xcom_pull(task_ids='extract_users')
    users = pd.json_normalize(users_data.get("USERS", []))

    col = "ADDRESSBOOK.BILLINGADDRESS"
    if col not in users.columns:
        print(f"Column '{col}' not found in users dataframe")
        return []

    def _parse_cell(x):
        if x is None:
            return None
        if isinstance(x, (dict, list)):
            return x
        s = str(x)
        try:
            return json.loads(s)
        except Exception:
            try:
                return ast.literal_eval(s)
            except Exception:
                return None

    parsed = users[col].apply(_parse_cell)

    normalized_items = []
    for item in parsed:
        if isinstance(item, list):
            if len(item) == 0:
                normalized_items.append({})
            elif isinstance(item[0], dict):
                normalized_items.append(item[0])
            else:
                normalized_items.append({"value": item})
        elif isinstance(item, dict):
            normalized_items.append(item)
        else:
            normalized_items.append({})

    billing_df = pd.json_normalize(normalized_items)
    print("Billing address dataframe created:", billing_df.shape)
    return billing_df.to_dict('records')


# ─────────────────────────────────────────────────────────────
# Load Function
# ─────────────────────────────────────────────────────────────

def load_fact_table(**context):
    """
    Merge all extracted data and create the final EcoVital fact table.

    Combines data from three sources:
        1. Order details (pedidos) - line items with prices and quantities
        2. NIF data - customer FID mappings from Ecoceutics
        3. Billing data - pharmacy addresses from user profiles

    Processing steps:
        1. Extract customer ID from NIF API response
        2. Merge NIF data with orders on customer tax ID
        3. Merge billing addresses for pharmacy details
        4. Apply column renaming for final schema
        5. Round prices to 2 decimal places
        6. Export to Excel file

    Output columns:
        - Pedido: Order number
        - F.Pedido: Order date
        - Farmacia: Pharmacy name
        - Direcion: Address
        - Poblacion: City
        - Codigo Postal: ZIP code
        - Provincia: State/Province
        - Codigo Producto: SKU
        - C.Pedida: Quantity
        - Precio: Unit price (rounded)
        - Descuento: Discount applied
        - CustomerCifId: Customer NIF
        - NIF2: Ecoceutics customer ID

    Returns:
        str: Path to the generated Excel file
    """
    ti = context['task_instance']

    # Pull all transformed data
    pedidos = ti.xcom_pull(task_ids='extract_order_details')
    nif_data = ti.xcom_pull(task_ids='extract_nif_data')
    billing_data = ti.xcom_pull(task_ids='transform_users')

    pedidos_df = pd.DataFrame(pedidos)
    billing_df = pd.DataFrame(billing_data)

    # Process NIF data
    df_NIF = pd.DataFrame(nif_data, columns=['raw', 'nif'])
    df_NIF['id'] = df_NIF['raw'].apply(lambda x: x[0].get('id') if isinstance(x, list) and x else None)
    df_NIF = df_NIF[['id', 'nif']]
    df_NIF.drop_duplicates(['id'], inplace=True)

    # Merge NIF with pedidos
    df = df_NIF.merge(pedidos_df, left_on='nif', right_on='NIF', how='right')
    df.drop(columns=['nif'], inplace=True)

    # Rename columns (first mapping)
    mapping1 = {
        "id": "NIF2",
        "SKU": "PRODUCTO",
        "PRICE": "PRECIO",
        "QUANTITY": "UNIDADES",
        "DISCOUNTVALUE": "DTO",
        "DATE": "FECHA",
    }
    df.rename(columns=mapping1, inplace=True)

    # Merge with billing data
    fact_df = df.merge(billing_df, left_on='NIF', right_on='NIF', how='left')

    # Final column mapping
    mapping2 = {
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
    fact_df.rename(columns=mapping2, inplace=True)

    if 'Precio' in fact_df.columns:
        fact_df['Precio'] = fact_df['Precio'].round(2)

    # Save to Excel
    output_path = "/opt/airflow/dags/output/EcoVital_FactTable.xlsx"
    fact_df.to_excel(output_path, index=False, sheet_name="in")
    print(f"Saved fact table to {output_path}")
    print(fact_df.head())

    return output_path


# ─────────────────────────────────────────────────────────────
# Task Definitions
# ─────────────────────────────────────────────────────────────

generate_token_task = PythonOperator(
    task_id='generate_token',
    python_callable=generate_token,
    dag=dag,
)

extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

extract_users_task = PythonOperator(
    task_id='extract_users',
    python_callable=extract_users,
    dag=dag,
)

transform_orders_task = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

extract_order_details_task = PythonOperator(
    task_id='extract_order_details',
    python_callable=extract_order_details,
    dag=dag,
)

extract_nif_task = PythonOperator(
    task_id='extract_nif_data',
    python_callable=extract_nif_data,
    dag=dag,
)

transform_users_task = PythonOperator(
    task_id='transform_users',
    python_callable=transform_users,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_fact_table',
    python_callable=load_fact_table,
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

[extract_nif_task, transform_users_task] >> load_task
