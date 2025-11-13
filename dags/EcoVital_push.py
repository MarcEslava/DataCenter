from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import hashlib, base64, requests, pandas as pd

def generate_token():
    data = "pK3c76MsxY73eS9F2ke9gAvfBb2x84"
    digest = hashlib.sha256(data.encode("utf-8")).digest()
    token = base64.b64encode(digest).decode("utf-8")
    print(f"Generated token: {token}")
    return token

def call_api(**context):
    token = context['ti'].xcom_pull(task_ids='generate_token_task')
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

def transform_data(**context):
    data = context['ti'].xcom_pull(task_ids='call_api_task')
    # print("Raw Data:", data)
    df = pd.json_normalize(data.get("ORDERS", []))
    print("Transformed Data:", df['DOCUMENTNUMBER'].head())
    return df


with DAG(
    dag_id="logicommerce_order_retrieval",
    start_date=datetime(2025, 11, 5),
    schedule=None,
    catchup=False
) as dag:

    generate_token_task = PythonOperator(
        task_id="generate_token_task",
        python_callable=generate_token
    )

    call_api_task = PythonOperator(
        task_id="call_api_task",
        python_callable=call_api,
    )
    
    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data,
    )

    generate_token_task >> call_api_task >> transform_data_task
