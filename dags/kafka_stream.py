from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    return res.json()['results'][0]

def format_data(res):
    data = {
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'email': res['email']
    }
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while time.time() < curr_time + 60:  # Stream for 1 minute
        try:
            res = get_data()
            formatted_data = format_data(res)
            producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )