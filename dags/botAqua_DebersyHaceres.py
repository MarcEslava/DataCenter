
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'botAqua_debers_y_haceres',
    default_args=default_args,
    description='Debers y Haceres bot execution',
    schedule=Variable.get("botaqua_debers_schedule", default_var="0 7 * * *"),
    tags=["aqua", "bot", "rpa"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
)


def task_execute_bot(**_):
    import requests

    try:
        response = requests.post(
            "https://ecoceutics-dev.bbos.services.aquabpi.com/api/instance/execute",
            json={
                "InstanceId":  "b71ccef7-959c-421d-b909-71180ae55172",
                "SkillId":     "f760bc8e-6c0f-4f49-acf3-2b3e2692d7e9",
                "Password":    "ecoceutics",
                "Parameters":  [],
                "IsDebug":     None,
                "DeveloperId": None,
            },
            verify=False,
        )
        response.raise_for_status()
        print(f"Bot triggered: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Error executing bot: {e}")
        raise


execute_bot_task = PythonOperator(
    task_id='execute_bot',
    python_callable=task_execute_bot,
    dag=dag,
)

execute_bot_task
