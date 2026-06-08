from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta


@dag(
    dag_id='push_alloga_etl',
    description='EcoVital Orders ETL Pipeline',
    schedule=Variable.get("push_alloga_schedule", default_var="30 10 * * 1-5"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
)
def push_alloga_etl():

    @task
    def execute_bot() -> None:
        import requests
        try:
            response = requests.post(
                "https://ecoceutics-dev.bbos.services.aquabpi.com/api/instance/execute",
                json={
                    "InstanceId":  "b71ccef7-959c-421d-b909-71180ae55172",
                    "SkillId":     "f760bc8e-6c0f-4f49-acf3-2b3e2692d7e9",
                    "Password":    "ecoceutics",
                    "Parameters":  [{"Name": "Id", "Type": 0, "Value": "0d59a167-89f1-4e58-a46c-0c420269ff87"}],
                    "IsDebug":     True,
                    "DeveloperId": None,
                },
                verify=False,
                timeout=30,
            )
            response.raise_for_status()
            print(f"Bot triggered: {response.status_code} {response.text}")
        except Exception as e:
            print(f"Error executing bot: {e}")
            raise

    execute_bot()


push_alloga_etl()
