# dags/pharmacy_queries.py
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from  airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import json
import uuid

with DAG(
    dag_id="rust_microservice",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    start = EmptyOperator(task_id="start")

    run_id = "{{ run_id }}"  # tie to idempotency
    payload = {
        "query_type": "INVENTORY_SYNC",
        "params": {"sku_list": ["A123","B456"]},
        "pharmacies": [f"pharmacy-{i:03d}" for i in range(1, 101)],
        "idempotency_key": "{{ dag.dag_id }}::{{ task.task_id }}::" + run_id,
        "airflow_run_id": run_id,
    }

    create_job = SimpleHttpOperator(
        task_id="create_job",
        http_conn_id="rust_service",  # define in Airflow connections
        endpoint="health",
        method="GET",
        headers={"Content-Type": "application/json"},
        # data=json.dumps(payload),
        response_filter=lambda r: r.json(),
        log_response=True,
        do_xcom_push=True,
    )

    def job_done(response):
        data = response.json()
        return data.get("status") in ("SUCCEEDED", "FAILED")

    # wait_job = HttpSensor(
    #     task_id="wait_job",
    #     http_conn_id="rust_service",
    #     endpoint="health",
    #     poke_interval=10,
    #     timeout=60 * 60,
    #     headers={"Authorization": "Bearer {{ var.value.RUST_API_TOKEN }}"},
    #     mode="reschedule",  # deferrable style polling
    # )

    end = EmptyOperator(task_id="end")

    start >> create_job  >> end
