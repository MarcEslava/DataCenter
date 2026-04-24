from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

# ── Connections (set in Airflow UI → Admin → Connections) ──────────────────
ECOEXTRACT_DB_CONN_ID  = "ecoextract_db"   # DB connection to read units
ECOEXTRACT_SSH_CONN_ID = "ecoextract_ssh"  # SSH connection to the server

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'actualizar_ecoextract',
    default_args=default_args,
    description='Run ecoextract_batch.sh for each unit via SSH',
    schedule=Variable.get("ecoextract_schedule", default_var="0 5 * * *"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
)


def task_run_ecoextract(**_):
    import paramiko
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection

    # ── 1. Fetch units from DB ───────────────────────────────
    db_conn = BaseHook.get_connection(ECOEXTRACT_DB_CONN_ID)
    db = SQLConnection(
        db_host=db_conn.host,
        db_port=db_conn.port or 3306,
        db_database=db_conn.schema,
        db_username=db_conn.login,
        db_password=db_conn.password,
        dialect=db_conn.conn_type or "mysql",
        driver="pymysql",
    )
    db.connect()
    try:
        units_df = db.fech_dataframe("""
            SELECT
                id,
                IFNULL(id_ecobuy, 0)                        AS id_ecobuy,
                TIMESTAMPDIFF(DAY, lastconnection, NOW())    AS days_since_connection
            FROM Unit
            WHERE ecobuy = 0
              AND IFNULL(port, '') <> ''
              AND enabled = 1
              AND TIMESTAMPDIFF(DAY, lastconnection, NOW()) > 2
            ORDER BY port
        """)
    finally:
        db.close()

    units = units_df.to_dict("records")
    print(f"Found {len(units)} units", flush=True)

    # ── 2. SSH into server and run script for each unit ──────
    ssh_conn = BaseHook.get_connection(ECOEXTRACT_SSH_CONN_ID)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = dict(
        hostname=ssh_conn.host,
        port=ssh_conn.port or 22,
        username=ssh_conn.login,
    )
    if ssh_conn.password:
        connect_kwargs["password"] = ssh_conn.password
    if ssh_conn.extra_dejson.get("private_key"):
        from io import StringIO
        pkey_str = ssh_conn.extra_dejson["private_key"]
        for key_class in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key):
            try:
                connect_kwargs["pkey"] = key_class.from_private_key(StringIO(pkey_str))
                break
            except paramiko.SSHException:
                continue

    client.connect(**connect_kwargs)
    print(f"SSH connected to {ssh_conn.host}", flush=True)

    try:
        errors = []
        for unit in units:
            idunit    = str(unit["id"])
            id_ecobuy = int(unit["id_ecobuy"])
            cmd = f"bash /var/www/ecoextract/scripts/ecoextract_batch.sh {idunit} {id_ecobuy}"
            print(f"[unit {idunit}] Running: {cmd}", flush=True)
            _, stdout, stderr = client.exec_command(cmd)
            exit_code = stdout.channel.recv_exit_status()
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()

            if out:
                print(f"[unit {idunit}] stdout: {out}", flush=True)
            if exit_code != 0:
                msg = f"[unit {idunit}] FAILED (exit {exit_code}): {err}"
                print(msg, flush=True)
                errors.append(msg)
            else:
                print(f"[unit {idunit}] OK", flush=True)

        if errors:
            raise RuntimeError(f"{len(errors)} unit(s) failed:\n" + "\n".join(errors))

    finally:
        client.close()
        print("SSH connection closed.", flush=True)


execute_task = PythonOperator(
    task_id='run_ecoextract_per_unit',
    python_callable=task_run_ecoextract,
    dag=dag,
)

execute_task
