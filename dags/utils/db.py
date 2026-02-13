"""
Database connection module for Airflow ETL pipelines.

Single entry point: get_connection(conn_id)
    - conn_id ending with '_ssh' -> SSH tunnel (via utils.ssh)
    - conn_id without '_ssh'     -> direct Airflow hook

Usage:
    from utils.db import get_connection

    with get_connection("eco_extract_ssh") as db:     # SSH tunnel
        df = db.fetchdf("SELECT * FROM orders")

    with get_connection("postgres_default") as db:    # Direct
        db.execute("INSERT INTO t VALUES (%s)", (1,))
        rows = db.fetchall("SELECT * FROM t")
"""

from contextlib import contextmanager
from typing import List, Optional
import pandas as pd


# ─────────────────────────────────────────────────────────────
# DBConn (direct via Airflow hook)
# ─────────────────────────────────────────────────────────────

class DBConn:
    """
    Context manager for PostgreSQL connections via Airflow hook.

    Usage:
        with DBConn("postgres_default") as db:
            db.execute("INSERT INTO table VALUES (%s)", (value,))
            rows = db.fetchall("SELECT * FROM table")
            df = db.fetchdf("SELECT * FROM table")
    """

    def __init__(self, conn_id: str = "postgres_default"):
        self.conn_id = conn_id
        self.conn = None
        self.cursor = None

    def __enter__(self):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.conn = hook.get_conn()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.cursor.close()
        self.conn.close()
        return False

    def execute(self, sql: str, parameters: Optional[tuple] = None) -> None:
        """Execute a SQL statement."""
        self.cursor.execute(sql, parameters)

    def executemany(self, sql: str, seq_of_parameters: List[tuple]) -> None:
        """Execute a SQL statement with multiple parameter sets."""
        self.cursor.executemany(sql, seq_of_parameters)

    def fetchall(self, sql: str, parameters: Optional[tuple] = None) -> List:
        """Execute SELECT and fetch all rows."""
        self.cursor.execute(sql, parameters)
        return self.cursor.fetchall()

    def fetchone(self, sql: str, parameters: Optional[tuple] = None) -> Optional[tuple]:
        """Execute SELECT and fetch one row."""
        self.cursor.execute(sql, parameters)
        return self.cursor.fetchone()

    def fetchdf(self, sql: str, parameters: Optional[tuple] = None) -> pd.DataFrame:
        """Execute SELECT and return as DataFrame."""
        return pd.read_sql(sql, self.conn, params=parameters)


# ─────────────────────────────────────────────────────────────
# Connection Factory (auto-detect SSH by conn_id suffix)
# ─────────────────────────────────────────────────────────────

@contextmanager
def get_connection(conn_id: str):
    """
    Factory that returns the right DB connection based on conn_id.

    If conn_id ends with '_ssh', opens an SSH tunnel using SSH credentials
    from the Airflow connection and DB credentials from its 'extra' JSON.
    Otherwise, uses a direct Airflow hook connection.

    Airflow connection setup for SSH tunneled DBs:
        Connection Id:   eco_extract_ssh
        Connection Type: SSH
        Host:            bastion.example.com
        Login:           ssh_user
        Port:            22
        Password:        (or leave empty if using key_file)
        Extra:           {
            "key_file":    "/home/app/.ssh/id_rsa",
            "db_host":     "127.0.0.1",
            "db_port":     3306,
            "db_user":     "root",
            "db_password": "secret",
            "db_name":     "ecoextract",
            "db_driver":   "mysql"
        }

    Both paths return an object with the same interface:
        .execute(sql, params)
        .executemany(sql, params_list)
        .fetchall(sql, params)
        .fetchone(sql, params)
        .fetchdf(sql, params)
    """
    if conn_id.endswith("_ssh"):
        from airflow.hooks.base import BaseHook
        from utils.ssh import SSHTunnelDB

        conn_meta = BaseHook.get_connection(conn_id)
        extra = conn_meta.extra_dejson

        tunnel = SSHTunnelDB(
            ssh_host=conn_meta.host,
            ssh_port=conn_meta.port or 22,
            ssh_user=conn_meta.login,
            ssh_password=conn_meta.password,
            ssh_pkey=extra.get("key_file"),
            db_host=extra.get("db_host", "127.0.0.1"),
            db_port=int(extra.get("db_port", 3306)),
            db_user=extra.get("db_user"),
            db_password=extra.get("db_password"),
            db_name=extra.get("db_name"),
            db_driver=extra.get("db_driver", "mysql"),
        )
        with tunnel as db:
            yield db
    else:
        with DBConn(conn_id) as db:
            yield db
