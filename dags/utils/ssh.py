"""
SSH Tunnel database connection module.

Connects to remote databases through an SSH tunnel (bastion host).
Supports MySQL/MariaDB and PostgreSQL.

Usage:
    from utils.ssh import SSHTunnelDB

    # Direct credentials
    with SSHTunnelDB(
        ssh_host="bastion.example.com",
        ssh_user="deploy",
        ssh_pkey="/home/app/.ssh/id_rsa",
        db_host="10.0.0.5",
        db_port=3306,
        db_user="root",
        db_password="secret",
        db_name="ecoextract",
    ) as db:
        df = db.fetchdf("SELECT * FROM orders")
        rows = db.fetchall("SHOW TABLES")

    # From Airflow SSH connection
    with SSHTunnelDB.from_airflow(
        ssh_conn_id="eco_extract_ssh",
        db_host="127.0.0.1",
        db_port=3306,
        db_user="root",
        db_password="secret",
        db_name="ecoextract",
    ) as db:
        df = db.fetchdf("SELECT * FROM orders")
"""

from typing import List, Optional
import pandas as pd


class SSHTunnelDB:
    """
    Database connection through SSH tunnel.

    Opens an SSH tunnel to a remote host, then connects to a database
    through the tunnel. Auto-closes tunnel and connection on exit.
    """

    def __init__(
        self,
        ssh_host: str,
        ssh_user: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
        ssh_port: int = 22,
        ssh_password: Optional[str] = None,
        ssh_pkey: Optional[str] = None,
        db_driver: str = "mysql",
    ):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.ssh_pkey = ssh_pkey
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.db_driver = db_driver
        self.tunnel = None
        self.conn = None
        self.cursor = None

    @classmethod
    def from_airflow(
        cls,
        ssh_conn_id: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
        db_driver: str = "mysql",
    ):
        """Create SSHTunnelDB from an Airflow SSH connection ID."""
        from airflow.hooks.base import BaseHook
        ssh_conn = BaseHook.get_connection(ssh_conn_id)
        return cls(
            ssh_host=ssh_conn.host,
            ssh_port=ssh_conn.port or 22,
            ssh_user=ssh_conn.login,
            ssh_password=ssh_conn.password,
            ssh_pkey=ssh_conn.extra_dejson.get("key_file"),
            db_host=db_host,
            db_port=db_port,
            db_user=db_user,
            db_password=db_password,
            db_name=db_name,
            db_driver=db_driver,
        )

    def _connect_mysql(self, local_port: int):
        """Open MySQL/MariaDB connection through tunnel."""
        import pymysql
        return pymysql.connect(
            host="127.0.0.1",
            port=local_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _connect_postgres(self, local_port: int):
        """Open PostgreSQL connection through tunnel."""
        import psycopg2
        return psycopg2.connect(
            host="127.0.0.1",
            port=local_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name,
        )

    def __enter__(self):
        import paramiko
        if not hasattr(paramiko, 'DSSKey'):
            paramiko.DSSKey = type(None)  # stub for sshtunnel compat with paramiko 3.x
        from sshtunnel import SSHTunnelForwarder

        # Build SSH auth kwargs
        ssh_kwargs = {}
        if self.ssh_pkey:
            ssh_kwargs["ssh_pkey"] = self.ssh_pkey
        if self.ssh_password:
            ssh_kwargs["ssh_password"] = self.ssh_password

        # Open SSH tunnel
        self.tunnel = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            remote_bind_address=(self.db_host, self.db_port),
            **ssh_kwargs,
        )
        self.tunnel.start()

        # Connect to DB through tunnel
        local_port = self.tunnel.local_bind_port
        if self.db_driver == "mysql":
            self.conn = self._connect_mysql(local_port)
        else:
            self.conn = self._connect_postgres(local_port)

        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        if self.tunnel:
            self.tunnel.stop()
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
