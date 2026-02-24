"""Add alloga_ftp connection via Airflow's API (handles Fernet encryption)."""
from airflow.models.connection import Connection
from airflow.settings import Session

session = Session()

# Delete existing if any
session.query(Connection).filter(Connection.conn_id == "alloga_ftp").delete()
session.commit()

# Create with proper encryption
conn = Connection(
    conn_id="alloga_ftp",
    conn_type="ftp",
    host="ftp.ecoceutics.com",
    login="alloga",
    password="Vx2019Alloga",
    port=735,
    extra='{"protocol": "ftp"}',
)
session.add(conn)
session.commit()
print(f"Connection 'alloga_ftp' created (password encrypted with Fernet)")
session.close()
