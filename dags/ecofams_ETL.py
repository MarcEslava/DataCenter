"""
Ecofams ETL

For each EcoFams pharmacy, produces a per-pharmacy article file ({unit}.txt) and
ships it to ecofams via FTP, where the pharmacy DB ingests it.

Flow:
  1. extract            — list the EcoFams pharmacies (ecoextract.Unit)
  2. run_scripts        — SSH ingestion: push each pharmacy's data into ecoextract
  3. process_per_pharmacy — per unit, build the .txt from
        Articu JOIN Fam_SubFam_x_unit on (unit, local family)
     → cn / descripcion / FAMILIA / SUPERFAMILIA, tab-separated, CRLF, ISO-8859-1,
       sorted by cn → uploaded to FTP (or written to LOCAL_TXT_DIR in test mode).
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ECOFAMS_CONN_ID    = "ecofams_db"        # ecofams mysql (FTP config + Excel_conversor)
ECOEXTRACT_CONN_ID = "ecoextract_db"     # ecoextract mysql (Articu, Fam_SubFam_x_unit, Unit)
BIFARMA_C_CONN_ID  = "bifarmaCentral_db" # mssql — BifarmaCentral (canonical product family)
SSH_CONN_ID        = "ecoextract_ssh"    # bastion for the ingestion batch script

# TEST MODE: write the per-pharmacy .txt files to LOCAL_TXT_DIR for inspection and
# skip the FTP upload. Set ENABLE_FTP_UPLOAD = True to push to the FTP server instead.
ENABLE_FTP_UPLOAD = True
LOCAL_TXT_DIR     = "/opt/airflow/docs"   # mounted to ./docs on the host

# ─────────────────────────────────────────────────────────────
# DB helper
# ─────────────────────────────────────────────────────────────

_DIALECT_DEFAULTS = {
    "mssql": {"driver": "pymssql", "port": 1433},
    "mysql": {"driver": "pymysql",  "port": 3306},
}

def _query_sql(conn_id: str, sql: str, dialect: str):
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    conn = BaseHook.get_connection(conn_id)
    defs = _DIALECT_DEFAULTS[dialect]
    db = SQLConnection(
        db_host=conn.host, db_port=conn.port or defs["port"],
        db_database=conn.schema, db_username=conn.login,
        db_password=conn.password, dialect=dialect, driver=defs["driver"],
    )
    with db:
        df = db.fech_dataframe(sql)
    # XCom can't serialize pandas Timestamps — stringify datetime columns; NaT → None.
    for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        df[col] = df[col].astype(str)
        df.loc[df[col] == "NaT", col] = None
    return df

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='ecofams_ETL',
    description='Per-pharmacy article .txt export to ecofams FTP',
    schedule=Variable.get("ecofams_etl_schedule", default_var="0 3 1 * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)
def ecofams_etl():

    @task
    def extract() -> list[dict]:
        df = _query_sql(ECOEXTRACT_CONN_ID, """
            SELECT id AS idunit, description
            FROM Unit
            WHERE ecoFams = 1
        """, dialect="mysql")
        print(f"Extracted {len(df)} farmacias")
        return df.to_dict("records")

    @task
    def run_scripts(farmacias: list[dict]) -> None:
        """Ingestion: SSH to the bastion and run the batch script per pharmacy,
        pushing each pharmacy's data into ecoextract."""
        import paramiko
        from airflow.hooks.base import BaseHook
        if not farmacias:
            print("No farmacias to process")
            return
        conn = BaseHook.get_connection(SSH_CONN_ID)
        key_file = conn.extra_dejson.get("key_file")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=conn.host,
            port=conn.port or 22,
            username=conn.login,
            key_filename=key_file,
            password=conn.password or None,
            look_for_keys=False,
            allow_agent=False,
        )
        try:
            for farmacia in farmacias:
                idunit = farmacia["idunit"]
                cmd = f"bash /var/www/ecoextract/scripts/ecoextract_batch_art.sh {idunit}"
                _, stdout, stderr = client.exec_command(cmd)
                exit_code = stdout.channel.recv_exit_status()
                out = stdout.read().decode().strip()
                err = stderr.read().decode().strip()
                if exit_code != 0:
                    print(f"[{idunit}] ERROR (exit {exit_code}): {err}")
                else:
                    print(f"[{idunit}] OK: {out[:200]}")
        finally:
            client.close()

    @task
    def process_per_pharmacy(farmacias: list[dict]) -> None:
        """
        Build and ship the per-pharmacy .txt. The only deliverable is the file
        ({unit}.txt) sent to ecofams via FTP, which the pharmacy DB then ingests.

        Family labels: BifarmaCentral has PRIORITY. For each article, if its cn exists in
        BifarmaCentral the canonical bifarma family is used; otherwise we fall back to the
        pharmacy's LOCAL family from Fam_SubFam_x_unit (e.g. orthopedics not in bifarma).
            cn / descripcion / FAMILIA / SUPERFAMILIA
        → tab-separated, CRLF, no header, ISO-8859-1, sorted by cn.
        """
        import os
        import pandas as pd
        from utils.ftp import FTPConn
        if not farmacias:
            print("No farmacias to process")
            return

        # FTP target
        ftp_cfg = _query_sql(ECOFAMS_CONN_ID,
            "SELECT server_ftp, port_ftp, user_ftp, pass_ftp, folder_ftp FROM ftp WHERE id = 1",
            dialect="mysql").iloc[0]
        ftp_folder = str(ftp_cfg["folder_ftp"]).rstrip("/")

        # ── Canonical bifarma family map (cn → FAMILIA/SUPERFAMILIA), built once. ──
        # bifarma product family (efp=1 → 220) mapped through Excel_conversor → labels.
        bif = _query_sql(BIFARMA_C_CONN_ID, """
            SELECT codProducto,
                   CASE WHEN efp = 1 THEN 220 ELSE idFamiliaEco END AS famEco
            FROM [dbo].[tme_productos]
            WHERE efp = 1 OR idFamiliaEco IS NOT NULL
        """, dialect="mssql")
        conv = _query_sql(ECOFAMS_CONN_ID, """
            SELECT ec.idfamilia_Bifarmaeco AS famEco, ok.FAMILIA, ok.SUPERFAMILIA
            FROM Excel_conversor ec
            JOIN Excel_conversor_OK ok ON ec.id_Ecofams = ok.`CODIGO FAMILIA`
        """, dialect="mysql")
        bif['cnkey']  = bif['codProducto'].astype(str).str.strip()
        bif['famEco'] = pd.to_numeric(bif['famEco'], errors='coerce')
        conv['famEco'] = pd.to_numeric(conv['famEco'], errors='coerce')
        bifmap = (bif.merge(conv, on='famEco', how='inner')[['cnkey', 'FAMILIA', 'SUPERFAMILIA']]
                     .rename(columns={'FAMILIA': 'FAM_BIF', 'SUPERFAMILIA': 'SUP_BIF'})
                     .drop_duplicates(subset=['cnkey']))
        print(f"Bifarma family map: {len(bifmap)} cn entries")

        for f in farmacias:
            unit = 233 # test with 10044 -- f["idunit"]
            df = _query_sql(ECOEXTRACT_CONN_ID, f"""
                SELECT
                    a.idArticu    AS cn,
                    a.Descripcion AS descripcion,
                    f.nomFamilia  AS FAMILIA,
                    f.nomSFamilia AS SUPERFAMILIA
                FROM Articu a
                JOIN Fam_SubFam_x_unit f
                    ON a.unit = f.unit AND a.XFam_IdFamilia = f.IdFamilia
                WHERE a.unit = {unit} AND LENGTH(a.idArticu) <> 0
                ORDER BY a.idArticu
            """, dialect="mysql")
            if df.empty:
                print(f"[{unit}] no articles — skipping")
                continue

            # bifarma priority: override the local family where bifarma has one for this cn
            df['cnkey'] = df['cn'].astype(str).str.strip()
            df = df.merge(bifmap, on='cnkey', how='left')
            has_bif = df['FAM_BIF'].notna()
            df.loc[has_bif, 'FAMILIA']      = df.loc[has_bif, 'FAM_BIF']
            df.loc[has_bif, 'SUPERFAMILIA'] = df.loc[has_bif, 'SUP_BIF']
            print(f"[{unit}] {int(has_bif.sum())}/{len(df)} families from bifarma, rest local")

            # tab-separated, CRLF, no header, ISO-8859-1 (Latin-1) — matching the legacy files.
            # Strip any tab/CR/LF embedded in the fields (some descriptions contain them) and
            # join manually so no CSV quoting is introduced — the legacy format is raw delimited.
            cols = ['cn', 'descripcion', 'FAMILIA', 'SUPERFAMILIA']
            clean = df[cols].apply(
                lambda c: c.fillna('').astype(str).str.replace(r'[\t\r\n]+', ' ', regex=True).str.strip()
            )
            lines   = clean.apply('\t'.join, axis=1)
            content = ('\r\n'.join(lines) + '\r\n').encode('latin-1', errors='replace')

            if ENABLE_FTP_UPLOAD:
                remote = f"{ftp_folder}/{unit}.txt"
                with FTPConn(
                    host=str(ftp_cfg["server_ftp"]), user=str(ftp_cfg["user_ftp"]),
                    password=str(ftp_cfg["pass_ftp"]), port=int(ftp_cfg["port_ftp"]),
                    protocol="ftp",
                ) as ftp:
                    ftp.upload_bytes(content, remote)
                dest = f"ftp:{remote}"
            else:
                os.makedirs(LOCAL_TXT_DIR, exist_ok=True)
                local_path = f"{LOCAL_TXT_DIR}/{unit}.txt"
                with open(local_path, "wb") as fh:
                    fh.write(content)
                dest = local_path

            print(f"[{unit}] {len(df)} rows -> {dest}")

    # ── Wire ──────────────────────────────────────────────────
    farmacias    = extract()

    # 1. Ingestion: push each pharmacy's data into ecoextract FIRST, so the source
    #    (Articu / Fam_SubFam_x_unit) is fresh.
    scripts_done = run_scripts(farmacias)

    # 2. Build & ship the per-pharmacy .txt straight from Articu + Fam_SubFam_x_unit.
    per_pharmacy = process_per_pharmacy(farmacias)
    scripts_done >> per_pharmacy

ecofams_etl()
