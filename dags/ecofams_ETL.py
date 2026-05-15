"""
Ecofams ETL

<Describe what this DAG does>
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SQL_CONN_ID        = "BIFarma_db"        # mssql
BIFARMA_C_CONN_ID  = "bifarmaCentral_db" # mssql — BifarmaCentral
ACORDS_CONN_ID     = "biOps_db"          # mysql
ECOFAMS_CONN_ID    = "ecofams_db"        # ecofams mysql
ECOEXTRACT_CONN_ID = "ecoextract_db"     # ecoextract mysql
ZOHO_CONN_ID       = "zoho_crm"
SSH_CONN_ID        = "ecoextract_ssh"

MAIL_TO = [{"address": "meslava@ecoceutics.com", "name": "Marc Eslava"}]

# ─────────────────────────────────────────────────────────────
# DB helpers
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
        return db.fech_dataframe(sql)

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='ecofams_ETL',
    description='<short description>',
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
        df = _query_sql(SQL_CONN_ID, """
            SELECT id AS idunit, description
            FROM Unit
            WHERE ecoFams = 1 AND ecoBuy = 0
        """, dialect="mssql")
        print(f"Extracted {len(df)} farmacias")
        return df.to_dict("records")

    @task
    def query_ecofams(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = [str(f["idunit"]) for f in farmacias]
        placeholders = ",".join(ids)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT * FROM farmacias
            WHERE idunit IN ({placeholders})
        """, dialect="mysql")
        print(f"Queried ecofams: {len(df)} rows for {len(ids)} farmacias")
        return df.to_dict("records")

    @task
    def query_ecoextract(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOEXTRACT_CONN_ID, f"""
            SELECT
                view_Sinonimo.Sinonimo      AS BarCode,
                Articu.idArticu             AS CN,
                Articu.Descripcion,
                Articu.Laboratorio,
                Articu.XFam_IdFamilia,
                Articu.StockActual,
                Articu.FechaUltimaSalida,
                Articu.unit
            FROM Articu
            LEFT JOIN view_Sinonimo
                ON Articu.IdArticu = view_Sinonimo.IdArticu
                AND Articu.unit    = view_Sinonimo.unit
            WHERE Articu.unit IN ({ids})
        """, dialect="mysql")
        print(f"Queried ecoextract: {len(df)} articles for {len(farmacias)} farmacias")
        return df.to_dict("records")

    @task
    def query_familia_aux(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOEXTRACT_CONN_ID, f"""
            SELECT * FROM FamiliaAux
            WHERE unit IN ({ids})
        """, dialect="mysql")
        print(f"Queried FamiliaAux: {len(df)} rows for {len(farmacias)} farmacias")
        return df.to_dict("records")

    # ── Step 2: Vademecum ─────────────────────────────────────────────────

    @task
    def query_bifarma_products() -> list[dict]:
        df = _query_sql(BIFARMA_C_CONN_ID, """
            SELECT
                codProducto,
                desProducto,
                idLab,
                CASE WHEN efp = 1 THEN 220 ELSE idFamiliaEco END AS idFamiliaEco
            FROM [dbo].[tme_productos]
            WHERE idLab IS NOT NULL
            AND idFamiliaEco IS NOT NULL
        """, dialect="mssql")
        print(f"Queried BifarmaCentral: {len(df)} products")
        return df.to_dict("records")

    @task
    def map_bifarma_to_vademecum(bifarma_products: list[dict]) -> list[dict]:
        import pandas as pd
        if not bifarma_products:
            return []
        df = pd.DataFrame(bifarma_products)
        df = df[df['codProducto'].notna() | df['idLab'].notna()]
        vademecum = pd.DataFrame({
            'id':             None,
            'cn':             df['codProducto'],
            'descripcion':    df['desProducto'],
            'id_laboratorio': df['idLab'],
            'id_familia':     df['idFamiliaEco'],
        })
        print(f"Mapped bifarma products to vademecum: {len(vademecum)} rows")
        return vademecum.to_dict("records")

    @task
    def commit_vademecum(bifarma_vademecum: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not bifarma_vademecum:
            print("No vademecum data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        df = pd.DataFrame(bifarma_vademecum)
        with engine.begin() as cx:
            cx.execute(text("DELETE FROM vademecum"))
            df.to_sql("vademecum", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to vademecum")

    @task
    def query_ftp_config() -> dict:
        df = _query_sql(ECOFAMS_CONN_ID, "SELECT * FROM ftp WHERE id = 1", dialect="mysql")
        if df.empty:
            raise ValueError("No FTP config found in ftp table (id=1)")
        row = df.iloc[0]
        return {
            "server_ftp": row["server_ftp"],
            "port_ftp":   int(row["port_ftp"]),
            "user_ftp":   row["user_ftp"],
            "pass_ftp":   row["pass_ftp"],
            "folder_ftp": row["folder_ftp"],
        }

    # ── Step 3: Novedades ────────────────────────────────────────────────

    @task
    def query_vademecum() -> list[dict]:
        df = _query_sql(ECOFAMS_CONN_ID, "SELECT * FROM vademecum", dialect="mysql")
        print(f"Queried vademecum: {len(df)} rows")
        return df.to_dict("records")

    @task
    def join_artifarma_vademecum(artifarma_data: list[dict], vademecum_data: list[dict]) -> list[dict]:
        import pandas as pd
        if not artifarma_data or not vademecum_data:
            return []
        df_artifarma  = pd.DataFrame(artifarma_data)
        df_vademecum  = pd.DataFrame(vademecum_data)
        merged = pd.merge(
            df_artifarma[['cn', 'unit']],
            df_vademecum[['id', 'cn', 'descripcion', 'id_laboratorio', 'id_familia', 'unit']],
            on='cn',
            how='inner',
            suffixes=('_art', ''),
        )
        result = merged[['id', 'cn', 'descripcion', 'id_laboratorio', 'id_familia', 'unit']].drop_duplicates()
        print(f"Joined artifarma × vademecum: {len(result)} rows")
        return result.to_dict("records")

    @task
    def enrich_novedades(novedades: list[dict], artifarma_data: list[dict]) -> list[dict]:
        import pandas as pd
        if not novedades or not artifarma_data:
            return []
        row5 = pd.DataFrame(novedades)[['id', 'cn']]
        row6 = pd.DataFrame(artifarma_data)[['cn', 'descripcion', 'id_laboratorio', 'id_familia', 'unit', 'BarCode', 'StockActual', 'FechaUltimaSalida']]
        merged = pd.merge(row5, row6, on='cn', how='inner')
        result = merged[['id', 'cn', 'descripcion', 'id_laboratorio', 'id_familia', 'unit', 'BarCode', 'StockActual', 'FechaUltimaSalida']]
        print(f"Enriched novedades: {len(result)} rows")
        return result.to_dict("records")

    @task
    def query_artifarma(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT * FROM artifarma
            WHERE unit IN ({ids})
        """, dialect="mysql")
        print(f"Queried artifarma: {len(df)} rows for {len(farmacias)} farmacias")
        return df.to_dict("records")

    @task
    def build_vademecum(ecoextract_data: list[dict], familia_aux_data: list[dict]) -> list[dict]:
        import pandas as pd
        if not ecoextract_data:
            return []
        row1 = pd.DataFrame(ecoextract_data)
        row2 = pd.DataFrame(familia_aux_data) if familia_aux_data else pd.DataFrame(
            columns=['unit', 'idFamilia', 'IdSuperFamilia']
        )
        merged = pd.merge(
            row1,
            row2[['unit', 'idFamilia', 'IdSuperFamilia']],
            left_on=['unit', 'XFam_IdFamilia'],
            right_on=['unit', 'idFamilia'],
            how='left',
        )
        vademecum = pd.DataFrame({
            'id':               range(1, len(merged) + 1),
            'cn':               merged['CN'],
            'BarCode':          merged['BarCode'],
            'descripcion':      merged['Descripcion'],
            'id_laboratorio':   merged['Laboratorio'],
            'id_familia':       merged['XFam_IdFamilia'],
            'IdSuperFamilia':   merged['IdSuperFamilia'],
            'unit':             merged['unit'],
            'StockActual':      merged['StockActual'],
            'FechaUltimaSalida': merged['FechaUltimaSalida'],
        })
        print(f"Built vademecum: {len(vademecum)} rows")
        return vademecum.to_dict("records")

    @task
    def commit_artifarma(vademecum: list[dict], farmacias: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not vademecum:
            print("No vademecum data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df  = pd.DataFrame(vademecum)
        with engine.begin() as cx:
            cx.execute(text(f"DELETE FROM artifarma WHERE unit IN ({ids})"))
            df.to_sql("artifarma", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to artifarma for units: {ids}")

    @task
    def query_vademecum_salidas(farmacias: list[dict]) -> list[dict]:
        import pandas as pd
        if not farmacias:
            return []
        df = _query_sql(ECOFAMS_CONN_ID,
            "SELECT id, cn, descripcion, id_familia FROM vademecum",
            dialect="mysql")
        if df.empty:
            return []
        frames = [df.assign(unit=f["idunit"]) for f in farmacias]
        result = pd.concat(frames, ignore_index=True)
        print(f"Queried vademecum for salidas: {len(result)} rows for {len(farmacias)} farmacias")
        return result.to_dict("records")

    @task
    def query_salidas(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT A.*
            FROM artifarma AS A
            INNER JOIN vademecum AS B ON A.cn = B.cn
            WHERE A.unit IN ({ids})
            AND LENGTH(A.cn) <> 0
        """, dialect="mysql")
        print(f"Queried salidas: {len(df)} rows for {len(farmacias)} farmacias")
        return df.to_dict("records")

    @task
    def query_barcode_candidates(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT
                A.cn,
                A.descrip_farma,
                A.descrip_vade,
                A.Diff,
                B.BarCode
            FROM Articulos_a_eliminar2 AS A
            INNER JOIN artifarma AS B ON A.cn = B.cn
            WHERE A.Diff = '0'
            AND LENGTH(B.BarCode) >= 8
            AND B.unit IN ({ids})
            AND (
                ((SELECT YEAR(C.FechaUltimaSalida)
                  FROM ecoextract.Articu AS C
                  WHERE C.IdArticu = A.cn LIMIT 1) BETWEEN YEAR(NOW()) - 1 AND YEAR(NOW()))
                OR
                ((SELECT C.StockActual
                  FROM ecoextract.Articu AS C
                  WHERE C.IdArticu = A.cn LIMIT 1) <> 0)
            )
        """, dialect="mysql")
        print(f"Queried barcode candidates: {len(df)} rows")
        return df.to_dict("records")

    @task
    def commit_articulos_eliminar2(diff_data: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not diff_data:
            print("No diff data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        df = pd.DataFrame(diff_data)
        with engine.begin() as cx:
            cx.execute(text("DELETE FROM Articulos_a_eliminar2"))
            df.to_sql("Articulos_a_eliminar2", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to Articulos_a_eliminar2")

    @task
    def query_articulos_revision() -> list[dict]:
        df = _query_sql(ECOFAMS_CONN_ID, """
            SELECT
                A.cn,
                A.var1,
                A.descripcion_farma,
                A.descripcion_vade,
                REPLACE(CONVERT(A.descripcion_vade, CHAR), '.', ',') AS Dif
            FROM Articulos_a_eliminar AS A
            LEFT JOIN ecoextract.Articu AS B
                ON A.cn = B.IdArticu AND B.unit = 21
            WHERE YEAR(B.FechaUltimaSalida) >= YEAR(NOW()) - 1
            AND B.StockActual <> 0
            AND IFNULL(A.BarCode, 0) = 0
            ORDER BY B.IdArticu DESC
        """, dialect="mysql")
        print(f"Queried articulos revision: {len(df)} rows")
        return df.to_dict("records")

    @task
    def compute_diff(articulos_revision: list[dict]) -> list[dict]:
        import difflib
        if not articulos_revision:
            return []

        def boyer_moore_count(word: str, text: str) -> int:
            return text.upper().count(word.upper()) if word and text else 0

        def levenshtein_similarity(s1: str, s2: str) -> float:
            return difflib.SequenceMatcher(None, s1.upper(), s2.upper()).ratio()

        results = []
        for row in articulos_revision:
            desc_farma = row.get('descripcion_farma') or ''
            desc_vade  = row.get('descripcion_vade') or ''
            diff = row.get('Dif', '')
            if desc_farma and desc_vade:
                b = sum(boyer_moore_count(w, desc_vade) for w in (desc_farma + ' ').split(' ') if w)
                sim_pct = round(levenshtein_similarity(desc_farma, desc_vade) * 100, 2)
                diff = str(sim_pct) if (sim_pct > 25 and b > 2) else '0'
            results.append({
                'cn':           row['cn'],
                'descrip_farma': desc_farma,
                'descrip_vade':  desc_vade,
                'Diff':          diff,
            })
        print(f"Computed diff for {len(results)} articulos")
        return results

    @task
    def commit_articulos_eliminar(resultados_enriched: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not resultados_enriched:
            print("No resultados data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        df = pd.DataFrame(resultados_enriched)
        with engine.begin() as cx:
            cx.execute(text("DELETE FROM Articulos_a_eliminar"))
            df.to_sql("Articulos_a_eliminar", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to Articulos_a_eliminar")

    @task
    def enrich_resultados(resultados_definitivos: list[dict], vademecum_filtered: list[dict]) -> list[dict]:
        import pandas as pd
        if not resultados_definitivos or not vademecum_filtered:
            return []

        def filtro_words(desc_vade, desc_farma) -> int:
            if not desc_vade or not desc_farma:
                return 0
            words = str(desc_vade).upper().split()
            target = str(desc_farma).upper()
            return sum(1 for w in words if w in target)

        row1 = pd.DataFrame(resultados_definitivos)[['cn', 'descripcion', 'BarCode']]
        row3 = pd.DataFrame(vademecum_filtered)[['cn', 'descripcion']]
        merged = pd.merge(row1, row3, on='cn', how='inner', suffixes=('_farma', '_vade'))
        merged['var1'] = merged.apply(
            lambda r: filtro_words(r['descripcion_vade'], r['descripcion_farma']), axis=1
        )
        result = merged[merged['var1'] > 0][['cn', 'var1', 'descripcion_farma', 'descripcion_vade', 'BarCode']]
        print(f"Enriched resultados: {len(result)} rows after filtroWords filter")
        return result.to_dict("records")

    @task
    def query_vademecum_filtered() -> list[dict]:
        df = _query_sql(ECOFAMS_CONN_ID, """
            SELECT * FROM vademecum
            WHERE id_familia <> 99
        """, dialect="mysql")
        print(f"Queried vademecum (id_familia<>99): {len(df)} rows")
        return df.to_dict("records")

    @task
    def query_resultados_definitivos(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT
                farmacia_txt.id,
                farmacia_txt.cn,
                farmacia_txt.descripcion,
                farmacia_txt.unit,
                farmacia_txt.FAMILIA,
                farmacia_txt.SUPERFAMILIA,
                farmacia_txt.BarCode
            FROM farmacia_txt
            WHERE farmacia_txt.unit IN ({ids})
        """, dialect="mysql")
        print(f"Queried resultados definitivos: {len(df)} rows for {len(farmacias)} farmacias")
        return df.to_dict("records")

    @task
    def commit_farmacia_txt(resultados_data: list[dict], farmacias: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not resultados_data:
            print("No resultados data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df  = pd.DataFrame(resultados_data)
        with engine.begin() as cx:
            cx.execute(text(f"DELETE FROM farmacia_txt WHERE unit IN ({ids})"))
            df.to_sql("farmacia_txt", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to farmacia_txt for units: {ids}")

    @task
    def query_resultados(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT
                salida.id,
                salida.cn,
                salida.descripcion,
                salida.unit,
                Excel_conversor_OK.FAMILIA,
                Excel_conversor_OK.SUPERFAMILIA,
                salida.BarCode
            FROM salida
            LEFT JOIN Excel_conversor
                ON salida.familia = Excel_conversor.idfamilia_Bifarmaeco
            LEFT JOIN Excel_conversor_OK
                ON Excel_conversor.id_Ecofams = Excel_conversor_OK.`CODIGO FAMILIA`
            WHERE salida.unit IN ({ids})
        """, dialect="mysql")
        print(f"Queried resultados: {len(df)} rows for {len(farmacias)} farmacias")
        return df.to_dict("records")

    @task
    def commit_salidas(salidas_built: list[dict], farmacias: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not salidas_built:
            print("No salidas data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df  = pd.DataFrame(salidas_built)
        with engine.begin() as cx:
            cx.execute(text(f"DELETE FROM salida WHERE unit IN ({ids})"))
            df.to_sql("salida", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to salida for units: {ids}")

    @task
    def build_salidas(salidas_joined: list[dict], artifarma_salidas: list[dict]) -> list[dict]:
        import pandas as pd
        if not salidas_joined or not artifarma_salidas:
            return []
        row2 = pd.DataFrame(salidas_joined)[['id', 'cn', 'id_familia', 'unit']]
        row6 = pd.DataFrame(artifarma_salidas)[['cn', 'unit', 'BarCode', 'descripcion', 'IdSuperFamilia']]
        merged = pd.merge(row2, row6, on=['cn', 'unit'], how='inner')
        merged['subfamilia'] = pd.to_numeric(merged['IdSuperFamilia'], errors='coerce').astype('Int16')
        result = merged[['id', 'cn', 'descripcion', 'unit', 'id_familia', 'subfamilia', 'BarCode']].rename(
            columns={'id_familia': 'familia'}
        )
        print(f"Built salidas: {len(result)} rows")
        return result.to_dict("records")

    @task
    def query_categorias() -> list[dict]:
        df = _query_sql(ECOFAMS_CONN_ID, "SELECT * FROM categorias", dialect="mysql")
        print(f"Queried categorias: {len(df)} rows")
        return df.to_dict("records")

    @task
    def query_artifarma_salidas(farmacias: list[dict]) -> list[dict]:
        if not farmacias:
            return []
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df = _query_sql(ECOFAMS_CONN_ID, f"""
            SELECT id, cn, BarCode, descripcion, id_familia, idSuperFamilia, unit
            FROM artifarma
            WHERE unit IN ({ids})
            AND LENGTH(cn) <> 0
        """, dialect="mysql")
        print(f"Queried artifarma for salidas: {len(df)} rows for {len(farmacias)} farmacias")
        return df.to_dict("records")

    @task
    def join_salidas_vademecum(salidas_data: list[dict], vademecum_salidas: list[dict]) -> list[dict]:
        import pandas as pd
        if not salidas_data or not vademecum_salidas:
            return []
        df_salidas   = pd.DataFrame(salidas_data)[['cn']]
        df_vademecum = pd.DataFrame(vademecum_salidas)[['id', 'cn', 'descripcion', 'id_familia', 'unit']]
        merged = pd.merge(df_salidas, df_vademecum, on='cn', how='inner')
        result = merged[['id', 'cn', 'descripcion', 'id_familia', 'unit']].drop_duplicates()
        print(f"Joined salidas × vademecum: {len(result)} rows")
        return result.to_dict("records")

    @task
    def commit_novedades(novedades_enriched: list[dict], farmacias: list[dict]) -> None:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from airflow.hooks.base import BaseHook
        if not novedades_enriched:
            print("No novedades data to commit")
            return
        conn   = BaseHook.get_connection(ECOFAMS_CONN_ID)
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
        )
        ids = ",".join(str(f["idunit"]) for f in farmacias)
        df  = pd.DataFrame(novedades_enriched)
        with engine.begin() as cx:
            cx.execute(text(f"DELETE FROM tbl_novedades WHERE unit IN ({ids})"))
            df.to_sql("tbl_novedades", cx, if_exists="append", index=False)
        print(f"Committed {len(df)} rows to tbl_novedades for units: {ids}")

    @task
    def run_scripts(farmacias: list[dict]) -> None:
        import paramiko
        from airflow.hooks.base import BaseHook
        if not farmacias:
            print("No farmacias to process")
            return
        conn = BaseHook.get_connection(SSH_CONN_ID)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=conn.host,
            port=conn.port or 22,
            username=conn.login,
            password=conn.password or None,
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

    # ── Wire ──────────────────────────────────────────────────
    farmacias             = extract()
    ftp_config            = query_ftp_config()
    bifarma_products      = query_bifarma_products()
    bifarma_vademecum     = map_bifarma_to_vademecum(bifarma_products)
    vademecum_committed   = commit_vademecum(bifarma_vademecum)
    ecofams_data          = query_ecofams(farmacias)
    ecoextract_data       = query_ecoextract(farmacias)
    familia_aux_data      = query_familia_aux(farmacias)
    vademecum             = build_vademecum(ecoextract_data, familia_aux_data)
    committed             = commit_artifarma(vademecum, farmacias)
    vademecum_data        = query_vademecum()
    artifarma_data        = query_artifarma(farmacias)
    committed >> artifarma_data
    vademecum_committed >> vademecum_data
    novedades             = join_artifarma_vademecum(artifarma_data, vademecum_data)
    categorias_data       = query_categorias()
    salidas_data          = query_salidas(farmacias)
    vademecum_salidas     = query_vademecum_salidas(farmacias)
    artifarma_salidas     = query_artifarma_salidas(farmacias)
    [committed, vademecum_committed] >> salidas_data
    vademecum_committed >> vademecum_salidas
    committed >> artifarma_salidas
    salidas_joined        = join_salidas_vademecum(salidas_data, vademecum_salidas)
    salidas_built         = build_salidas(salidas_joined, artifarma_salidas)
    salidas_committed     = commit_salidas(salidas_built, farmacias)
    resultados_data       = query_resultados(farmacias)
    salidas_committed >> resultados_data
    farmacia_txt_committed  = commit_farmacia_txt(resultados_data, farmacias)
    resultados_definitivos  = query_resultados_definitivos(farmacias)
    farmacia_txt_committed >> resultados_definitivos
    vademecum_filtered      = query_vademecum_filtered()
    vademecum_committed >> vademecum_filtered
    resultados_enriched     = enrich_resultados(resultados_definitivos, vademecum_filtered)
    articulos_eliminados    = commit_articulos_eliminar(resultados_enriched)
    articulos_revision      = query_articulos_revision()
    articulos_eliminados >> articulos_revision
    diff_data               = compute_diff(articulos_revision)
    commit_articulos_eliminar2(diff_data)
    novedades_enriched    = enrich_novedades(novedades, artifarma_data)
    novedades_committed   = commit_novedades(novedades_enriched, farmacias)
    [ecofams_data, novedades_committed, ftp_config] >> run_scripts(farmacias)

ecofams_etl()