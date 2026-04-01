"""
Calculo Fee ETL

Wraps the existing clsBiFarmaEco logic in an Airflow DAG.

Parameters:
    rappel (str): Book sheet name, e.g. "BIFARMA" or "BIFARMA con BAJAS"
    period (str): "" for monthly, "YTD" for year-to-date
"""
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta

ZOHO_CONN_ID = "zoho_crm"




# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="calculo_fee_ETL",
    description="Monthly BIFarma Eco SI/SO fee calculation ETL",
    schedule=Variable.get("calculo_fee_schedule", default_var="0 6 1 * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "rappel": "BIFARMA",
        "period": "YTD",
    },
    default_args={
        "owner": "data-team",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
)
def calculo_fee_etl():

    @task
    def extract_crm_products() -> list[dict]:
        """Extract products already on CRM to compare with SQL data."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_products = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Products", params={"page": page, "per_page": 200})
                all_products.extend(batch)
            except Exception:
                break
            sleep(0.3)
            page += 1
        print(f"Extracted {len(all_products)} total products from Zoho CRM")
        return all_products

    @task
    def extract_acords() -> list[dict]:
        """Extract vendor/lab mapping from BI (same as novedades_SKU)."""
        from airflow.hooks.base import BaseHook
        from utils.clsSQL import SQLConnection

        _conn = BaseHook.get_connection("biOps_db")
        db = SQLConnection(
            db_host=_conn.host, db_port=_conn.port or 3306,
            db_database=_conn.schema, db_username=_conn.login,
            db_password=_conn.password, dialect="mysql",
        )
        with db:
            df = db.fech_dataframe("SELECT Laboratori, lab_description, BIF_id, date_agreement FROM vendors")
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        print(f"Extracted {len(df)} acords rows")
        return df.to_dict('records')

    @task
    def extract_vendors() -> list[dict]:
        """Extract vendors from Zoho CRM (Fee_Fijo1, Base_Calculo_Bruta_Neta)."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_vendors = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Vendors", params={"page": page, "per_page": 200})
                all_vendors.extend(batch)
            except Exception:
                break
            sleep(0.3)
            page += 1
        print(f"Extracted {len(all_vendors)} vendors from Zoho CRM")
        return all_vendors

    @task
    def extract_accounts() -> list[dict]:
        """Extract pharmacy accounts from Zoho CRM Accounts module."""
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(conn.login, conn.password)
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        all_accounts = []
        page = 0
        while True:
            try:
                batch = crm.fetch_module_data("Accounts", params={"page": page, "per_page": 200})
                all_accounts.extend(batch)
            except Exception:
                break
            sleep(0.3)
            page += 1
        print(f"Extracted {len(all_accounts)} accounts from Zoho CRM")
        return all_accounts

    @task
    def transform(crm_products: list[dict], acords: list[dict], accounts: list[dict], vendors: list[dict], **context):
        import os
        import pandas as pd
        from datetime import date
        from dateutil.relativedelta import relativedelta
        from types import SimpleNamespace
        from utils.clsDate import DateHelper
        from airflow.hooks.base import BaseHook
        from utils.clsSQL import SQLConnection

        rappel = context["params"]["rappel"]
        period = context["params"]["period"]

        # ── Extract products from BIFarma SQL ────────────────────────────────
        # Use last closed month (current month - 1); running on Apr 1 → data for Mar
        d = DateHelper().offset(months=-1)
        curr_yy   = d.anyo
        prev_yy   = d.offset(years=-1).anyo
        curr_year = d.offset(years=-2).anyo
        prev_year = d.offset(years=-2).anyo
        fin_month = str(d.mes).zfill(2)

        GROUP_BY = """
            GROUP BY
                pr.codproducto, pr.desproducto, pr.codlab, pr.deslab,
                de.identidad, de.iddelegacion, de.delegacion,
                pr.idproducto, f.nombresubgrupoproducto,
                pr.idsuperfamilia, f.nombresuperfamiliaeco,
                pr.idfamilia, f.nombrefamiliaeco"""

        BASE_FROM = """
            FROM dbo.bench_dwComprasVentasMesS T1
            INNER JOIN dbo.tme_delegaciones de ON T1.idendeS = de.idendeS
            INNER JOIN dbo.tbi_productosERS pr ON T1.idendeS = pr.idendeS AND T1.idproducto = pr.idproducto
            INNER JOIN dbo.vteco_familias f    ON f.idfamiliaeco = pr.idfamilia"""

        BASE_COLS = """
                pr.codproducto AS CodProducto,
                pr.desproducto AS Producto,
                pr.codlab AS IdLaboratorio,
                pr.deslab AS Laboratorio,
                de.identidad AS IdEntidad,
                de.iddelegacion AS IdDelegacion,
                de.delegacion AS Delegacion,
                pr.idproducto AS IdProducto,
                f.nombresubgrupoproducto AS SubGrupoProducto,
                pr.idsuperfamilia AS IdSuperFamilia,
                f.nombresuperfamiliaeco AS SuperFamilia,
                pr.idfamilia AS IdFamilia,
                f.nombrefamiliaeco AS Familia"""

        ECO_FILTER = "(T1.idendes IN (SELECT idendes FROM tme_delegaciones WHERE grupoCompras = 'ECO'))"

        _conn = BaseHook.get_connection("BIFarma_db")
        db = SQLConnection(
            db_host=_conn.host, db_port=_conn.port or 1433,
            db_database=_conn.schema, db_username=_conn.login,
            db_password=_conn.password, dialect="mssql", driver="pymssql",
        )
        with db:
            act_df = db.fech_dataframe(f"""
                SELECT {BASE_COLS},
                    MIN(pr.stockActual) AS Estoc,
                    SUM(ISNULL(T1.cantidad, 0))       AS CantidadAct,
                    SUM(ISNULL(T1.importe, 0))        AS ImporteAct,
                    SUM(ISNULL(T1.cantidadcompra, 0)) AS CantidadCompraAct,
                    SUM(ISNULL(T1.importecompra, 0))  AS ImporteCompraAct
                {BASE_FROM}
                WHERE T1.anyomes >= {curr_yy}01 AND T1.anyomes <= {curr_yy}{fin_month}
                    AND {ECO_FILTER}
                    AND pr.codLab IN (SELECT idLab FROM BifarmaCentral.dbo.labAcuerdos WHERE anyo = {curr_year})
                {GROUP_BY}""")

            ant_df = db.fech_dataframe(f"""
                SELECT {BASE_COLS},
                    SUM(ISNULL(T1.cantidad, 0))       AS CantidadAnt,
                    SUM(ISNULL(T1.importe, 0))        AS ImporteAnt,
                    SUM(ISNULL(T1.cantidadcompra, 0)) AS CantidadCompraAnt,
                    SUM(ISNULL(T1.importecompra, 0))  AS ImporteCompraAnt
                {BASE_FROM}
                WHERE T1.anyomes >= {prev_yy}01 AND T1.anyomes <= {prev_yy}{fin_month}
                    AND {ECO_FILTER}
                    AND pr.codLab IN (SELECT idLab FROM BifarmaCentral.dbo.labAcuerdos WHERE anyo = {prev_year})
                {GROUP_BY}""")

        MERGE_KEYS = ['CodProducto', 'IdLaboratorio', 'IdEntidad', 'IdDelegacion', 'IdProducto']
        products_df = pd.merge(act_df, ant_df, on=MERGE_KEYS, how='left', suffixes=('', '_ant'))
        for col in ['CantidadAnt', 'ImporteAnt', 'CantidadCompraAnt', 'ImporteCompraAnt']:
            if col not in products_df.columns:
                products_df[col] = 0.0
            products_df[col] = products_df[col].fillna(0.0)

        # Rename SQL columns to match the legacy Excel column names used below
        products_df.rename(columns={
            'CodProducto':       'Cod Unif',
            'IdLaboratorio':     'Id Lab',
            'Delegacion':        'Nombre Oficina',
            'CantidadAct':       'Venta (Ud)\nAct',
            'ImporteAct':        'Venta (€)\nAct',
            'CantidadCompraAct': 'Compra (Ud)\nAct',
            'ImporteCompraAct':  'Compra (€)\nAct',
            'CantidadAnt':       'Venta (Ud)\nAnt',
            'ImporteAnt':        'Venta (€)\nAnt',
            'CantidadCompraAnt': 'Compra (Ud)\nAnt',
            'ImporteCompraAnt':  'Compra (€)\nAnt',
            'Estoc':             'Stock\nactual',
            'SubGrupoProducto':  'SubGrupo',
            'IdSuperFamilia':    'Id\nSupFam',
            'IdFamilia':         'Id\nFamilia',
        }, inplace=True)
        products_df['Cod Nac'] = products_df['Cod Unif']

        print(f"Extracted {len(act_df)} current + {len(ant_df)} previous year rows -> {len(products_df)} merged")
        # ─────────────────────────────────────────────────────────────────────

        s = SimpleNamespace()
        s.rappel               = rappel
        s.period               = period
        s.df_bifarma           = products_df
        s.df_products_master   = pd.DataFrame(crm_products)
        s.df_bifarma_lastMonth = pd.DataFrame(crm_products)
        s.df_acords            = pd.DataFrame(acords)
        s.df_books             = pd.DataFrame(accounts)
        s.df_vendors           = pd.DataFrame(vendors)
        s._file_               = os.path.dirname(os.path.abspath(__file__))
        s.last_year            = format(date.today() - relativedelta(years=1), "%m.%Y")
        s.last_month           = format(date.today() - relativedelta(months=1), "%m.%Y")
        s.rappel_path          = r"Z:\Compres\INDÚSTRIA FARMACÈUTICA\01. CARPETES LABORATORIS\102. Informes i Rappel"

        def checkAmox():
            if s.df_bifarma['Producto'].str.contains('amoxici').any() or s.df_bifarma['Producto'].str.contains('AMOXICI').any():
                print("ok RJ")
            else:
                raise ValueError("Amox RJ missing — add: Grupo Producto=ESPECIALIDAD, SubGrupo=EFG, Producto=amoxi, Laboratorio=Reig Jofre")

        def checkAlmirall():
            if s.df_bifarma['Producto'].str.contains('ESERTIA').any() or s.df_bifarma['Producto'].str.contains('PARAPRES').any():
                print("ok Almirall")
            else:
                raise ValueError("RX Almirall missing — add: Grupo Producto=ESPECIALIDAD, SubGrupo=Especialidad, Laboratorio=Almirall")

        def checkMenaven():
            if s.df_bifarma['Producto'].str.contains('MENAVEN ').any():
                print("ok Menarini")
            else:
                raise ValueError("MV Menarini missing — add: Grupo Producto=ESPECIALIDAD, SubGrupo=Especialidad, Producto=menaven")

        def nompikis():
            for x, row_lab in s.df_bifarma.iterrows():
                if "156119" in str(row_lab['Cod Nac']) or 156119 == row_lab['Cod Nac']:
                    s.df_bifarma.at[x, 'Laboratorio'] = "ECOCEUTICS"

        def superestalvi():
            superestalvi_list = [151329, 153335, 171831, 196432, 196433, 214798, 219996, 219997, 260083, 263665, 300293, 395715, 395756]
            for x, row_lab in s.df_bifarma.iterrows():
                if row_lab['Cod Unif'] in superestalvi_list:
                    s.df_bifarma.at[x, 'Laboratorio'] = "SUPERESTALVI"

        def DataFilters():
            if s.df_bifarma['Cod Unif'].astype(str).str.contains('NOCOMUNES', na=False).any():
                s.filt_codUnif = s.df_bifarma['Cod Unif'].astype(str).str.contains('NOCOMUNES', na=False)
            else:
                s.filt_codUnif = 0
            s.filt_isAcord = s.df_acords['date_agreement'].isna()
            list_acords = s.df_acords['BIF_id'].tolist()
            s.filt_accords = s.df_bifarma['Id Lab'].apply(
                lambda x: any(acord in x for acord in list_acords) if isinstance(x, str) else False
            )

        def noComunesRename():
            if s.filt_codUnif.any():
                unique_labs = s.df_bifarma.loc[s.filt_codUnif, 'Laboratorio'].unique()
                lab_code_map = {lab: f"999999999{str(i).zfill(4)}" for i, lab in enumerate(unique_labs)}
                s.df_bifarma.loc[s.filt_codUnif, 'Cod Unif'] = s.df_bifarma.loc[s.filt_codUnif, 'Laboratorio'].map(lab_code_map)

        def cleanData():

            s.df_bifarma = s.df_bifarma.dropna(axis='index', subset=['Cod Unif'])
            s.df_bifarma.dropna(axis="columns", how='all')
            s.df_bifarma = s.df_bifarma.drop(columns=[
                # Excel-only columns (no longer in SQL source)
                'Var', '% Var', 'Var.1', '% Var.1', 'Var.2', '% Var.2', 'Var.3', '% Var.3',
                'PVP', 'Pr. Medio\nAnt', 'Pr. Medio\nAct',
                'SubGrupo', 'Id\nSupFam', 'SuperFamilia', 'Id\nFamilia', 'Familia',
                ' ', ' .1', ' .2', ' .3', ' .4', ' .5', ' .6', ' .7', ' .8',
                # SQL internal ID columns (IdEntidad/IdDelegacion kept for collateData key, dropped there)
                'IdSuperFamilia', 'IdFamilia',
                'Cod Nac',
                # _ant suffix duplicates from SQL merge (non-key repeated columns)
                'Producto_ant', 'Laboratorio_ant', 'Delegacion_ant',
                'SubGrupoProducto_ant', 'SuperFamilia_ant', 'IdSuperFamilia_ant',
                'IdFamilia_ant', 'Familia_ant',
            ], errors='ignore')
            
            s.df_acords = s.df_acords.drop(index=s.df_acords[s.filt_isAcord].index)
            s.df_bifarma_filtered = s.df_bifarma[s.filt_accords]

        def collateData():
            list_books = s.df_books[['Codigo_Bifarma', 'Identificador_Fiscal_Farmacia', 'Book_General', 'Book_Solares']].copy()
            list_books = list_books.rename(columns={
                'Identificador_Fiscal_Farmacia': 'NIF',
                'Book_General':                 'BOOK',
                'Book_Solares':                 'SOLAR',
            })
            list_books['_key'] = list_books['Codigo_Bifarma'].astype(str).str.strip()
            s.df_bifarma_filtered = s.df_bifarma_filtered.copy()
            s.df_bifarma_filtered['_key'] = (
                s.df_bifarma_filtered['IdEntidad'].astype(str).str.strip() +
                s.df_bifarma_filtered['IdDelegacion'].astype(str).str.strip().str.zfill(2)
            )
            s.df_bifarma_books = pd.merge(s.df_bifarma_filtered, list_books, on='_key', how='left')
            s.df_bifarma_books = s.df_bifarma_books.drop(columns=['_key', 'Codigo_Bifarma', 'IdEntidad', 'IdDelegacion'])
            s.df_bifarma_books = s.df_bifarma_books.dropna(axis=0, subset=['BOOK'])

        def transformNumValue():
            columns = ['Venta (Ud)\nAct', 'Venta (Ud)\nAnt',
                'Venta (€)\nAct', 'Venta (€)\nAnt', 'Compra (Ud)\nAct',
                'Compra (Ud)\nAnt', 'Compra (€)\nAct', 'Compra (€)\nAnt', 'Stock\nactual']
            s.df_bifarma_books.loc[:, columns] = s.df_bifarma_books[columns].astype(str).astype(float)
            numeric_columns = s.df_bifarma_books.select_dtypes(include=['number'])
            numeric_columns = numeric_columns.clip(lower=0)
            s.df_bifarma_books.loc[:, numeric_columns.columns] = numeric_columns
            _lm = s.df_bifarma_lastMonth[['EAN', 'Product_Code', 'Decimal_1', 'IVA2', 'Marca', 'Gama']].copy()
            ean = pd.to_numeric(_lm['EAN'], errors='coerce')
            pc  = pd.to_numeric(_lm['Product_Code'], errors='coerce')
            _lm['Cod Unif'] = ean.where(ean.notna(), pc.where(pc > 150000))
            _lm['PVL']      = _lm['Decimal_1']
            _lm['IVA']      = pd.to_numeric(_lm['IVA2'], errors='coerce') / 100
            _lm['MARCA']    = _lm['Marca']
            _lm['SUBMARCA'] = _lm['Gama']
            s.list_anterior = _lm[["Cod Unif", 'PVL', "IVA", "MARCA", "SUBMARCA"]]
            s.list_anterior.drop_duplicates(inplace=True)
            s.df_bifarma_books.loc[:, 'Cod Unif'] = pd.to_numeric(s.df_bifarma_books['Cod Unif'], errors='coerce')
            s.list_anterior = s.list_anterior.copy()
            s.list_anterior.loc[:, 'Cod Unif'] = pd.to_numeric(s.list_anterior['Cod Unif'], errors='coerce')
            s.df_bifarma_final = pd.merge(s.df_bifarma_books, s.list_anterior, on="Cod Unif", how="left")
            s.df_bifarma_final = s.df_bifarma_final.drop_duplicates()

        def addFormulas():
            s.df_bifarma_final['IVA'] = pd.to_numeric(s.df_bifarma_final['IVA'].astype(str).str.strip().str.replace('#N/D', '').str.replace(',', '.'), errors='coerce').fillna(0)
            s.df_bifarma_final['PVL'] = pd.to_numeric(s.df_bifarma_final['PVL'].astype(str).str.strip().str.replace('#N/D', '').str.replace(',', '.'), errors='coerce').fillna(0)
            s.df_bifarma_final['Compra (Ud)\nAct'] = s.df_bifarma_final['Compra (Ud)\nAct'].astype(str).str.strip().str.replace(',', '.')
            s.df_bifarma_final['PVL'] = s.df_bifarma_final['PVL'].astype(float)
            s.df_bifarma_final['Compra (Ud)\nAct'] = s.df_bifarma_final['Compra (Ud)\nAct'].astype(float)

        def unifyCamps():
            s.df_bifarma_final.loc[s.df_bifarma_final['NIF'] == '38793542V', 'Nombre Oficina'] = 'FARMACIA VIÑAMATA'
            s.df_bifarma_final.loc[s.df_bifarma_final['NIF'] == '52150363W', 'Nombre Oficina'] = 'FARMACIA VILA'

        def addLab():
            for x, row_lab in s.df_bifarma_final.iterrows():
                if row_lab['Id Lab'] in s.df_acords['BIF_id'].values:
                    s.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = s.df_acords.loc[s.df_acords['BIF_id'] == row_lab['Id Lab'], 'Laboratori'].iloc[0]

        def splitSensilir():
            for x, row_lab in s.df_bifarma_final.iterrows():
                if row_lab['MARCA'] == "SENSILIS" or row_lab['MARCA'] == "COMODYNES" or row_lab['MARCA'] == "AXOVITAL":
                    s.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = "SENSILIS"
                    s.df_bifarma_final.at[x, 'Laboratorio'] = "SENSILIS"

        def renameColumns():
            s.df_bifarma_final.rename(columns={
                'Cod Unif':   'Codigo de Producto',
                'IdProducto': 'CN6',
                'Venta (Ud)\nAct': 'SO (Ud)\nAct', 'Venta (Ud)\nAnt': 'SO (Ud)\nAnt',
                'Venta (€)\nAct':  'SO (€)\nAct',  'Venta (€)\nAnt':  'SO (€)\nAnt',
                'Compra (Ud)\nAct':'SI (Ud)\nAct',  'Compra (Ud)\nAnt':'SI (Ud)\nAnt',
                'Compra (€)\nAct': 'SI (€)\nAct',   'Compra (€)\nAnt': 'SI (€)\nAnt',
            }, inplace=True)
            # Move Codigo de Producto and CN6 to the front
            front = ['Codigo de Producto', 'CN6']
            rest  = [c for c in s.df_bifarma_final.columns if c not in front]
            s.df_bifarma_final = s.df_bifarma_final[front + rest]

        def detectDuplicateValues():
            dup_cols = [c for c in ["Cod Unif","Producto","Id Lab","Laboratorio","Farm","Of","Nombre Oficina"] if c in s.df_bifarma_final.columns]
            s.duplicated = s.df_bifarma_final[s.df_bifarma_final[dup_cols].duplicated()]

        def unionSisfarma():
            for x, row_lab in s.df_bifarma_final.iterrows():
                if "elmex" in str(row_lab['Producto']):
                    s.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = "SISFARMA"
                    s.df_bifarma_final.at[x, 'Laboratorio'] = "SISFARMA"

        # ── Run pipeline ──
        checkAmox()
        checkAlmirall()
        checkMenaven()
        nompikis()
        superestalvi()
        DataFilters()
        noComunesRename()
        cleanData()
        collateData()
        transformNumValue()
        addFormulas()
        unifyCamps()
        addLab()
        splitSensilir()
        renameColumns()
        detectDuplicateValues()
        unionSisfarma()
        print(f"Transform complete: {len(s.df_bifarma_final)} rows")

        # ── Send one mail per Laboratorio Categorizado (no XCom) ─────────────
        import io
        from utils.clsZohoMailing import ZohoMailer

        MAX_BYTES = 10 * 1024 * 1024  # 10 MB

        NUM_COLS = [
            'SO (Ud)\nAct', 'SO (€)\nAct', 'SO (Ud)\nAnt', 'SO (€)\nAnt',
            'SI (Ud)\nAct', 'SI (€)\nAct', 'SI (Ud)\nAnt', 'SI (€)\nAnt',
            'Stock\nactual',
        ]
        for _c in NUM_COLS:
            if _c in s.df_bifarma_final.columns:
                s.df_bifarma_final[_c] = pd.to_numeric(s.df_bifarma_final[_c], errors='coerce')

        def _autofit(ws, df, col_fmt):
            for i, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).str.replace('\n', ' ').str.len().max(),
                    len(str(col).replace('\n', ' ')),
                )
                ws.set_column(i, i, min(max_len + 2, 50), col_fmt(col))

        def _write_subtotals(ws, wb, df, group_cols, n_data_rows, col_num_format):
            from xlsxwriter.utility import xl_col_to_name
            group_set = {group_cols} if isinstance(group_cols, str) else set(group_cols)
            total_row_idx = n_data_rows + 1
            wrote_label = False
            for i, c in enumerate(df.columns):
                bold_fmt = wb.add_format({'bold': True, 'num_format': col_num_format(c)})
                if c in group_set or not pd.api.types.is_numeric_dtype(df[c]):
                    if not wrote_label:
                        ws.write(total_row_idx, i, 'TOTALES', wb.add_format({'bold': True}))
                        wrote_label = True
                else:
                    col_letter = xl_col_to_name(i)
                    fn = 101 if ('%' in c or c in ('Fee Fijo', 'Fee Información')) else 109
                    ws.write_formula(
                        total_row_idx, i,
                        f'=SUBTOTAL({fn},{col_letter}2:{col_letter}{n_data_rows + 1})',
                        bold_fmt,
                    )

        def _write_summary_sheet(writer, df_chunk, group_col, sheet_name, col_fmt, col_num_format):
            from xlsxwriter.utility import xl_col_to_name
            agg_cols = [c for c in NUM_COLS if c in df_chunk.columns]
            summary = df_chunk.groupby(group_col, dropna=False)[agg_cols].sum().reset_index()
            if 'NIF' in summary.columns:
                num_s = summary.select_dtypes(include='number').columns.tolist()
                txt_s = [c for c in summary.columns if c not in num_s and c != 'NIF']
                summary = summary.groupby('NIF').agg(
                    {**{c: 'sum' for c in num_s}, **{c: 'first' for c in txt_s}}
                ).reset_index()
                orig_order = [c for c in group_col if c in summary.columns] + \
                             [c for c in agg_cols if c in summary.columns]
                summary = summary[[c for c in orig_order if c in summary.columns]]
            if 'Nombre Oficina' in summary.columns:
                summary = summary.sort_values('Nombre Oficina', ascending=True, na_position='last').reset_index(drop=True)
            has_so = 'SO (€)\nAct' in summary.columns and 'SO (€)\nAnt' in summary.columns
            has_si = 'SI (€)\nAct' in summary.columns and 'SI (€)\nAnt' in summary.columns
            if has_so:
                summary['Crecimiento SO (%)'] = float('nan')
            if has_si:
                summary['Crecimiento SI (%)'] = float('nan')
            summary.to_excel(writer, sheet_name=sheet_name, index=False, na_rep="#N/D")
            ws = writer.sheets[sheet_name]
            n_rows, n_cols = summary.shape
            ws.add_table(0, 0, n_rows, n_cols - 1, {
                'name':    sheet_name.replace(' ', '_'),
                'style':   'Table Style Medium 18',
                'columns': [{'header': c} for c in summary.columns],
            })
            cols = list(summary.columns)
            fmt_pct = writer.book.add_format({'num_format': '0.00%'})
            if has_so:
                act_l = xl_col_to_name(cols.index('SO (€)\nAct'))
                ant_l = xl_col_to_name(cols.index('SO (€)\nAnt'))
                cre_i = cols.index('Crecimiento SO (%)')
                for i in range(n_rows):
                    r = i + 2
                    ws.write_formula(i + 1, cre_i, f'=IF({ant_l}{r}=0,0,({act_l}{r}-{ant_l}{r})/{ant_l}{r})', fmt_pct)
            if has_si:
                act_l = xl_col_to_name(cols.index('SI (€)\nAct'))
                ant_l = xl_col_to_name(cols.index('SI (€)\nAnt'))
                cre_i = cols.index('Crecimiento SI (%)')
                for i in range(n_rows):
                    r = i + 2
                    ws.write_formula(i + 1, cre_i, f'=IF({ant_l}{r}=0,0,({act_l}{r}-{ant_l}{r})/{ant_l}{r})', fmt_pct)
            _write_subtotals(ws, writer.book, summary, group_col, n_rows, col_num_format)
            # overwrite Crecimiento totals with growth formula referencing the € TOTALES cells
            fmt_pct_bold = writer.book.add_format({'num_format': '0.00%', 'bold': True})
            total_r = n_rows + 2  # Excel row number of TOTALES row
            if has_so:
                act_l = xl_col_to_name(cols.index('SO (€)\nAct'))
                ant_l = xl_col_to_name(cols.index('SO (€)\nAnt'))
                cre_i = cols.index('Crecimiento SO (%)')
                ws.write_formula(n_rows + 1, cre_i, f'=IF({ant_l}{total_r}=0,0,({act_l}{total_r}-{ant_l}{total_r})/{ant_l}{total_r})', fmt_pct_bold)
            if has_si:
                act_l = xl_col_to_name(cols.index('SI (€)\nAct'))
                ant_l = xl_col_to_name(cols.index('SI (€)\nAnt'))
                cre_i = cols.index('Crecimiento SI (%)')
                ws.write_formula(n_rows + 1, cre_i, f'=IF({ant_l}{total_r}=0,0,({act_l}{total_r}-{ant_l}{total_r})/{ant_l}{total_r})', fmt_pct_bold)
            _autofit(ws, summary, col_fmt)

        def _send_chunk(df_chunk, lab, safe_name, part_label):
            from xlsxwriter.utility import xl_col_to_name
            buf = io.BytesIO()
            writer = pd.ExcelWriter(buf, engine='xlsxwriter')
            wb = writer.book
            fmt_currency = wb.add_format({'num_format': '#,##0.00 "€"'})
            fmt_pct      = wb.add_format({'num_format': '0.00%'})

            CURRENCY_COLS = ('Importe Fee', 'Importe Información', 'Importe Marketing', 'Compra PUC', 'Compra PVL')
            PCT_COLS      = ('Fee Fijo', 'Fee Información', 'Fee Marketing')

            def _col_fmt(col_name):
                if '€' in col_name or col_name in CURRENCY_COLS or col_name.startswith('Base Calculo'):
                    return fmt_currency
                if '%' in col_name or col_name in PCT_COLS:
                    return fmt_pct
                return None

            def _col_num_format(col_name):
                if '€' in col_name or col_name in CURRENCY_COLS or col_name.startswith('Base Calculo'):
                    return '#,##0.00 "€"'
                if '%' in col_name or col_name in PCT_COLS:
                    return '0.00%'
                return 'General'
            df_chunk.to_excel(writer, sheet_name="SI Acord book", index=False, header=True, na_rep="#N/D")
            ws  = writer.sheets["SI Acord book"]
            cols = list(df_chunk.columns)
            def _cl(name): return xl_col_to_name(cols.index(name))
            # extend cols with calculated columns before building the table
            all_cols = cols + ['Compra PUC', 'Compra PVL']
            n_rows_main = len(df_chunk)
            n_cols_main = len(all_cols)
            df_extended = df_chunk.copy()
            df_extended['Compra PUC'] = pd.to_numeric(df_chunk.get('SI (€)\nAct'), errors='coerce') / (1 + pd.to_numeric(df_chunk.get('IVA'), errors='coerce').fillna(0))
            df_extended['Compra PVL'] = pd.to_numeric(df_chunk.get('PVL'), errors='coerce').fillna(0) * pd.to_numeric(df_chunk.get('SI (Ud)\nAct'), errors='coerce')
            ws.add_table(0, 0, n_rows_main, n_cols_main - 1, {
                'name':    'SI_Acord_book',
                'style':   'Table Style Medium 1',
                'columns': [{'header': c} for c in df_extended.columns],
            })
            # write Excel formulas for Compra PUC / PVL over the data rows
            puc_idx = len(cols)
            pvl_idx = len(cols) + 1
            for i in range(n_rows_main):
                r = i + 2  # Excel row (1=header)
                ws.write_formula(i + 1, puc_idx, f'={_cl("SI (€)\nAct")}{r}/(1+{_cl("IVA")}{r})', _col_fmt('Compra PUC'))
                ws.write_formula(i + 1, pvl_idx, f'={_cl("PVL")}{r}*{_cl("SI (Ud)\nAct")}{r}', _col_fmt('Compra PVL'))
            _write_subtotals(ws, writer.book, df_extended, ['Nombre Oficina'], n_rows_main, _col_num_format)
            _autofit(ws, df_extended, _col_fmt)
            _write_summary_sheet(writer, df_chunk, ['Nombre Oficina', 'NIF', 'BOOK'],                              'Por Farmacia', _col_fmt, _col_num_format)
            _write_summary_sheet(writer, df_chunk, ['Codigo de Producto', 'Producto', 'MARCA', 'SUBMARCA'],        'Por Producto', _col_fmt, _col_num_format)

            # ── Base Fee sheet (quarterly: months 3, 6, 9, 12 only) ─────────────
            if d.mes % 3 == 0:
                _df = df_chunk.copy()
                _df['Compra PUC'] = pd.to_numeric(_df['SI (€)\nAct'], errors='coerce') / (1 + pd.to_numeric(_df['IVA'], errors='coerce').fillna(0))
                _df['Compra PVL'] = pd.to_numeric(_df['PVL'], errors='coerce').fillna(0) * pd.to_numeric(_df['SI (Ud)\nAct'], errors='coerce')
                base_agg = _df.groupby(['Nombre Oficina', 'NIF'], dropna=False)[['Compra PUC', 'Compra PVL']].sum().reset_index()
                acords_ref = s.df_vendors[['BotPlus', 'Base_Calculo_Bruta_Neta', 'Fee_Fijo1', 'Fee_Informaci_n1', 'Fee_Marketing', 'Fee_Fijo_Minimo', 'Fee_Marketing_Minimo']].drop_duplicates('BotPlus').rename(columns={'BotPlus': 'BIF_id'})
                lab_per_farm = _df[['Nombre Oficina', 'Id Lab']].drop_duplicates('Nombre Oficina')
                base_agg = base_agg.merge(lab_per_farm, on='Nombre Oficina', how='left')
                base_agg = base_agg.merge(acords_ref, left_on='Id Lab', right_on='BIF_id', how='left').drop(columns=['BIF_id'], errors='ignore')
                num_ba = base_agg.select_dtypes(include='number').columns.tolist()
                txt_ba = [c for c in base_agg.columns if c not in num_ba and c != 'NIF']
                base_agg = base_agg.groupby('NIF').agg(
                    {**{c: 'sum' for c in num_ba}, **{c: 'first' for c in txt_ba}}
                ).reset_index()
                base_agg['Calculo Bruto/Neto'] = base_agg['Base_Calculo_Bruta_Neta'].apply(
                    lambda v: v if str(v).strip().upper() in ('BRUTA', 'NETA') else 'Bruta'
                )
                is_neta = base_agg['Base_Calculo_Bruta_Neta'].str.strip().str.upper().eq('NETA').any()
                base_calc_col = 'Base Calculo Neto' if is_neta else 'Base Calculo Bruto'
                base_agg[base_calc_col]        = base_agg.apply(
                    lambda r: r['Compra PUC'] if is_neta else r['Compra PVL'],
                    axis=1,
                )
                base_agg['Fee Fijo']            = pd.to_numeric(base_agg['Fee_Fijo1'], errors='coerce').fillna(0) / 100
                base_agg['Importe Fee']         = float('nan')
                base_agg['Fee Información']     = pd.to_numeric(base_agg['Fee_Informaci_n1'], errors='coerce').fillna(0) / 100
                base_agg['Importe Información'] = float('nan')
                base_agg['Fee Marketing']       = pd.to_numeric(base_agg['Fee_Marketing'], errors='coerce').fillna(0) / 100
                base_agg['Importe Marketing']   = float('nan')
                non_group = ['Nombre Oficina', 'NIF', 'Calculo Bruto/Neto']
                base_out = base_agg[['Nombre Oficina', 'NIF', 'Calculo Bruto/Neto', base_calc_col,
                                      'Fee Fijo', 'Importe Fee',
                                      'Fee Información', 'Importe Información',
                                      'Fee Marketing', 'Importe Marketing']]
                base_out.to_excel(writer, sheet_name='Base Fee', index=False, na_rep='#N/D')
                ws_bc = writer.sheets['Base Fee']
                n_r, n_c = base_out.shape
                ws_bc.add_table(0, 0, n_r, n_c - 1, {
                    'name':    'Base_Fee',
                    'style':   'Table Style Medium 2',
                    'columns': [{'header': c} for c in base_out.columns],
                })
                bc_cols = list(base_out.columns)
                bc_l    = xl_col_to_name(bc_cols.index(base_calc_col))
                for imp_col, fee_col in [('Importe Fee', 'Fee Fijo'), ('Importe Información', 'Fee Información'), ('Importe Marketing', 'Fee Marketing')]:
                    imp_i = bc_cols.index(imp_col)
                    fee_l = xl_col_to_name(bc_cols.index(fee_col))
                    fmt   = writer.book.add_format({'num_format': '#,##0.00 "€"'})
                    for i in range(n_r):
                        r = i + 2
                        ws_bc.write_formula(i + 1, imp_i, f'={bc_l}{r}*{fee_l}{r}', fmt)
                _write_subtotals(ws_bc, writer.book, base_out, non_group, n_r, _col_num_format)
                # overwrite Importe totals with minimum-aware formula
                fmt_bold_eur = writer.book.add_format({'num_format': '#,##0.00 "€"', 'bold': True})
                total_row_idx = n_r + 1
                for imp_col, minimo_field in [('Importe Fee', 'Fee_Fijo_Minimo'), ('Importe Marketing', 'Fee_Marketing_Minimo')]:
                    minimo_val = pd.to_numeric(base_agg[minimo_field], errors='coerce').fillna(0).iloc[0]
                    if minimo_val > 0:
                        threshold   = minimo_val / d.mes
                        imp_l = xl_col_to_name(bc_cols.index(imp_col))
                        sub   = f'SUBTOTAL(109,{imp_l}2:{imp_l}{n_r + 1})'
                        ws_bc.write_formula(total_row_idx, bc_cols.index(imp_col),
                                            f'=IF({sub}<{threshold},{minimo_val},{sub})',
                                            fmt_bold_eur)
                _autofit(ws_bc, base_out, _col_fmt)
            writer.sheets["Por Farmacia"].activate()
            writer.sheets["SI Acord book"].hide()

            # Owner from Zoho Vendors (matched by BotPlus == Id Lab)
            lab_ids = _df['Id Lab'].dropna().unique().tolist()
            vendor_row = s.df_vendors[s.df_vendors['BotPlus'].isin(lab_ids)]
            if vendor_row.empty:
                print(f"[{lab}] No vendor match — lab_ids: {lab_ids[:5]}, BotPlus sample: {s.df_vendors['BotPlus'].dropna().unique()[:5].tolist()}")
            if not vendor_row.empty and 'Owner' in vendor_row.columns:
                owner = vendor_row.iloc[0]['Owner']
                if isinstance(owner, dict):
                    owner_name  = owner.get('name', '[Nombre y apellidos]')
                    owner_email = owner.get('email') or ''
                else:
                    owner_name, owner_email = '[Nombre y apellidos]', ''
            else:
                owner_name, owner_email = '[Nombre y apellidos]', ''

            if not owner_email:
                owner_email = 'meslava@ecoceutics.com'

            writer.close()
            size_mb = len(buf.getvalue()) / 1024 / 1024
            print(f"Sending [{lab}{part_label}]: {len(df_chunk)} rows ({size_mb:.1f} MB)")
            html_body = f"""
<p>Estimado/a <b>{lab}</b>,</p>

<p>Nos ponemos en contacto con usted para hacerle llegar la factura correspondiente a los servicios prestados.</p>

<p>Adjuntamos el documento en formato Excel para su revisión y contabilización.</p>

<p>Para cualquier consulta o aclaración, no dude en ponerse en contacto con nosotros a través de este correo electrónico o en el teléfono [número de teléfono].</p>

<p>Agradecemos su confianza y quedamos a su disposición.</p>

<p>Atentamente,<br>
{owner_name}<br>
Category Manager<br>
Hygie31 España<br>
{owner_email}<br>
[Dirección]</p>
"""
            subject = f"[TEST PARA CATEGORY][Calculo Fee] {lab}{part_label} — {rappel} {period or 'mensual'}"
            mailer.send(
                to=[{"address": "meslava@ecoceutics.com", "name": owner_name}],
                subject=subject,
                html_body=html_body,
                attachments=[{"content": buf.getvalue(), "name": f"Parafarmacia {fin_month}.{curr_yy}{' ' + period if period else ''} - {safe_name}{part_label}.xlsx", "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}],
            )

        mailer = ZohoMailer()
        labs = s.df_bifarma_final['Laboratorio Categorizado'].dropna().unique()
        for lab in labs:
            df_lab = s.df_bifarma_final[s.df_bifarma_final['Laboratorio Categorizado'] == lab]
            safe_name = lab.replace("/", "-").replace("\\", "-")

            # probe size with full df first
            probe = io.BytesIO()
            df_lab.to_excel(probe, sheet_name="SI Acord book", index=False, header=True, engine='xlsxwriter', na_rep="#N/D")
            if len(probe.getvalue()) <= MAX_BYTES:
                _send_chunk(df_lab, lab, safe_name, "")
            else:
                chunk_size = 5000
                chunks = [df_lab.iloc[i:i+chunk_size] for i in range(0, len(df_lab), chunk_size)]
                for idx, chunk in enumerate(chunks, 1):
                    _send_chunk(chunk, lab, safe_name, f" ({idx}/{len(chunks)})")

        print(f"Done — {len(labs)} labs processed")

    crm_products = extract_crm_products()
    acords       = extract_acords()
    accounts     = extract_accounts()
    vendors      = extract_vendors()
    transform(crm_products, acords, accounts, vendors)


calculo_fee_etl()