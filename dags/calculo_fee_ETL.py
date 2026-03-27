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
        "period": "",
    },
    default_args={
        "owner": "data-team",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
)
def calculo_fee_etl():

    @task
    def extract_products() -> list[dict]:
        """Extract products/sales for current and previous year."""
        import pandas as pd
        from utils.clsDate import DateHelper
        from airflow.hooks.base import BaseHook
        from utils.clsSQL import SQLConnection
        try:
            d = DateHelper()
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
                db_host=_conn.host,
                db_port=_conn.port or 1433,
                db_database=_conn.schema,
                db_username=_conn.login,
                db_password=_conn.password,
                dialect="mssql",
                driver="pymssql",
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

            print(f"Extracted {len(act_df)} current + {len(ant_df)} previous year rows -> {len(products_df)} merged")
            return products_df.to_dict('records')
        except Exception as e:
            print(f"Error extracting products: {e}")
            raise e
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
            df = db.fech_dataframe("SELECT * FROM VendorMapping")
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        print(f"Extracted {len(df)} acords rows")
        return df.to_dict('records')

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
    def transform(products: list[dict], crm_products: list[dict], acords: list[dict], accounts: list[dict], **context):
        import os
        import pandas as pd
        from datetime import date
        from dateutil.relativedelta import relativedelta
        from types import SimpleNamespace

        rappel = context["params"]["rappel"]
        period = context["params"]["period"]

        s = SimpleNamespace()
        s.rappel               = rappel
        s.period               = period
        s.df_bifarma           = pd.DataFrame(products)
        s.df_products_master   = pd.DataFrame(crm_products)
        s.df_bifarma_lastMonth = pd.DataFrame(crm_products)
        s.df_acords            = pd.DataFrame(acords)
        s.df_books             = pd.DataFrame(accounts)
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
            s.filt_isAcord = s.df_acords['Acord'].str.contains('NO', na=False)
            list_acords = s.df_acords['BIF'].tolist()
            s.filt_accords = s.df_bifarma['Laboratorio'].apply(
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
            s.df_bifarma = s.df_bifarma.drop(columns=['Var', '% Var','Var.2','% Var.2', 'PVP','Pr. Medio\nAnt', 'Pr. Medio\nAct', 'Var.1', '% Var.1', 'Var.3', '% Var.3', 'SubGrupo', 'Id\nSupFam', 'SuperFamilia', 'Id\nFamilia', 'Familia',' ',' .1',' .2',' .3',' .4',' .5',' .6',' .7',' .8'])
            s.df_acords = s.df_acords.drop(index=s.df_acords[s.filt_isAcord].index)
            s.df_bifarma_filtered = s.df_bifarma[s.filt_accords]

        def collateData():
            list_books = s.df_books[['BIFARMA', "NIF", "BOOK", "SOLAR"]]
            s.df_bifarma_books = pd.merge(s.df_bifarma_filtered, list_books, left_on='Nombre Oficina', right_on='BIFARMA', how="left")
            s.df_bifarma_books = s.df_bifarma_books.dropna(axis=0, subset=['BOOK'])
            s.df_bifarma_books = s.df_bifarma_books.drop(columns=['BIFARMA'])

        def transformNumValue():
            columns = ['Venta (Ud)\nAct', 'Venta (Ud)\nAnt',
                'Venta (€)\nAct', 'Venta (€)\nAnt', 'Compra (Ud)\nAct',
                'Compra (Ud)\nAnt', 'Compra (€)\nAct', 'Compra (€)\nAnt', 'Stock\nactual']
            s.df_bifarma_books.loc[:, columns] = s.df_bifarma_books[columns].astype(str).astype(float)
            numeric_columns = s.df_bifarma_books.select_dtypes(include=['number'])
            numeric_columns = numeric_columns.clip(lower=0)
            s.df_bifarma_books.loc[:, numeric_columns.columns] = numeric_columns
            s.list_anterior = s.df_bifarma_lastMonth[["Cod Unif", 'PVL', "IVA", "MARCA", "SUBMARCA"]]
            s.list_anterior.drop_duplicates(inplace=True)
            s.df_bifarma_books.loc[:, 'Cod Unif'] = s.df_bifarma_books['Cod Unif'].astype(str).str.strip().astype(float)
            s.list_anterior.loc[:, 'Cod Unif'] = s.list_anterior['Cod Unif'].astype(str).str.strip().astype(float)
            s.df_bifarma_final = pd.merge(s.df_bifarma_books, s.list_anterior, on="Cod Unif", how="left")
            s.df_bifarma_final = s.df_bifarma_final.drop_duplicates()

        def addFormulas():
            s.df_bifarma_final['IVA'] = s.df_bifarma_final['IVA'].astype(str).str.strip().str.replace('#N/D', '0')
            s.df_bifarma_final['PVL'] = s.df_bifarma_final['PVL'].astype(str).str.strip().str.replace('#N/D', '0')
            s.df_bifarma_final['PVL'] = s.df_bifarma_final['PVL'].astype(str).str.strip().str.replace(',', '.')
            s.df_bifarma_final['Compra (Ud)\nAct'] = s.df_bifarma_final['Compra (Ud)\nAct'].astype(str).str.strip().str.replace(',', '.')
            s.df_bifarma_final.loc[:, 'Compra PUC'] = s.df_bifarma_final['Compra (€)\nAct'].astype(float) / (1 + s.df_bifarma_final['IVA'].astype(float))
            s.df_bifarma_final.loc[:, 'Compra PVL'] = s.df_bifarma_final['PVL'].astype(float) * s.df_bifarma_final['Compra (Ud)\nAct'].astype(float)
            s.df_bifarma_final['PVL'] = s.df_bifarma_final['PVL'].astype(float)
            s.df_bifarma_final['Compra (Ud)\nAct'] = s.df_bifarma_final['Compra (Ud)\nAct'].astype(float)

        def unifyCamps():
            s.df_bifarma_final.loc[s.df_bifarma_final['NIF'] == '38793542V', 'Nombre Oficina'] = 'FARMACIA VIÑAMATA'
            s.df_bifarma_final.loc[s.df_bifarma_final['NIF'] == '52150363W', 'Nombre Oficina'] = 'FARMACIA VILA'

        def addLab():
            for x, row_lab in s.df_bifarma_final.iterrows():
                if row_lab['Laboratorio'] in s.df_acords['BIF'].values:
                    s.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = s.df_acords.loc[s.df_acords['BIF'] == row_lab['Laboratorio'], 'Laboratori'].iloc[0]

        def splitSensilir():
            for x, row_lab in s.df_bifarma_final.iterrows():
                if row_lab['MARCA'] == "SENSILIS" or row_lab['MARCA'] == "COMODYNES" or row_lab['MARCA'] == "AXOVITAL":
                    s.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = "SENSILIS"
                    s.df_bifarma_final.at[x, 'Laboratorio'] = "SENSILIS"

        def renameColumns():
            s.df_bifarma_final.rename(columns={'Venta (Ud)\nAct':'SO (Ud)\nAct','Venta (Ud)\nAnt':'SO (Ud)\nAnt','Venta (€)\nAct':'SO (€)\nAct','Venta (€)\nAnt':'SO (€)\nAnt','Compra (Ud)\nAct':'SI (Ud)\nAct','Compra (Ud)\nAnt':'SI (Ud)\nAnt','Compra (€)\nAct':'SI (€)\nAct','Compra (€)\nAnt':'SI (€)\nAnt'}, inplace=True)

        def detectDuplicateValues():
            s.duplicated = s.df_bifarma_final[s.df_bifarma_final[["Cod Unif","Producto","Id Lab","Laboratorio","Farm","Of", "Nombre Oficina"]].duplicated()]
            print(s.duplicated)
            if s.duplicated.empty:
                print("No duplicates found")
            else:
                print("Duplicates found")

        def unionSisfarma():
            for x, row_lab in s.df_bifarma_final.iterrows():
                if "elmex" in str(row_lab['Producto']):
                    s.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = "SISFARMA"
                    s.df_bifarma_final.at[x, 'Laboratorio'] = "SISFARMA"

        def toExcel():
            s.df_bifarma_final.to_excel(s._file_ + r"\output\df_bifarma_output" + period + ".xlsx", sheet_name="SI Acord book", index=False, header=True, engine='xlsxwriter', na_rep="#N/D")
            print("done!")

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
        toExcel()

    products     = extract_products()
    crm_products = extract_crm_products()
    acords       = extract_acords()
    accounts     = extract_accounts()
    transform(products, crm_products, acords, accounts)


calculo_fee_etl()
