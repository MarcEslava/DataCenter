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
import pandas as pd
import os
from datetime import date
from dateutil.relativedelta import relativedelta

ZOHO_CONN_ID = "zoho_crm"


class clsBiFarmaEco:
    def __init__(self, rappel, period, df_bifarma=None, df_crm_products=None) -> None:
        self.rappel = rappel
        self.period = period
        self.df_bifarma           = df_bifarma
        self.df_products_master   = df_crm_products
        self.df_bifarma_lastMonth = df_crm_products
        self._dir_ = os.getcwd()
        self._file_ = os.path.dirname(os.path.abspath(__file__))
        # Creates the format to get the same date from last year
        self.last_year = date.today() - relativedelta(years=1)
        self.last_year = format(self.last_year,"%m.%Y")
        self.last_month = date.today() - relativedelta(months=1)
        self.last_month = format(self.last_month,"%m.%Y")
        #absolute path to informes i rappel
        self.rappel_path = r"Z:\Compres\INDÚSTRIA FARMACÈUTICA\01. CARPETES LABORATORIS\102. Informes i Rappel"

# ----------FUNCTIONS-------------
        self.getFiles()
        self.checkAmox()
        self.checkAlmirall()
        self.checkMenaven()
        self.nompikis()
        self.superestalvi()
        self.DataFilters()
        self.noComunesRename()
        self.cleanData()
        self.collateData()
        self.transformNumValue()
        self.addFormulas()
        self.unifyCamps()
        self.addLab()
        self.splitSensilir()
        self.renameColumns()
        self.detectDuplicateValues()
        self.unionSisfarma()
        self.toExcel(period)

    def getFiles(self):
        self.df_acords = pd.read_excel(r"Z:\Compres\PowerBI\Power BI EFG\Ecoextract\Data\Acords Ecos 23.xlsx", sheet_name="Bifarma_info")
        self.df_books = pd.read_excel(r"Z:\Compres\INDÚSTRIA FARMACÈUTICA\BBDD_Book INTERN.xlsx", sheet_name=self.rappel)

    def cleanData(self):
        self.df_bifarma =  self.df_bifarma.dropna(axis='index', subset=['Cod Unif'])
        self.df_bifarma.dropna(axis="columns", how = 'all')
        self.df_bifarma = self.df_bifarma.drop(columns=['Var', '% Var','Var.2','% Var.2', 'PVP','Pr. Medio\nAnt', 'Pr. Medio\nAct', 'Var.1', '% Var.1', 'Var.3', '% Var.3', 'SubGrupo', 'Id\nSupFam', 'SuperFamilia', 'Id\nFamilia', 'Familia',' ',' .1',' .2',' .3',' .4',' .5',' .6',' .7',' .8'])
        self.df_acords = self.df_acords.drop(index = self.df_acords[self.filt_isAcord].index)
        self.df_bifarma_filtered = self.df_bifarma[self.filt_accords]

    def DataFilters(self):
        if self.df_bifarma['Cod Unif'].astype(str).str.contains('NOCOMUNES', na=False).any():
            self.filt_codUnif = self.df_bifarma['Cod Unif'].astype(str).str.contains('NOCOMUNES', na=False)
        else:
            self.filt_codUnif= 0
        self.filt_isAcord = self.df_acords['Acord'].str.contains('NO', na=False)
        list_acords = self.df_acords['BIF'].tolist()
        self.filt_accords = self.df_bifarma['Laboratorio'].apply(
            lambda x: any(acord in x for acord in list_acords) if isinstance(x, str) else False
        )

    def collateData(self):
        list_books = self.df_books[['BIFARMA',"NIF","BOOK","SOLAR"]]
        self.df_bifarma_books = pd.merge(self.df_bifarma_filtered, list_books,left_on='Nombre Oficina', right_on='BIFARMA', how="left")
        self.df_bifarma_books = self.df_bifarma_books.dropna(axis=0, subset=['BOOK'])
        self.df_bifarma_books = self.df_bifarma_books.drop(columns=['BIFARMA'])

    def transformNumValue(self):
        columns = ['Venta (Ud)\nAct', 'Venta (Ud)\nAnt',
       'Venta (€)\nAct', 'Venta (€)\nAnt', 'Compra (Ud)\nAct',
       'Compra (Ud)\nAnt', 'Compra (€)\nAct', 'Compra (€)\nAnt','Stock\nactual']
        self.df_bifarma_books.loc[:, columns] = self.df_bifarma_books[columns].astype(str).astype(float)
        numeric_columns = self.df_bifarma_books.select_dtypes(include=['number'])
        numeric_columns = numeric_columns.clip(lower=0)
        self.df_bifarma_books.loc[:, numeric_columns.columns] = numeric_columns
        self.list_anterior = self.df_bifarma_lastMonth[["Cod Unif",'PVL',"IVA","MARCA","SUBMARCA"]]
        self.list_anterior.drop_duplicates(inplace=True)
        self.df_bifarma_books.loc[:,'Cod Unif'] = self.df_bifarma_books['Cod Unif'].astype(str).str.strip().astype(float)
        self.list_anterior.loc[:,'Cod Unif'] = self.list_anterior['Cod Unif'].astype(str).str.strip().astype(float)
        self.df_bifarma_final = pd.merge(self.df_bifarma_books, self.list_anterior,on="Cod Unif", how="left")
        self.df_bifarma_final = self.df_bifarma_final.drop_duplicates()

    def addFormulas(self):
        self.df_bifarma_final['IVA'] = self.df_bifarma_final['IVA'].astype(str).str.strip().str.replace('#N/D', '0')
        self.df_bifarma_final['PVL'] = self.df_bifarma_final['PVL'].astype(str).str.strip().str.replace('#N/D', '0')
        self.df_bifarma_final['PVL'] = self.df_bifarma_final['PVL'].astype(str).str.strip().str.replace(',', '.')
        self.df_bifarma_final['Compra (Ud)\nAct'] = self.df_bifarma_final['Compra (Ud)\nAct'].astype(str).str.strip().str.replace(',', '.')
        self.df_bifarma_final.loc[:, 'Compra PUC'] = self.df_bifarma_final['Compra (€)\nAct'].astype(float) / (1 + self.df_bifarma_final['IVA'].astype(float))
        self.df_bifarma_final.loc[:,'Compra PVL'] = self.df_bifarma_final['PVL'].astype(float) * self.df_bifarma_final['Compra (Ud)\nAct'].astype(float)
        self.df_bifarma_final['PVL'] = self.df_bifarma_final['PVL'].astype(float)
        self.df_bifarma_final['Compra (Ud)\nAct'] = self.df_bifarma_final['Compra (Ud)\nAct'].astype(float)

    def renameColumns(self):
        self.df_bifarma_final.rename(columns={'Venta (Ud)\nAct':'SO (Ud)\nAct','Venta (Ud)\nAnt':'SO (Ud)\nAnt','Venta (€)\nAct':'SO (€)\nAct','Venta (€)\nAnt':'SO (€)\nAnt','Compra (Ud)\nAct':'SI (Ud)\nAct','Compra (Ud)\nAnt':'SI (Ud)\nAnt','Compra (€)\nAct':'SI (€)\nAct','Compra (€)\nAnt':'SI (€)\nAnt'}, inplace=True)

    def unifyCamps(self):
        self.df_bifarma_final.loc[self.df_bifarma_final['NIF'] == '38793542V', 'Nombre Oficina'] = 'FARMACIA VIÑAMATA'
        self.df_bifarma_final.loc[self.df_bifarma_final['NIF'] == '52150363W', 'Nombre Oficina'] = 'FARMACIA VILA'

    def toExcel(self, period):
        self.df_bifarma_final.to_excel(self._file_ + r"\output\df_bifarma_output"+ period + ".xlsx",sheet_name="SI Acord book",index = False, header=True, engine='xlsxwriter', na_rep="#N/D")
        print("done!")

    def addLab(self):
        for x, row_lab in self.df_bifarma_final.iterrows():
            if row_lab['Laboratorio'] in self.df_acords['BIF'].values:
                self.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = self.df_acords.loc[self.df_acords['BIF'] == row_lab['Laboratorio'], 'Laboratori'].iloc[0]

    def checkAmox(self):
        if self.df_bifarma['Producto'].str.contains('amoxici').any() or self.df_bifarma['Producto'].str.contains('AMOXICI').any():
            print("ok RJ")
        else:
            raise ValueError("Amox RJ missing — add: Grupo Producto=ESPECIALIDAD, SubGrupo=EFG, Producto=amoxi, Laboratorio=Reig Jofre")

    def noComunesRename(self):
        if self.filt_codUnif.any():
            unique_labs = self.df_bifarma.loc[self.filt_codUnif, 'Laboratorio'].unique()
            lab_code_map = {lab: f"999999999{str(i).zfill(4)}" for i, lab in enumerate(unique_labs)}
            self.df_bifarma.loc[self.filt_codUnif, 'Cod Unif'] = self.df_bifarma.loc[self.filt_codUnif, 'Laboratorio'].map(lab_code_map)

    def splitSensilir(self):
        for x,row_lab in self.df_bifarma_final.iterrows():
            if row_lab['MARCA'] == "SENSILIS" or row_lab['MARCA'] == "COMODYNES" or row_lab['MARCA'] == "AXOVITAL":
                self.df_bifarma_final.at[x,'Laboratorio Categorizado'] = "SENSILIS"
                self.df_bifarma_final.at[x,'Laboratorio'] = "SENSILIS"

    def unionSisfarma(self):
        for x,row_lab in self.df_bifarma_final.iterrows():
            if "elmex" in str(row_lab['Producto']):
                self.df_bifarma_final.at[x,'Laboratorio Categorizado'] = "SISFARMA"
                self.df_bifarma_final.at[x,'Laboratorio'] = "SISFARMA"

    def __str__(self):
        return f"Rappel: {self.rappel}, Period: {self.period}"

    def nompikis(self):
        for x,row_lab in self.df_bifarma.iterrows():
            if "156119" in str(row_lab['Cod Nac']) or 156119 == row_lab['Cod Nac']:
                self.df_bifarma.at[x,'Laboratorio'] = "ECOCEUTICS"

    def detectDuplicateValues(self):
        self.duplicated = self.df_bifarma_final[self.df_bifarma_final[["Cod Unif","Producto","Id Lab","Laboratorio","Farm","Of", "Nombre Oficina"]].duplicated()]
        print(self.duplicated)
        if self.duplicated.empty:
            print("No duplicates found")
        else:
            print("Duplicates found")

    def checkAlmirall(self):
        if self.df_bifarma['Producto'].str.contains('ESERTIA').any() or self.df_bifarma['Producto'].str.contains('PARAPRES').any():
            print("ok Almirall")
        else:
            raise ValueError("RX Almirall missing — add: Grupo Producto=ESPECIALIDAD, SubGrupo=Especialidad, Laboratorio=Almirall")

    def checkMenaven(self):
        if self.df_bifarma['Producto'].str.contains('MENAVEN ').any():
            print("ok Menarini")
        else:
            raise ValueError("MV Menarini missing — add: Grupo Producto=ESPECIALIDAD, SubGrupo=Especialidad, Producto=menaven")

    def newLabs(self):
        for x, row_lab in self.df_bifarma_final.iterrows():
            for  row_product_lab in self.df_products_master.iterrows():
                if (row_lab['Cod Unif'] == row_product_lab['EAN'] or row_lab['Cod Unif'] == row_product_lab['CN6']) and (row_lab['PVL'] == None or row_lab['PVL'] == 0):
                    self.df_bifarma_final.at[x, 'PVL'] = row_product_lab['PVL']
                    self.df_bifarma_final.at[x, 'IVA'] = row_product_lab['IVA']
                    self.df_bifarma_final.at[x, 'MARCA'] = row_product_lab['MARCA']
                    self.df_bifarma_final.at[x, 'SUBMARCA'] = "-"

    def superestalvi(self):
        superestalvi = [151329, 153335, 171831, 196432, 196433, 214798, 219996, 219997, 260083, 263665, 300293, 395715, 395756]
        for x, row_lab in self.df_bifarma.iterrows():
            if row_lab['Cod Unif'] in superestalvi:
                self.df_bifarma.at[x, 'Laboratorio'] = "SUPERESTALVI"


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
    def run(products: list[dict], crm_products: list[dict], **context):
        import pandas as pd
        rappel         = context["params"]["rappel"]
        period         = context["params"]["period"]
        df_bifarma     = pd.DataFrame(products)
        df_crm         = pd.DataFrame(crm_products)
        clsBiFarmaEco(rappel, period, df_bifarma, df_crm)

    products     = extract_products()
    crm_products = extract_crm_products()
    run(products, crm_products)


calculo_fee_etl()
