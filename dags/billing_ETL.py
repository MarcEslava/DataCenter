import pandas as pd
import os
from datetime import date
from dateutil.relativedelta import relativedelta

'''
**************************************************************************************************************************************************************************************
The purpose of this program is the automatization of the process we use to clean the data extracted from BIFarma eco where we have all SO and SI from this year,
this procedure is done monthly so we can get it was necessary to make it more doable.


SI - Sell in (Compres)
SO - Sell out (Ventas)

Author: Marc Eslava
 
**************************************************************************************************************************************************************************************
'''

class clsBiFarmaEco:
    def __init__(self,rappel, period) -> None:
        self.rappel = rappel
        self.period = period
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
        # try:
        self.getFiles(period)
        self.checkAmox()
        self.checkAlmirall()
        self.checkMenaven()
        self.nompikis()
        self.superestalvi()
        self.DataFilters() #dataFilters
        self.noComunesRename()
        self.cleanData()
        self.collateData()
        self.transformNumValue()
        self.addFormulas()
        self.unifyCamps()  # Farmacia change/unify names
        self.addLab()
        self.splitSensilir()
        self.renameColumns()
        self.detectDuplicateValues()
        self.unionSisfarma()
        self.newLabs()
        self.toExcel(period)
        # except Exception as e:
        #     print(f"Error during processing: {e}")
    
    # Get the diferent files we need to run the program
    def getFiles(self, period):
        if period == "": # mes
            self.df_bifarma = pd.read_excel(self._file_ + r"\Bifarma_exports\BIExportGrid_" + self.last_month + r".xlsx")
        elif period == "YTD":
            #exports from bifarma eco rappels cambia
            df = pd.read_excel(self._file_ + r"\Bifarma_exports\BIExportGrid_autocuidado_" + self.last_month + r"_YTD.xlsx")
            df2 = pd.read_excel(self._file_ + r"\Bifarma_exports\BIExportGrid_consejo_" + self.last_month + r"_YTD.xlsx")
            # form the main file
            self.df_bifarma = pd.concat([df,df2], axis=0)
        # Acords file to check labs we have a deal with
        self.df_acords = pd.read_excel(r"Z:\Compres\PowerBI\Power BI EFG\Ecoextract\Data\Acords Ecos 23.xlsx", sheet_name="Bifarma_info")
        #  We get 'NIF', 'BOOK' and 'SOLAR' from this file
        self.df_books = pd.read_excel(r"Z:\Compres\INDÚSTRIA FARMACÈUTICA\BBDD_Book INTERN.xlsx", sheet_name=self.rappel)
        
        self.df_products_master = pd.read_excel(r"Z:\Compres\INDÚSTRIA FARMACÈUTICA\01. CARPETES LABORATORIS\101. Llistat referencies per laboratori i books\Listado referencias por labo y cluster - 2025 YTD.xlsx", sheet_name="Listado Acuerdos")
        # We get 'PVL', 'IVA' and 'RE' from the last month file
        # self.df_bifarma_lastMonth = pd.read_excel(self.rappel_path + r"\Parafarmacia " + self.last_month + r" YTD.xlsx", sheet_name="SI Acord book")
        self.df_bifarma_lastMonth = pd.read_excel(r"Z:\Compres\INDÚSTRIA FARMACÈUTICA\01. CARPETES LABORATORIS\102. Informes i Rappel\Parafarmacia 01.2026.xlsx", sheet_name="SI Acord book")
        
    # Creates de filtered file from the original bifarmaeco file we download, the result has the columns we need from the orginal file with only labs we have an acord with all cleaned. 
    def cleanData(self):
        # if self.filt_codUnif.any() != 0:
        #     self.df_bifarma = self.df_bifarma.drop(index = self.df_bifarma[self.filt_codUnif].index)
        self.df_bifarma =  self.df_bifarma.dropna(axis='index', subset=['Cod Unif'])
        self.df_bifarma.dropna(axis="columns", how = 'all')
        self.df_bifarma = self.df_bifarma.drop(columns=['Var', '% Var','Var.2','% Var.2', 'PVP','Pr. Medio\nAnt', 'Pr. Medio\nAct', 'Var.1', '% Var.1', 'Var.3', '% Var.3', 'SubGrupo', 'Id\nSupFam', 'SuperFamilia', 'Id\nFamilia', 'Familia',' ',' .1',' .2',' .3',' .4',' .5',' .6',' .7',' .8'])
        self.df_acords = self.df_acords.drop(index = self.df_acords[self.filt_isAcord].index)
        self.df_bifarma_filtered = self.df_bifarma[self.filt_accords]
        
    #  Creates the diferent filters we use when cleaning data i/o treating it.
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

    #  We add the columns 'BIFARMA',"NIF","BOOK" and "SOLAR".
    def collateData(self):
        list_books = self.df_books[['BIFARMA',"NIF","BOOK","SOLAR"]]
        self.df_bifarma_books = pd.merge(self.df_bifarma_filtered, list_books,left_on='Nombre Oficina', right_on='BIFARMA', how="left")
        self.df_bifarma_books = self.df_bifarma_books.dropna(axis=0, subset=['BOOK'])
        self.df_bifarma_books = self.df_bifarma_books.drop(columns=['BIFARMA'])
        # print(self.df_bifarma_books['Nombre Oficina'].unique())

    #  SO and SI values come in object format so we transform them to float values to treat them properly.
    def transformNumValue(self):
        columns = ['Venta (Ud)\nAct', 'Venta (Ud)\nAnt',   #this are the columns we need in numeric value
       'Venta (€)\nAct', 'Venta (€)\nAnt', 'Compra (Ud)\nAct',
       'Compra (Ud)\nAnt', 'Compra (€)\nAct', 'Compra (€)\nAnt','Stock\nactual']
        self.df_bifarma_books.loc[:, columns] = self.df_bifarma_books[columns].astype(str).astype(float)
        numeric_columns = self.df_bifarma_books.select_dtypes(include=['number'])
        numeric_columns = numeric_columns.clip(lower=0)
        self.df_bifarma_books.loc[:, numeric_columns.columns] = numeric_columns
        
        
        # We add "Cod Unif",'PVL',"IVA","RE","MARCA"and "SUBMARCA" form the last month information
        self.list_anterior = self.df_bifarma_lastMonth[["Cod Unif",'PVL',"IVA","MARCA","SUBMARCA"]]
        self.list_anterior.drop_duplicates(inplace=True)
        # ---- We need to transform "cod unif" to a numeric value to truncate them ----
        self.df_bifarma_books.loc[:,'Cod Unif'] = self.df_bifarma_books['Cod Unif'].astype(str).str.strip().astype(float)
        self.list_anterior.loc[:,'Cod Unif'] = self.list_anterior['Cod Unif'].astype(str).str.strip().astype(float)
        self.df_bifarma_final = pd.merge(self.df_bifarma_books, self.list_anterior,on="Cod Unif", how="left")
        self.df_bifarma_final = self.df_bifarma_final.drop_duplicates()
        
    #  we add the formulas for "Compra PUC" and the formula for "Compra PVL"
        #  PUC - The price of 1 product "neta"
        #  PVL - The selling price of the lab of 1 unit. "bruta"
    def addFormulas(self):
        self.df_bifarma_final['IVA'] = self.df_bifarma_final['IVA'].astype(str).str.strip().str.replace('#N/D', '0')
        self.df_bifarma_final['PVL'] = self.df_bifarma_final['PVL'].astype(str).str.strip().str.replace('#N/D', '0')
        self.df_bifarma_final['PVL'] = self.df_bifarma_final['PVL'].astype(str).str.strip().str.replace(',', '.')
        self.df_bifarma_final['Compra (Ud)\nAct'] = self.df_bifarma_final['Compra (Ud)\nAct'].astype(str).str.strip().str.replace(',', '.')
        self.df_bifarma_final.loc[:, 'Compra PUC'] = self.df_bifarma_final['Compra (€)\nAct'].astype(float) / (1 + self.df_bifarma_final['IVA'].astype(float)) # hemos quitado el RE de la formula
        self.df_bifarma_final.loc[:,'Compra PVL'] = self.df_bifarma_final['PVL'].astype(float) * self.df_bifarma_final['Compra (Ud)\nAct'].astype(float)
        self.df_bifarma_final['PVL'] = self.df_bifarma_final['PVL'].astype(float)
        self.df_bifarma_final['Compra (Ud)\nAct'] = self.df_bifarma_final['Compra (Ud)\nAct'].astype(float)
        
    # We rename the columns so they are more acurate to their real function, on the future we should change them to a more generic name so we can use the pivot tables.
    def renameColumns(self):
        self.df_bifarma_final.rename(columns={'Venta (Ud)\nAct':'SO (Ud)\nAct','Venta (Ud)\nAnt':'SO (Ud)\nAnt','Venta (€)\nAct':'SO (€)\nAct','Venta (€)\nAnt':'SO (€)\nAnt','Compra (Ud)\nAct':'SI (Ud)\nAct','Compra (Ud)\nAnt':'SI (Ud)\nAnt','Compra (€)\nAct':'SI (€)\nAct','Compra (€)\nAnt':'SI (€)\nAnt'}, inplace=True)
    
    def unifyCamps(self):
        # Viñamata has 2 office we unify them on the same name using the NIF
        self.df_bifarma_final.loc[self.df_bifarma_final['NIF'] == '38793542V', 'Nombre Oficina'] = 'FARMACIA VIÑAMATA'
        self.df_bifarma_final.loc[self.df_bifarma_final['NIF'] == '52150363W', 'Nombre Oficina'] = 'FARMACIA VILA'
        
    #  Creates the resulting excel file we will use to create the reports for the labs
    def toExcel(self, period):
        self.df_bifarma_final.to_excel(self._file_ + r"\output\df_bifarma_output"+ period + ".xlsx",sheet_name="SI Acord book",index = False, header=True, engine='xlsxwriter', na_rep="#N/D")
        print("done!")
    
    def addLab(self):
        for x, row_lab in self.df_bifarma_final.iterrows():
            if row_lab['Laboratorio'] in self.df_acords['BIF'].values:
                self.df_bifarma_final.at[x, 'Laboratorio Categorizado'] = self.df_acords.loc[self.df_acords['BIF'] == row_lab['Laboratorio'], 'Laboratori'].iloc[0]
    def checkAmox (self):
        if  self.df_bifarma['Producto'].str.contains('amoxici').any() or self.df_bifarma['Producto'].str.contains('AMOXICI').any():
            print("ok RJ")
        else:
            print('''
**************************************************************************************************************************************************************************************
*                                                                                                                                                                                    *
* Amox RJ -> Grupo Producto: ESPECIALIDAD; SubGrupo: EFG; Producto: amoxi; Laboratorio: Reig Jofre                                                                                   *
*                                                                                                                                                                                    *
**************************************************************************************************************************************************************************************
''')
            exit()
    def noComunesRename(self):
        if self.filt_codUnif.any():
            unique_labs = self.df_bifarma.loc[self.filt_codUnif, 'Laboratorio'].unique()

            # Generate a unique code for each lab and map it to avoid duplicates
            lab_code_map = {lab: f"999999999{str(i).zfill(4)}" for i, lab in enumerate(unique_labs)}

            # Update the Cod Unif values based on the map
            self.df_bifarma.loc[self.filt_codUnif, 'Cod Unif'] = self.df_bifarma.loc[self.filt_codUnif, 'Laboratorio'].map(lab_code_map)
    def splitSensilir(self):
        # since MARCA is something we create manually we have to remember that this f have some margin error induced by human work
        for x,row_lab in self.df_bifarma_final.iterrows():
            if row_lab['MARCA'] == "SENSILIS" or row_lab['MARCA'] == "COMODYNES" or row_lab['MARCA'] == "AXOVITAL":
                self.df_bifarma_final.at[x,'Laboratorio Categorizado'] = "SENSILIS"
                self.df_bifarma_final.at[x,'Laboratorio'] = "SENSILIS"
    def unionSisfarma(self):
        # since MARCA is something we create manually we have to remember that this f have some margin error induced by human work
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
        # We detect duplicate values in the file
        self.duplicated = self.df_bifarma_final[self.df_bifarma_final[["Cod Unif","Producto","Id Lab","Laboratorio","Farm","Of", "Nombre Oficina"]].duplicated()]
        print(self.duplicated)
        if self.duplicated.empty:
            print("No duplicates found")
        else:
            print("Duplicates found")
    def checkAlmirall(self):
        if  self.df_bifarma['Producto'].str.contains('ESERTIA').any() or self.df_bifarma['Producto'].str.contains('PARAPRES').any():
            print("ok Almirall")
        else:
            print('''
**************************************************************************************************************************************************************************************
*                                                                                                                                                                                    *
* RX Almirall -> Grupo Producto: ESPECIALIDAD; SubGrupo: Especialidad; Laboratorio: Almirall                                                                                         *
*                                                                                                                                                                                    *
**************************************************************************************************************************************************************************************
''')
            exit()
    def checkMenaven(self):
        if  self.df_bifarma['Producto'].str.contains('MENAVEN ').any() :
            print("ok Menarini")
        else:
            print('''
**************************************************************************************************************************************************************************************
*                                                                                                                                                                                    *
* MV Menarini -> Grupo Producto: ESPECIALIDAD; SubGrupo: Especialidad; Producto: menaven                                                                                             *
*                                                                                                                                                                                    *
**************************************************************************************************************************************************************************************
''')
            exit()
    def newLabs(self):
        for x, row_lab in self.df_bifarma_final.iterrows():
            for  row_product_lab in self.df_products_master.iterrows():
                if (row_lab['Cod Unif'] == row_product_lab['EAN'] or row_lab['Cod Unif'] == row_product_lab['CN6']) and (row_lab['PVL'] == None or row_lab['PVL'] == 0):
                    self.df_bifarma_final.at[x, 'PVL'] = row_product_lab['PVL']
                    self.df_bifarma_final.at[x, 'IVA'] = row_product_lab['IVA']
                    self.df_bifarma_final.at[x, 'MARCA'] = row_product_lab['MARCA']
                    self.df_bifarma_final.at[x, 'SUBMARCA'] = "-"
    def superestalvi(self):
        # We check if the lab is in the superestalvi list and we change the name to "SUPERESTALVI"
        superestalvi = [151329, 153335, 171831, 196432, 196433, 214798, 219996, 219997, 260083, 263665, 300293, 395715, 395756]
        for x, row_lab in self.df_bifarma.iterrows():
            if row_lab['Cod Unif'] in superestalvi:
                self.df_bifarma.at[x, 'Laboratorio'] = "SUPERESTALVI"
                 



clsBiFarmaEco("BIFARMA", "") #([bifarma, BIFARMA con BAJAS], [YTD, ""])

