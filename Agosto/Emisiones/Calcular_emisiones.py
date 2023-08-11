import pandas as pd
import numpy as np
import tkinter as tk
import time
from tkinter import filedialog, messagebox, simpledialog, Tk
import openpyxl


def mostrar_mensaje(titulo, mensaje):
    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo(title=titulo, message=mensaje)
    root.destroy()

def pedir_archivo(titulo):
    root = tk.Tk()
    root.withdraw()
    archivo_path = filedialog.askopenfilename(title=titulo, filetypes=[('CSV Files', '*.csv')])
    root.destroy()  # Cerramos la ventana después de obtener el archivo
    return archivo_path

# Definir la lista de palabras a mantener en minúsculas
palabras_mantener_min = ['y', 'de', 'por']  # Agrega aquí las palabras que desees mantener en minúsculas

# Diccionario de coincidencias de nombres invertido para transformar regional_total
coincidencias = {
    'Coque Metalúrgico': 'Coque Mineral',
    'Gas de Alto Horno': 'Gas Altos Hornos',
    'Gas Licuado de Petróleo': 'Gas Licuado',
    'Derivados Industriales de Petróleo': 'D.I. de Petróleo',
    'Energía Hidroeléctrica': 'Energia Hídrica',
    'Gas de Coque': 'Gas Coque',
    'Petróleo Diésel': 'Diesel',
    'Gasolina de Aviación': 'Gasolina Aviacion',
    'Kerosene de Aviación': 'Kerosene Aviacion'
}
sectores = {
    'Comercial, público y residencial': 'Sector Cmrcl., Púb. Y Residencial',
     'Energía': 'Energético: Consumo Propio',
     'Industria y Minería': 'Industrial y Minero',
     'Transformación': 'Centros de transfornación',
     }
# Función para transformar los nombres en regional_total según el diccionario de coincidencias
def transformar_nombres(row):
    combustible = row['Energético']
    if combustible in coincidencias:
        return coincidencias[combustible]
    return combustible


def transformar_sector(row):
    sector = row['Sector']
    if sector in sectores:
        return sectores[sector]
    return sector

def procesar_BNE_regional(archivo,año):
    regional = pd.read_excel(archivo,sheet_name='BNE regional')
    año = int(año)
    regional_orden = regional.query('(anio == @año) and ((actividad != "Transformación") or (actividad == "Transformación" and subactividad == "Electricidad Servicio Público"))')[['nombre_region','cod_region','actividad','subactividad','energetico','tcal']]
    regional_orden.columns = ['nombre_region','cod_region','Sector','Subsector','Energético','Tcal BNE']

    regional_orden['Energético'] = regional_orden.apply(transformar_nombres, axis=1)
    regional_orden['Sector'] = regional_orden.apply(transformar_sector, axis=1)
    
    return regional_orden

def romano_a_entero(romano):
    valores = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
               'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
               'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15, 'XVI': 16}
    return valores.get(romano)

# Función personalizada para capitalizar las palabras, excepto las que están en la lista
def capitalizar_excepto_palabras_min(palabra):
    palabras = palabra.split()
    palabras_capitalizadas = [palabra.capitalize() if palabra.lower() not in palabras_mantener_min else palabra.lower() for palabra in palabras]
    return ' '.join(palabras_capitalizadas)

def calcular_emisiones_regionales(archivo,año,datos_SEC_regional):
    #Carga de datos
    Datos_SEC = datos_SEC_regional.copy()
    Datos_SEC = Datos_SEC.rename(columns={'Energia estimada': 'SEC Unid. Físicas'})
    Datos_SEC['cod_region'] = Datos_SEC['Region'].apply(romano_a_entero)

    # Filtrar filas de Datos_SEC con condiciones específicas en 'Sector'
    filtro_indust_miner = (Datos_SEC['Sector'] == "Industrial y Minero y Cmrcl pub Residencial") | (Datos_SEC['Sector'] == "Industrial y Minero")
    datos_SEC_indust_mine = Datos_SEC[filtro_indust_miner].copy()
    datos_SEC_indust_mine['Sector'] = "Industrial y Minero"
    datos_SEC_indust_mine = datos_SEC_indust_mine.drop(columns=['Region','Subsector'])


    Datos_SEC = Datos_SEC.drop(columns=['Region','Sector'])
    UFisicas_to_Tcal = pd.read_excel(archivo,sheet_name='U. fisica a Tcal')
    BNE_reg = procesar_BNE_regional(archivo,año)
    FE_CO2_df = pd.read_excel(archivo,sheet_name='FE CO2 Data').drop(columns='Poder calorifico inferior TJ/kg')
    FE_CO2_df.columns = ['Energético','Factor de emision CO2 kg/Tj']
    CE_S = pd.read_excel(archivo,sheet_name='CE S.')
    PCI_df = pd.read_excel(archivo,sheet_name='PCI')
    



    Datos_SEC['Subsector'] = Datos_SEC['Subsector'].str.lower()
    Datos_SEC['Energético'] = Datos_SEC['Energético'].str.lower()
    datos_SEC_indust_mine['Sector'] = datos_SEC_indust_mine['Sector'].str.lower()
    datos_SEC_indust_mine['Energético'] = datos_SEC_indust_mine['Energético'].str.lower()
    UFisicas_to_Tcal['Energético'] = UFisicas_to_Tcal['Energético'].str.lower()
    BNE_reg['Sector'] = BNE_reg['Sector'].str.lower()
    BNE_reg['Subsector'] = BNE_reg['Subsector'].str.lower()
    BNE_reg['Energético'] = BNE_reg['Energético'].str.lower()
    FE_CO2_df['Energético'] = FE_CO2_df['Energético'].str.lower()
    CE_S['Energético'] = CE_S['Energético'].str.lower()
    CE_S['Sector'] = CE_S['Sector'].str.lower()
    PCI_df['Energético'] = PCI_df['Energético'].str.lower()


    #Juntamos todo en una sola tabla
    data_merge = BNE_reg.merge(FE_CO2_df, how='left',on='Energético')
    data_merge = data_merge.merge(CE_S,on=['Sector','Energético'],how='left')
    data_merge = data_merge.merge(PCI_df, how='left',on='Energético')
    data_merge = data_merge.merge(Datos_SEC,how='left',on=['cod_region','Subsector','Energético'])
    data_merge = data_merge.merge(UFisicas_to_Tcal,how='left',on='Energético')
    data_merge = data_merge.merge(datos_SEC_indust_mine, how='left', on=['cod_region', 'Sector', 'Energético'])

    # Calcular la suma total de Tcal BNE por cada combinación de ['cod_region', 'Sector', 'Energético']
    sumas_Tcal_BNE = data_merge.groupby(['cod_region', 'Sector', 'Energético'])['Tcal BNE'].transform('sum')

    # Calcular el porcentaje de contribución de cada subsector en base a Tcal BNE
    data_merge['Porcentaje'] = data_merge['Tcal BNE'] / sumas_Tcal_BNE

    # Calcular los valores proporcionales de SEC Unid. Físicas
    data_merge['SEC Unid. Físicas Proporcionado'] = data_merge['SEC Unid. Físicas_y'] * data_merge['Porcentaje']

    # Combinar los resultados de ambos merges en la columna 'SEC Unid. Físicas'
    data_merge['SEC Unid. Físicas'] = data_merge['SEC Unid. Físicas Proporcionado'].combine_first(data_merge['SEC Unid. Físicas_x'])
    data_merge.drop(['SEC Unid. Físicas_x', 'SEC Unid. Físicas_y', 'SEC Unid. Físicas Proporcionado','Porcentaje'], axis=1, inplace=True)

    
    # Capitalizar la primera letra de cada palabra después del merge
    data_merge['Sector'] = data_merge['Sector'].apply(capitalizar_excepto_palabras_min)
    data_merge['Subsector'] = data_merge['Subsector'].apply(capitalizar_excepto_palabras_min)
    data_merge['Energético'] = data_merge['Energético'].apply(capitalizar_excepto_palabras_min)

    #Seleccionamos variables
    #SEC_Tcal = data_merge['SEC Tcal']
    SEC_UF = data_merge['SEC Unid. Físicas']
    Factor_Tcal = data_merge['Factor a Tcal']     
    BNE_Tcal = data_merge['Tcal BNE']
    FE_CO2 = data_merge['Factor de emision CO2 kg/Tj']
    FE_CH4 = data_merge['FE CH4 (kg/TJ)']
    FE_N2O = data_merge['FE N2O (kg/TJ)']
    PCI = data_merge['ajuste a PCI']


    #Cálculos
    #Energía SEC en TJ 
    SEC_Tcal = SEC_UF * Factor_Tcal
    data_merge['SEC Tcal'] = SEC_Tcal
    SEC_TJ = SEC_Tcal * 4.1868

    #Cálculo de emisiones SEC en (kg)
    Emisiones_SEC_CO2_kg = SEC_TJ * FE_CO2
    Emisiones_SEC_CH4_kg = SEC_TJ * FE_CH4
    Emisiones_SEC_N2O_kg = SEC_TJ * FE_N2O

    #Cálculo de emisiones SEC en (Millt)
    Emisiones_SEC_CO2_Millt = Emisiones_SEC_CO2_kg / 1000000000
    Emisiones_SEC_CH4_Millt = Emisiones_SEC_CH4_kg / 1000000000 
    Emisiones_SEC_N2O_Millt = Emisiones_SEC_N2O_kg / 1000000000 

    #Calculo de emisiones SEC en (Millt) con ajuste PCI
    Emisiones_SEC_CO2_Millt_PCI = PCI * Emisiones_SEC_CO2_Millt
    Emisiones_SEC_CH4_Millt_PCI = PCI * Emisiones_SEC_CH4_Millt
    Emisiones_SEC_N2O_Millt_PCI = PCI * Emisiones_SEC_N2O_Millt
    
    #Cálculo de emisiones CO2eq de SEC en (Millt) 
    data_merge['Emisiones SEC CO2eq (Millt)'] = Emisiones_SEC_CO2_Millt_PCI*1 + Emisiones_SEC_CH4_Millt_PCI*25 + Emisiones_SEC_N2O_Millt_PCI*298
    data_merge['Emisiones SEC CO2 ajustadas con PCI (Millt)'] = Emisiones_SEC_CO2_Millt_PCI




    #Energía BNE en TJ
    data_merge['BNE TJ'] = BNE_Tcal * 4.1868

    #Consumo de energía del BNE en TJ
    Consumo_BNE_TJ = BNE_Tcal * 4.184

    #Cálculo de emisiones BNE en (kg)
    Emisiones_BNE_CO2_kg = Consumo_BNE_TJ * FE_CO2
    Emisiones_BNE_CH4_kg = Consumo_BNE_TJ * FE_CH4
    Emisiones_BNE_N2O_kg = Consumo_BNE_TJ * FE_N2O

    #Cálculo de emisiones BNE en (Millt)
    Emisiones_BNE_CO2_Millt = Emisiones_BNE_CO2_kg / 1000000000
    Emisiones_BNE_CH4_Millt = Emisiones_BNE_CH4_kg / 1000000000
    Emisiones_BNE_N2O_Millt = Emisiones_BNE_N2O_kg / 1000000000

    #Cálculo de emisiones BNE en (Millt) con ajuste PCI
    Emisiones_BNE_CO2_Millt_PCI = PCI * Emisiones_BNE_CO2_Millt
    Emisiones_BNE_CH4_Millt_PCI = PCI * Emisiones_BNE_CH4_Millt
    Emisiones_BNE_N2O_Millt_PCI = PCI * Emisiones_BNE_N2O_Millt

    #Cálculo de emisiones CO2eq de BNE en (Millt)
    data_merge['Emisiones BNE CO2eq (Millt)'] = Emisiones_BNE_CO2_Millt_PCI*1 + Emisiones_BNE_CH4_Millt_PCI*25 + Emisiones_BNE_N2O_Millt_PCI*298
    data_merge['Emisiones BNE CO2 BNE ajustadas con PCI (Millt)'] = Emisiones_BNE_CO2_Millt_PCI

    #Creamos una tabla con las columnas de interés
    data_final = data_merge[['nombre_region','cod_region','Sector','Subsector','Energético','Tcal BNE','SEC Tcal','Emisiones BNE CO2eq (Millt)','Emisiones BNE CO2 BNE ajustadas con PCI (Millt)','Emisiones SEC CO2eq (Millt)','Emisiones SEC CO2 ajustadas con PCI (Millt)']]

    data_final = data_final.drop_duplicates()
    data_final = data_final.rename(columns={'Tcal BNE': 'BNE Tcal'})
    data_final.to_csv(f'Emisiones_regionales_{año}.csv', index=False, encoding='utf-8-sig')

    #Calculamos por categoría INGEI
    diccionario_INGEI = pd.read_excel(archivo,sheet_name='diccionario_INGEI_BNE')
    diccionario_INGEI.columns = ['Subsector','Categoría INGEI']

    emisiones_INGEI = data_final.merge(diccionario_INGEI,how='left',on='Subsector')
    emisiones_INGEI = emisiones_INGEI.drop_duplicates()
    INGEI_group = emisiones_INGEI.groupby(['nombre_region','cod_region','Categoría INGEI']).sum().reset_index()[['nombre_region','cod_region','Categoría INGEI','BNE Tcal','SEC Tcal','Emisiones BNE CO2eq (Millt)','Emisiones BNE CO2 BNE ajustadas con PCI (Millt)','Emisiones SEC CO2eq (Millt)','Emisiones SEC CO2 ajustadas con PCI (Millt)']]
    
    #Exportamos
    emisiones_INGEI.to_csv(f'Emisiones_regionales_INGEI_{año}.csv',index=False,encoding='utf-8-sig')
    INGEI_group.to_csv(f'regionales_INGEI_agrupadas_{año}.csv',index=False,encoding='utf-8-sig')
    return



def calcular_emisiones(archivo,año):
    #Carga de datos
    año = int(año)
    Datos_SEC = pd.read_excel(archivo, sheet_name='Datos SEC').query('Año == @año').drop(columns='Año')
    UFisicas_to_Tcal = pd.read_excel(archivo,sheet_name='U. fisica a Tcal')
    BNE = pd.read_excel(archivo,sheet_name='BNE').query('Año == @año').drop(columns=['Item','Seccion','Año'])
    BNE.columns = ['Subsector','Energético','Tcal BNE']
    FE_CO2_df = pd.read_excel(archivo,sheet_name='FE CO2 Data').drop(columns='Poder calorifico inferior TJ/kg')
    FE_CO2_df.columns = ['Energético','Factor de emision CO2 kg/Tj']
    CE_S = pd.read_excel(archivo,sheet_name='CE S.')
    PCI_df = pd.read_excel(archivo,sheet_name='PCI')

    Datos_SEC['Sector'] = Datos_SEC['Sector'].str.lower()
    Datos_SEC['Subsector'] = Datos_SEC['Subsector'].str.lower()
    Datos_SEC['Energético'] = Datos_SEC['Energético'].str.lower()
    UFisicas_to_Tcal['Energético'] = UFisicas_to_Tcal['Energético'].str.lower()
    BNE['Subsector'] = BNE['Subsector'].str.lower()
    BNE['Energético'] = BNE['Energético'].str.lower()
    FE_CO2_df['Energético'] = FE_CO2_df['Energético'].str.lower()
    CE_S['Energético'] = CE_S['Energético'].str.lower()
    CE_S['Sector'] = CE_S['Sector'].str.lower()
    PCI_df['Energético'] = PCI_df['Energético'].str.lower()


    #Juntamos todo en una sola tabla
    data_merge = Datos_SEC.merge(UFisicas_to_Tcal,how='left',on='Energético')
    data_merge = data_merge.merge(BNE,how='left',on=['Subsector','Energético'])
    data_merge = data_merge.merge(FE_CO2_df, how='left',on='Energético')
    data_merge = data_merge.merge(CE_S,on=['Sector','Energético'],how='left')
    data_merge = data_merge.merge(PCI_df, how='left',on='Energético')
    
    
    # Capitalizar la primera letra de cada palabra después del merge
    data_merge['Sector'] = data_merge['Subsector'].apply(capitalizar_excepto_palabras_min)
    data_merge['Subsector'] = data_merge['Subsector'].apply(capitalizar_excepto_palabras_min)
    data_merge['Energético'] = data_merge['Energético'].apply(capitalizar_excepto_palabras_min)


    #Seleccionamos variables
    SEC_UF = data_merge['SEC Unid. Físicas']
    Factor_Tcal = data_merge['Factor a Tcal'] 
    BNE_Tcal = data_merge['Tcal BNE']
    FE_CO2 = data_merge['Factor de emision CO2 kg/Tj']
    FE_CH4 = data_merge['FE CH4 (kg/TJ)']
    FE_N2O = data_merge['FE N2O (kg/TJ)']
    PCI = data_merge['ajuste a PCI']


    #Cálculos
    #Energía SEC en TCal
    SEC_Tcal = SEC_UF * Factor_Tcal
    data_merge['SEC Tcal'] = SEC_Tcal
    #Energía BNE agregado en Tcal 
    BNE_TCal_agregado = BNE_Tcal
    #Diferencia Tcal vs SEC
    data_merge['Dif %'] = np.where(BNE_Tcal > 0, (BNE_Tcal - SEC_Tcal)/BNE_Tcal, np.nan)
    #Energía BNE en TJ
    data_merge['BNE TJ'] = BNE_Tcal * 4.1868

    #Energía SEC en TJ 
    SEC_TJ = SEC_Tcal * 4.1868

    #Cálculo de emisiones SEC en (kg)
    Emisiones_SEC_CO2_kg = SEC_TJ * FE_CO2
    Emisiones_SEC_CH4_kg = SEC_TJ * FE_CH4
    Emisiones_SEC_N2O_kg = SEC_TJ * FE_N2O

    #Cálculo de emisiones SEC en (Millt)
    Emisiones_SEC_CO2_Millt = Emisiones_SEC_CO2_kg / 1000000000
    Emisiones_SEC_CH4_Millt = Emisiones_SEC_CH4_kg / 1000000000 
    Emisiones_SEC_N2O_Millt = Emisiones_SEC_N2O_kg / 1000000000 

    #Calculo de emisiones SEC en (Millt) con ajuste PCI
    Emisiones_SEC_CO2_Millt_PCI = PCI * Emisiones_SEC_CO2_Millt
    Emisiones_SEC_CH4_Millt_PCI = PCI * Emisiones_SEC_CH4_Millt
    Emisiones_SEC_N2O_Millt_PCI = PCI * Emisiones_SEC_N2O_Millt
    
    #Cálculo de emisiones CO2eq de SEC en (Millt) 
    data_merge['Emisiones SEC CO2eq (Millt)'] = Emisiones_SEC_CO2_Millt_PCI*1 + Emisiones_SEC_CH4_Millt_PCI*25 + Emisiones_SEC_N2O_Millt_PCI*298
    data_merge['Emisiones SEC CO2 ajustadas con PCI (Millt)'] = Emisiones_SEC_CO2_Millt_PCI

    #Consumo de energía del BNE en TJ
    Consumo_BNE_TJ = BNE_Tcal * 4.184

    #Cálculo de emisiones BNE en (kg)
    Emisiones_BNE_CO2_kg = Consumo_BNE_TJ * FE_CO2
    Emisiones_BNE_CH4_kg = Consumo_BNE_TJ * FE_CH4
    Emisiones_BNE_N2O_kg = Consumo_BNE_TJ * FE_N2O

    #Cálculo de emisiones BNE en (Millt)
    Emisiones_BNE_CO2_Millt = Emisiones_BNE_CO2_kg / 1000000000
    Emisiones_BNE_CH4_Millt = Emisiones_BNE_CH4_kg / 1000000000
    Emisiones_BNE_N2O_Millt = Emisiones_BNE_N2O_kg / 1000000000

    #Cálculo de emisiones BNE en (Millt) con ajuste PCI
    Emisiones_BNE_CO2_Millt_PCI = PCI * Emisiones_BNE_CO2_Millt
    Emisiones_BNE_CH4_Millt_PCI = PCI * Emisiones_BNE_CH4_Millt
    Emisiones_BNE_N2O_Millt_PCI = PCI * Emisiones_BNE_N2O_Millt

    #Cálculo de emisiones CO2eq de BNE en (Millt)
    data_merge['Emisiones BNE CO2eq (Millt)'] = Emisiones_BNE_CO2_Millt_PCI*1 + Emisiones_BNE_CH4_Millt_PCI*25 + Emisiones_BNE_N2O_Millt_PCI*298
    data_merge['Emisiones BNE CO2 BNE ajustadas con PCI (Millt)'] = Emisiones_BNE_CO2_Millt_PCI

    #Consumo de energía agregado BNE en TJ
    Consumo_BNE_TJ_agregado = BNE_TCal_agregado * 4.1868

    #Cálculo de emisiones BNE agregado en (kg)
    Emisiones_BNE_CO2_kg_agregado = Consumo_BNE_TJ_agregado * FE_CO2
    Emisiones_BNE_CH4_kg_agregado = Consumo_BNE_TJ_agregado * FE_CH4
    Emisiones_BNE_N2O_kg_agregado = Consumo_BNE_TJ_agregado * FE_N2O

    #Cálculo de emisiones BNE agregado en (Millt)
    Emisiones_BNE_CO2_Millt_agregado = Emisiones_BNE_CO2_kg_agregado / 1000000000
    Emisiones_BNE_CH4_Millt_agregado = Emisiones_BNE_CH4_kg_agregado / 1000000000
    Emisiones_BNE_N2O_Millt_agregado = Emisiones_BNE_N2O_kg_agregado / 1000000000

    #Cálculo de emisiones BNE agregado en (Millt) con ajuste PCI
    Emisiones_BNE_CO2_Millt_PCI_agregado = PCI * Emisiones_BNE_CO2_Millt_agregado
    Emisiones_BNE_CH4_Millt_PCI_agregado = PCI * Emisiones_BNE_CH4_Millt_agregado
    Emisiones_BNE_N2O_Millt_PCI_agregado = PCI * Emisiones_BNE_N2O_Millt_agregado

    #Cálculo de emisiones CO2eq de BNE agregado en (Millt)
    data_merge['Emisiones BNE CO2eq (Millt) agregado'] = Emisiones_BNE_CO2_Millt_PCI_agregado * 1 + Emisiones_BNE_CH4_Millt_PCI_agregado * 25 + Emisiones_BNE_N2O_Millt_PCI_agregado * 298
    #Creamos una tabla con las columnas de interés
    data_final = data_merge[['Sector','Subsector','Energético','Unidad','SEC Tcal','Tcal BNE','Dif %','Emisiones SEC CO2eq (Millt)','Emisiones SEC CO2 ajustadas con PCI (Millt)','Emisiones BNE CO2eq (Millt)','Emisiones BNE CO2 BNE ajustadas con PCI (Millt)']]


    data_final = data_final.drop_duplicates()
    data_final = data_final.rename(columns={'Tcal BNE': 'BNE Tcal'})
    data_final.to_csv(f'BNE_vs_SEC_{año}.csv',index=False,encoding='latin1')


    #Calculamos por categoría INGEI
    diccionario_INGEI = pd.read_excel(archivo,sheet_name='diccionario_INGEI_BNE')
    diccionario_INGEI.columns = ['Subsector','Categoría INGEI']

    emisiones_INGEI = data_final.merge(diccionario_INGEI,how='left',on='Subsector')
    emisiones_INGEI = emisiones_INGEI.drop_duplicates()
    INGEI_group = emisiones_INGEI.groupby(['Categoría INGEI']).sum().reset_index()[['Categoría INGEI','Emisiones SEC CO2eq (Millt)','Emisiones SEC CO2 ajustadas con PCI (Millt)','Emisiones BNE CO2eq (Millt)','Emisiones BNE CO2 BNE ajustadas con PCI (Millt)']]
    
    #Exportamos
    emisiones_INGEI.to_csv(f'Emisiones_INGEI_{año}.csv',index=False,encoding='utf-8-sig')
    INGEI_group.to_csv(f'INGEI_agrupadas_{año}.csv',index=False,encoding='utf-8-sig')

    return

def subir_datos():
    print('Por favor archivo para cálculo de emisiones')
# Create a tkinter window
    root = tk.Tk()
    root.withdraw()  # Hide the main window

# Open a file dialog and get the selected file's name
    file_path = filedialog.askopenfilename()
    #file_name = file_path.split('/')[-1]
    print(f'Archivo cargado: {file_path.split("/")[-1]}')
    return file_path

#Ejecutamos el calculo
def main(path):
    start_time = time.time()
    print()
    print('Calculando Emisiones CO2eq')
    año = input('Por favor ingresar año a calcular: ')
    calcular_emisiones(path,año)
    print()
    print('Emisiones Calculadas. Se ha generado un archivo CSV')
    print()
    print('Calculando Emisiones CO2eq BNE Regional')

    mostrar_mensaje('Cargar Archivos', 'Por favor, cargue el archivo de energía estimada de la SEC.')
    path_SEC = pedir_archivo('Cargar energía estimada de la SEC')
    if not path_SEC:
        mostrar_mensaje('Error', 'No se seleccionó el archivo de la SEC. El proceso ha sido cancelado.')
        return
    df_SEC = pd.read_csv(path_SEC)
    calcular_emisiones_regionales(path,año,df_SEC)
    print()
    print('Emisiones Regionales Calculadas. Se ha generado un archivo CSV')
    print()
    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main(subir_datos())