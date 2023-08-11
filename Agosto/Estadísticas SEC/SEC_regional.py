import os
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def mostrar_mensaje(titulo, mensaje):
    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo(title=titulo, message=mensaje)
    root.destroy()

def mostrar_mensaje_final(titulo, mensaje, archivo_guardado):
    root = tk.Tk()
    root.withdraw()

    result = messagebox.showinfo(title=titulo, message=mensaje, icon='info', type='okcancel')

    if result == 'ok' and archivo_guardado:
        abrir_carpeta_button = messagebox.askyesno(title="Abrir Carpeta", message="¿Deseas abrir la carpeta donde se guardó el archivo?")
        if abrir_carpeta_button:
            os.system(f'explorer /select,"{archivo_guardado}"')

    root.destroy()

def pedir_archivo(titulo):
    root = tk.Tk()
    root.withdraw()
    archivo_path = filedialog.askopenfilename(title=titulo, filetypes=[('Excel Files', '*.xlsx')])
    root.destroy()  # Cerramos la ventana después de obtener el archivo
    return archivo_path

def obtener_sec_regional():
    Regiones = ['I','II','III','IV','V','VI','VII','VIII','IX','X','XI','XII','XIII','XIV','XV','XVI']
    diccionario_liq = pd.read_csv('diccionario_combustibles.csv')

    mostrar_mensaje('Cargar Archivos', 'Por favor, cargue el archivo de combustibles líquidos.')
    liquidos_path = pedir_archivo('Cargar estadísticas de venta de combustibles líquidos')
    if not liquidos_path:
        mostrar_mensaje('Error', 'No se seleccionó el archivo de combustibles líquidos. El proceso ha sido cancelado.')
        return

    mostrar_mensaje('Cargar Archivos', 'Por favor, cargue el archivo de GLP.')
    glp_path = pedir_archivo('Cargar estadísticas de venta de GLP')
    if not glp_path:
        mostrar_mensaje('Error', 'No se seleccionó el archivo de GLP. El proceso ha sido cancelado.')
        return

    mostrar_mensaje('Cargar Archivos', 'Por favor, cargue el archivo de GRC.')
    gn_path = pedir_archivo('Cargar estadísticas de venta de Gas Natural')
    if not glp_path:
        mostrar_mensaje('Error', 'No se seleccionó el archivo de GRC. El proceso ha sido cancelado.')
        return

    year = simpledialog.askstring("Año", "Por favor, ingrese el año de la estimación:")
    if not year:
        mostrar_mensaje('Error', 'No se ingresó el año. El proceso ha sido cancelado.')
        return

    # Crear o buscar la carpeta para el año
    year_folder = os.path.join(os.getcwd(), year)
    if not os.path.exists(year_folder):
        os.mkdir(year_folder)

    # Crear o buscar la carpeta para las estimaciones regionales
    estimaciones_folder = os.path.join(year_folder, 'Estimaciones Regionales')
    if not os.path.exists(estimaciones_folder):
        os.mkdir(estimaciones_folder)

    # DataFrame para almacenar todas las tablas de las regiones
    df_total = pd.DataFrame()

    for region in Regiones:
        df_liquidos = pd.read_excel(liquidos_path, sheet_name=region, skiprows=4).drop(columns='MES ').dropna()
        df_glp = pd.read_excel(glp_path, sheet_name=region, skiprows=5).drop(columns='Fecha').dropna()
        df_gn = pd.read_excel(gn_path,skiprows=4).drop(columns='Mes').dropna()

        #Combustibles liquidos
        liq_merge = df_liquidos.merge(diccionario_liq,how='left',on='Combustible')
        liq_merge_group = liq_merge.groupby('Combustible SEC').sum().reset_index()
        liq_merge_group['Terrestre'] = (liq_merge_group['Empresa de Transporte'] + liq_merge_group['Canal Minorista'])/1000
        liq_merge_group['Industrial y Minero'] = liq_merge_group['Venta directa']/1000
        liq_merge_group['Residencial'] = liq_merge_group['Canal Minorista']/1000
        liq_final = liq_merge_group[['Combustible SEC','Terrestre','Industrial y Minero','Residencial']]
        liq_final.columns= ['Combustible','Terrestre','Industrial y Minero','Residencial']
        liq_final = liq_final.melt(id_vars='Combustible', var_name='Sector', value_name='Energia estimada')

        #Gas Licuado
        df_glp['Energia estimada'] = (df_glp[' Total General'] - df_glp['  Cil. Vehicular'])/1000
        df_glp['Combustible'] = 'Gas Licuado'
        df_glp_final = df_glp[['Combustible','Tipo Consumidor','Energia estimada' ]]
        df_glp_final.columns = ['Combustible','Sector','Energia estimada']
        glp_final = df_glp_final.groupby(['Combustible','Sector']).sum().reset_index()
        #Vehicular GLP
        df_glp_veh = df_glp[['Tipo Consumidor','  Cil. Vehicular']].copy()
        df_glp_veh['Energia estimada'] = df_glp_veh['  Cil. Vehicular']/1000
        df_glp_veh['Combustible'] = 'Gas Licuado'
        veh_glp = df_glp_veh[['Combustible','Tipo Consumidor','Energia estimada']]
        veh_glp.columns = ['Combustible','Sector','Energia estimada']
        veh_glp_final = veh_glp.groupby(['Combustible']).sum().reset_index()
        veh_glp_final['Sector'] = 'Terrestre'


        #Gas Natural
        # Diccionario para mapear nombres de regiones a números romanos
        regiones_romanas = {
            'Tarapacá': 'I',
            'Antofagasta': 'II',
            'Atacama': 'III',
            'Coquimbo': 'IV',
            'Valparaíso': 'V',
            "O'Higgins": 'VI',
            'Maule': 'VII',
            'Bío-Bío': 'VIII',
            'Araucanía': 'IX',
            'Los Lagos': 'X',
            'Aisén del Gral.Carlos Ibáñez del Campo': 'XI',
            'Magallanes': 'XII',
            'Metropolitana': 'XIII',
            'Los Ríos': 'XIV',
            'Arica y Parinacota': 'XV',
            'Ñuble': 'XVI'
        }

        # Agregar una nueva columna 'Región Romana' con los números romanos correspondientes
        df_gn['Región Romana'] = df_gn['Región'].map(regiones_romanas)

        df_gn = df_gn.drop(columns='Región')
        df_gn = df_gn.rename(columns={'Fiscal':'Público','Vehicular':'Todos, T','Región Romana': 'Región','Tipo Gas':'Combustible','Industrial':'Todos, IM'})
        df_gn['Todos, CP'] = df_gn['Consumo Propio'] + df_gn['Distribuidoras']
        gn_final = df_gn[['Región','Combustible','Comercial','Todos, IM','Residencial','Público','Todos, T','Todos, CP']]
        df_gn_final = gn_final.groupby(['Región','Combustible']).sum().reset_index()

        # Lista de columnas numéricas
        columnas_numericas = df_gn_final.columns.difference(['Región', 'Combustible'])

        # Dividir por 1000 las columnas numéricas
        df_gn_final[columnas_numericas] = df_gn_final[columnas_numericas] / 1000

        grc_final = df_gn_final.melt(id_vars=['Región','Combustible'], var_name='Sector', value_name='Energia estimada')
        grc_final.columns = ['Región','Combustible','Sector','Energia estimada']
        grc_final = grc_final.query('Región == @region')[['Sector','Combustible','Energia estimada']]


        df_final = pd.concat([liq_final, glp_final,veh_glp_final,grc_final])
        df_final = df_final.rename(columns={'Sector':'Subsector','Combustible':'Energético'})
        df_final = df_final[['Subsector', 'Energético', 'Energia estimada']]

        # Cambiar valores de "Comercial" a "Residencial" cuando "Energético" es "Biomasa" o "Kerosene"
        condition = (df_final['Energético'].isin(['Kerosene'])) & (df_final['Subsector'] == 'Industrial y Minero')
        df_final.loc[condition, 'Subsector'] = 'Industrial y minero y comercial'

        #Cambiar Industrial a Todos, IM
        condition = df_final['Subsector'].isin(['Industrial'])
        df_final.loc[condition, 'Subsector'] = 'Todos, IM'

        # Filtrar las filas que tengan "Residencial" como "Subsector" pero "Energético" diferente a "Gas Natural", "Biomasa", "Kerosene" o "Gas Licuado"
        condition = (df_final['Subsector'] == 'Residencial') & (~df_final['Energético'].isin(['Gas Natural', 'Biomasa', 'Kerosene', 'Gas Licuado']))
        df_final = df_final.query('not @condition')

        # Agregar columna de Región
        df_final.insert(0, 'Region', region)

        # Mapear el Subsector a la nueva columna Sector
        df_final['Sector'] = df_final['Subsector'].map({
            'Terrestre': 'Transporte',
            'Comercial': 'Sector Cmrcl., Púb. Y Residencial',
            'Servicio Público': 'Sector Cmrcl., Púb. Y Residencial',
            'Residencial': 'Sector Cmrcl., Púb. Y Residencial',
            'Industrial y Minero': 'Industrial y Minero y Cmrcl pub Residencial',
            'Industrial y minero y comercial': 'Industrial y Minero y Cmrcl pub Residencial',
            'Cmrcl pub Residencial': 'Industrial y Minero y Cmrcl pub Residencial',
            'Todos, IM' : 'Industrial y Minero',
            'Todos, T': 'Transporte',
            'Público': 'Sector Cmrcl., Púb. Y Residencial',
            'Todos, CP': 'Energético: Consumo Propio'

        })

        # Reordenar las columnas
        df_final = df_final[['Region', 'Sector', 'Subsector', 'Energético', 'Energia estimada']]

        # Concatenar la tabla de la región al DataFrame total
        df_total = pd.concat([df_total, df_final])

    #df_total['Año'] = int(year)
    df_total = df_total[['Region','Sector','Subsector','Energético','Energia estimada']]
    #Cambiar Servicio Público a Público
    condition = df_total['Subsector'].isin(['Servicio Público'])
    df_total.loc[condition, 'Subsector'] = 'Público'

    # Guardar el DataFrame total en un archivo CSV
    file_name = os.path.join(estimaciones_folder, f'Estimaciones_regionales_SEC.csv')
    df_total.to_csv(file_name, encoding='utf-8-sig', index=False)

    mostrar_mensaje_final('Proceso Completo', 'Estimaciones regionales cargadas exitosamente.', archivo_guardado=file_name)

def main():
    obtener_sec_regional()

if __name__ == "__main__":
    main()