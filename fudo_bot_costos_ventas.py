import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def calcular_margen_detallado_big_salads():
    print("1. Conectando a Google Sheets para Big Salads Sexta...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # Carga de credenciales desde Secrets de GitHub
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Analisis Fudo")
    sheet_ventas = spreadsheet.worksheet("Hoja 1")
    sheet_costos = spreadsheet.worksheet("Maestro_Costos")

    # 2. PROCESAR DICCIONARIO DE COSTOS
    df_costos = pd.DataFrame(sheet_costos.get_all_records())
    # Convertimos nombres a string para evitar errores con códigos numéricos
    dict_costos = pd.Series(df_costos['Costo'].values, index=df_costos['Nombre'].astype(str).str.strip()).to_dict()

    # 3. LEER VENTAS ACTUALES
    data_ventas = sheet_ventas.get_all_records()
    df_ventas = pd.DataFrame(data_ventas)
    
    # --- LIMPIEZA DE COLUMNAS (Evita el AttributeError de .str) ---
    df_ventas.columns = [str(col).strip() for col in df_ventas.columns]

    # Borramos columnas de cálculos previos si existen para que el reordenamiento sea limpio
    cols_viejas = ['Costo_Total_Venta', 'Comision_PeYa_$', 'Margen_Neto_$', 'Margen_Neto_%']
    df_ventas = df_ventas.drop(columns=[c for c in cols_viejas if c in df_ventas.columns])

    # --- CÁLCULOS DE COSTO E INSUMOS ---
    def calcular_costo_acumulado(celda_productos):
        if not celda_productos or str(celda_productos).lower() in ['nan', '']:
            return 0
        lista_items = [item.strip() for item in str(celda_productos).split(',')]
        return sum(dict_costos.get(producto, 0) for producto in lista_items)

    # Verificación de seguridad para la columna clave
    if 'Detalle_Productos' in df_ventas.columns:
        df_ventas['Costo_Total_Venta'] = df_ventas['Detalle_Productos'].apply(calcular_costo_acumulado)
    else:
        print(f"❌ Error: No se encontró 'Detalle_Productos'. Columnas: {df_ventas.columns}")
        return

    # --- LÓGICA FINANCIERA (Comisión PeYa 30%) ---
    def procesar_finanzas(fila):
        venta = pd.to_numeric(fila.get('Total', 0), errors='coerce') or 0
        costo_insumos = fila.get('Costo_Total_Venta', 0)
        origen = str(fila.get('Origen', '')).lower()
        
        comision = round(venta * 0.30, 2) if "pedidos ya" in origen else 0
        margen = round(venta - costo_insumos - comision, 2)
        return pd.Series([comision, margen])

    df_ventas[['Comision_PeYa_$', 'Margen_Neto_$']] = df_ventas.apply(procesar_finanzas, axis=1)
    
    df_ventas['Margen_Neto_%'] = np.where(
        df_ventas['Total'].astype(float) > 0, 
        ((df_ventas['Margen_Neto_$'] / df_ventas['Total'].astype(float)) * 100).round(1), 
        0
    )

    # --- 4. REORDENAMIENTO FORZADO ---
    # Aseguramos que 'Comision_PeYa_$' sea siempre la última columna
    columnas_sin_comision = [c for c in df_ventas.columns if c != 'Comision_PeYa_$']
    nuevo_orden = columnas_sin_comision + ['Comision_PeYa_$']
    
    df_final = df_ventas[nuevo_orden].copy()

    print("5. Sincronizando Hoja 1 en Drive...")
    df_final = df_final.replace([np.nan, np.inf, -np.inf], 0)
    
    # Preparamos los datos (Headers + Filas)
    datos_subir = [df_final.columns.tolist()] + df_final.values.tolist()
    
    sheet_ventas.clear()
    sheet_ventas.update(range_name='A1', values=datos_subir)
    
    print(f"✅ ¡Éxito! Margen detallado actualizado y comisión al final.")

if __name__ == "__main__":
    calcular_margen_detallado_big_salads()
