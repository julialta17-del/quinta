import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np
from datetime import datetime

# --- CONFIGURACIÓN DE RUTAS ---
base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel")
ruta_excel = os.path.join(temp_excel_path, "ventas.xls")

def limpiar_numero(serie):
    """Convierte strings con comas a números flotantes limpios"""
    return pd.to_numeric(serie.astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

def procesar_y_analizar():
    print(f"Buscando archivo en: {ruta_excel}")
    
    if not os.path.exists(ruta_excel):
        print(f"❌ Error: No se encontró el archivo")
        return

    # 1. CARGAR DATOS
    try:
        df_v = pd.read_excel(ruta_excel, sheet_name='Ventas', skiprows=3)
        df_a = pd.read_excel(ruta_excel, sheet_name='Adiciones')
        df_d = pd.read_excel(ruta_excel, sheet_name='Descuentos')
        df_e = pd.read_excel(ruta_excel, sheet_name='Costos de Envío')
    except Exception as e:
        print(f"❌ Error al leer Excel: {e}")
        return

    df_v.columns = df_v.columns.str.strip()
    
    # Procesamiento de fechas
    if not pd.api.types.is_datetime64_any_dtype(df_v['Creación']):
        df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], unit='D', origin='1899-12-30', errors='coerce')
    else:
        df_v['Fecha_DT'] = df_v['Creación']
    
    df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
    df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
    df_v['Hora_Int'] = df_v['Fecha_DT'].dt.hour 
    df_v['Turno'] = df_v['Hora_Int'].apply(lambda h: "Mañana" if h < 16 else "Noche")

    # --- CORRECCIÓN DE NÚMEROS (Comas por Puntos) ---
    df_v['Total'] = limpiar_numero(df_v['Total'])

    # 2. AGRUPAR ADICIONALES
    prod_resumen = df_a.groupby('Id. Venta')['Producto'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
    prod_resumen.columns = ['Id', 'Detalle_Productos']

    # Sumar Descuentos y Envíos limpiando las comas
    df_d['Valor'] = limpiar_numero(df_d['Valor'])
    df_e['Valor'] = limpiar_numero(df_e['Valor'])

    desc_resumen = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
    desc_resumen.columns = ['Id', 'Descuento_Total']

    envio_resumen = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()
    envio_resumen.columns = ['Id', 'Costo_Envio']

    # 3. CONSOLIDACIÓN
    columnas_interes = ['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']
    consolidado = df_v[columnas_interes].copy()
    
    consolidado = consolidado.merge(prod_resumen, on='Id', how='left')
    consolidado = consolidado.merge(desc_resumen, on='Id', how='left')
    consolidado = consolidado.merge(envio_resumen, on='Id', how='left')

    consolidado[['Descuento_Total', 'Costo_Envio']] = consolidado[['Descuento_Total', 'Costo_Envio']].fillna(0)
    consolidado['Detalle_Productos'] = consolidado['Detalle_Productos'].fillna("Sin detalle")

    # --- FILTRO SIEMPRE HOY ---
    fecha_hoy = datetime.now().strftime('%d/%m/%Y')
    print(f"Filtrando ventas para el día: {fecha_hoy}")
    
    consolidado = consolidado[consolidado['Fecha_Texto'] == fecha_hoy]

    if consolidado.empty:
        print(f"⚠️ No hay ventas registradas con la fecha de hoy ({fecha_hoy}).")
    else:
        print(f"✅ Procesadas {len(consolidado)} ventas de hoy.")

    # 4. SUBIR A GOOGLE SHEETS
    subir_a_google(consolidado)

def subir_a_google(consolidado):
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    
    try:
        spreadsheet = client.open("Analisis Fudo")
        sheet_data = spreadsheet.worksheet("Hoja 1")
        sheet_data.clear()
        
        # Convertimos todo a string para subirlo sin problemas de formato
        # Si prefieres puntos en Google Sheets, ya van limpios aquí
        datos_finales = [consolidado.columns.values.tolist()] + \
                         consolidado.fillna("").astype(str).values.tolist()
        
        sheet_data.update(range_name='A1', values=datos_finales)
        print("🚀 Hoja 1 actualizada con éxito.")
    except Exception as e:
        print(f"❌ Error al subir: {e}")

if __name__ == "__main__":
    procesar_y_analizar()
