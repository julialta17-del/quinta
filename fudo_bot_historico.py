import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime

def ejecutar_sincronizacion_macro():
    print("Conectando con Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Quinta Analisis Fudo")

    # 1. LEER HOJA 1 (Tal cual está)
    try:
        sheet_uno = spreadsheet.worksheet("Hoja 1")
        # numericise_ignore=['all'] es clave para que Pandas no toque las comas
        data_uno = pd.DataFrame(sheet_uno.get_all_records(numericise_ignore=['all']))
    except Exception as e:
        print(f"Error al leer Hoja 1: {e}")
        return

    if data_uno.empty:
        print("Hoja 1 vacía.")
        return

    data_uno.columns = [str(c).strip() for c in data_uno.columns]

    # 2. FILTRAR SOLO LO DE HOY
    fecha_hoy_str = datetime.now().strftime('%d/%m/%Y')
    print(f"Filtrando para dejar solo el día: {fecha_hoy_str}")
    
    if 'Fecha_Texto' in data_uno.columns:
        # Borramos cualquier fila que no sea de hoy
        data_hoy = data_uno[data_uno['Fecha_Texto'] == fecha_hoy_str].copy()
    else:
        print("No se encontró la columna Fecha_Texto.")
        return

    # 3. PREPARAR EL HISTÓRICO
    try:
        sheet_hist = spreadsheet.worksheet("Historico")
    except gspread.exceptions.WorksheetNotFound:
        sheet_hist = spreadsheet.add_worksheet(title="Historico", rows="50000", cols="30")

    # 4. LIMPIEZA Y PEGADO "ESPEJO"
    print(f"Borrando Historico y pegando {len(data_hoy)} filas nuevas...")
    sheet_hist.clear() # Borra todo lo anterior
    
    # --- EL TRUCO PARA QUE NO SE BORREN LAS COMAS ---
    # Convertimos absolutamente todo a string y nos aseguramos de que no haya nulos
    data_hoy = data_hoy.fillna("").astype(str)
    
    columnas = data_hoy.columns.tolist()
    # Convertimos cada celda en un String puro para Google Sheets
    filas_finales = data_hoy.values.tolist()
    datos_a_subir = [columnas] + filas_finales

    # Usamos RAW para que Google no intente "formatear" el texto a número
    sheet_hist.update(values=datos_a_subir, range_name='A1', value_input_option='RAW')
    
    print(f"✅ Sincronización terminada. Se mantuvo el formato original de Fudo.")

if __name__ == "__main__":
    ejecutar_sincronizacion_macro()
