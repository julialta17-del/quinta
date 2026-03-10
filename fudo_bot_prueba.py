import pandas as pd  # <-- CORREGIDO: Antes decía 'import pd'
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

    try:
        sheet_uno = spreadsheet.worksheet("Hoja 1")
    except Exception as e:
        print(f"Error: No se encontró la 'Hoja 1'. {e}")
        return

    # IMPORTANTE: numericise_ignore=['all'] para mantener las comas intactas
    data_uno = pd.DataFrame(sheet_uno.get_all_records(numericise_ignore=['all']))
    if data_uno.empty:
        print("La Hoja 1 está vacía.")
        return

    data_uno.columns = [str(c).strip() for c in data_uno.columns]

    # --- FILTRO: SOLO FECHA DE HOY ---
    fecha_hoy_str = datetime.now().strftime('%d/%m/%Y')
    print(f"Buscando pedidos de hoy: {fecha_hoy_str}")
    
    if 'Fecha_Texto' in data_uno.columns:
        # Filtramos para que solo pase al histórico lo que se vendió hoy
        data_uno = data_uno[data_uno['Fecha_Texto'] == fecha_hoy_str].copy()
    
    if data_uno.empty:
        print(f"No hay pedidos nuevos de hoy ({fecha_hoy_str}) para procesar.")
        return

    # --- GESTIÓN DEL HISTÓRICO ---
    try:
        sheet_hist = spreadsheet.worksheet("Historico")
        # Leemos histórico también ignorando formatos numéricos para no chocar puntos con comas
        data_hist = pd.DataFrame(sheet_hist.get_all_records(numericise_ignore=['all']))
    except gspread.exceptions.WorksheetNotFound:
        print("Creando hoja 'Historico'...")
        sheet_hist = spreadsheet.add_worksheet(title="Historico", rows="50000", cols="30")
        sheet_hist.append_row(data_uno.columns.tolist())
        data_hist = pd.DataFrame(columns=data_uno.columns)

    # Evitar duplicados por ID
    data_uno['Id_Str'] = data_uno['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if not data_hist.empty and 'Id' in data_hist.columns:
        ids_viejos = data_hist['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().tolist()
        nuevos_datos = data_uno[~data_uno['Id_Str'].isin(ids_viejos)].copy()
    else:
        nuevos_datos = data_uno.copy()

    # --- SUBIDA AL HISTÓRICO ---
    if not nuevos_datos.empty:
        filas_a_subir = nuevos_datos.drop(columns=['Id_Str']).fillna("")
        
        # value_input_option='RAW' asegura que la coma se guarde como texto y no se borre
        sheet_hist.append_rows(filas_a_subir.astype(str).values.tolist(), value_input_option='RAW')
        print(f"✅ ÉXITO: Se agregaron {len(filas_a_subir)} filas nuevas al Historico con sus comas.")
    else:
        print("El Historico ya tiene estos pedidos. No se agregó nada.")

if __name__ == "__main__":
    ejecutar_sincronizacion_macro()
