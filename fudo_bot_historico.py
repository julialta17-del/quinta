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

    # 1. LEER HOJA 1
    try:
        sheet_uno = spreadsheet.worksheet("Hoja 1")
        # Traemos todo como texto para que no se borren las comas en el proceso
        data_completa = pd.DataFrame(sheet_uno.get_all_records(numericise_ignore=['all']))
    except Exception as e:
        print(f"Error al acceder a Hoja 1: {e}")
        return

    if data_completa.empty:
        print("La Hoja 1 ya está vacía.")
        return

    data_completa.columns = [str(c).strip() for c in data_completa.columns]

    # 2. FILTRAR: SEPARAR LO DE HOY DE LO VIEJO
    fecha_hoy_str = datetime.now().strftime('%d/%m/%Y')
    print(f"Buscando pedidos de hoy: {fecha_hoy_str}")

    if 'Fecha_Texto' in data_completa.columns:
        # Esto es lo que se QUEDA en Hoja 1 y se PASA al Historico
        solo_hoy = data_completa[data_completa['Fecha_Texto'] == fecha_hoy_str].copy()
    else:
        print("Error: No existe la columna 'Fecha_Texto'.")
        return

    # 3. LIMPIAR HOJA 1 (Borrar todo y pegar solo lo de hoy)
    print("Limpiando Hoja 1 (borrando días anteriores)...")
    sheet_uno.clear()
    if not solo_hoy.empty:
        # Convertimos a string para asegurar que la coma (,) no desaparezca
        datos_hoja1 = [solo_hoy.columns.tolist()] + solo_hoy.fillna("").astype(str).values.tolist()
        sheet_uno.update(values=datos_hoja1, range_name='A1', value_input_option='RAW')
        print(f"✅ Hoja 1 limpia: Quedaron {len(solo_hoy)} filas de hoy.")
    else:
        print("⚠️ No hay ventas de hoy. La Hoja 1 quedó vacía.")

    # 4. PASAR AL HISTÓRICO (Solo lo de hoy)
    if not solo_hoy.empty:
        try:
            sheet_hist = spreadsheet.worksheet("Historico")
        except gspread.exceptions.WorksheetNotFound:
            sheet_hist = spreadsheet.add_worksheet(title="Historico", rows="50000", cols="30")
            sheet_hist.append_row(solo_hoy.columns.tolist())

        # Leemos histórico para no duplicar si el bot corre dos veces
        data_hist = pd.DataFrame(sheet_hist.get_all_records(numericise_ignore=['all']))
        
        # Evitar duplicados por ID
        solo_hoy['Id_Str'] = solo_hoy['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        if not data_hist.empty:
            ids_viejos = data_hist['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().tolist()
            nuevos_para_hist = solo_hoy[~solo_hoy['Id_Str'].isin(ids_viejos)].copy()
        else:
            nuevos_para_hist = solo_hoy.copy()

        if not nuevos_para_hist.empty:
            print(f"Pasando {len(nuevos_para_hist)} pedidos nuevos al Historico...")
            filas_hist = nuevos_para_hist.drop(columns=['Id_Str']).fillna("").astype(str).values.tolist()
            sheet_hist.append_rows(filas_hist, value_input_option='RAW')
            print("✅ Sincronización al Historico completada con éxito.")
        else:
            print("El Historico ya estaba al día.")

if __name__ == "__main__":
    ejecutar_sincronizacion_macro()
