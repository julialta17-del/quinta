import pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np
from datetime import datetime

def ejecutar_sincronizacion_macro():
    # 1. CONEXIÓN SEGURA
    print("Conectando con Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Quinta Analisis Fudo")

    # 2. CARGAR HOJAS
    try:
        sheet_uno = spreadsheet.worksheet("Hoja 1")
    except Exception as e:
        print(f"Error: No se encontró la 'Hoja 1'. {e}")
        return

    # Usamos get_all_records(numericise_ignore=['all']) para que traiga el texto tal cual (con la coma)
    data_uno = pd.DataFrame(sheet_uno.get_all_records(numericise_ignore=['all']))
    if data_uno.empty:
        print("La Hoja 1 está vacía.")
        return

    data_uno.columns = [str(c).strip() for c in data_uno.columns]

    # --- REQUISITO 1: FILTRAR SOLO FILAS DE HOY ---
    fecha_hoy_str = datetime.now().strftime('%d/%m/%Y')
    print(f"Filtrando filas con fecha: {fecha_hoy_str}")
    # Buscamos en la columna 'Fecha_Texto'
    if 'Fecha_Texto' in data_uno.columns:
        data_uno = data_uno[data_uno['Fecha_Texto'] == fecha_hoy_str].copy()
    
    if data_uno.empty:
        print(f"No hay pedidos con fecha de hoy ({fecha_hoy_str}) para sincronizar.")
        return

    # 3. GESTIÓN DEL HISTÓRICO
    try:
        sheet_hist = spreadsheet.worksheet("Historico")
        # Traemos el histórico ignorando numeración automática para no romper las comas
        data_hist = pd.DataFrame(sheet_hist.get_all_records(numericise_ignore=['all']))
    except gspread.exceptions.WorksheetNotFound:
        print("Creando hoja 'Historico'...")
        sheet_hist = spreadsheet.add_worksheet(title="Historico", rows="50000", cols="30")
        sheet_hist.append_row(data_uno.columns.tolist())
        data_hist = pd.DataFrame(columns=data_uno.columns)

    # 4. LÓGICA DE COMPARACIÓN (Evitar duplicados)
    data_uno['Id_Str'] = data_uno['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if not data_hist.empty and 'Id' in data_hist.columns:
        ids_viejos = data_hist['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().tolist()
        nuevos_datos = data_uno[~data_uno['Id_Str'].isin(ids_viejos)].copy()
    else:
        nuevos_datos = data_uno.copy()

    # 5. ACTUALIZAR HISTÓRICO
    if not nuevos_datos.empty:
        # --- REQUISITO 2: MANTENER LA COMA ---
        # Al convertir a string y haber usado 'numericise_ignore', mantenemos el formato original
        filas_a_subir = nuevos_datos.drop(columns=['Id_Str']).fillna("")
        
        # Subimos los datos asegurando que Google los trate como texto/formato original
        sheet_hist.append_rows(filas_a_subir.astype(str).values.tolist(), value_input_option='RAW')
        print(f"Éxito: Se agregaron {len(filas_a_subir)} filas nuevas al Historico.")
    else:
        print("El Historico ya está al día.")

    # 6. ANÁLISIS MACRO (Para el Dashboard)
    print("Actualizando Dashboard Macro...")
    # Para cálculos, aquí sí convertimos temporalmente a número
    df_macro = pd.DataFrame(sheet_hist.get_all_records())
    col_plata = 'Total'
    
    # Limpiamos el formato de plata de Argentina (1.200,50 -> 1200.50) para poder sumar
    if col_plata in df_macro.columns:
        df_macro[col_plata] = df_macro[col_plata].astype(str).str.replace('.', '').str.replace(',', '.')
        df_macro[col_plata] = pd.to_numeric(df_macro[col_plata], errors='coerce').fillna(0)

    # ... (Resto del código de Dashboard y Gráficos se mantiene igual)
    print("✅ Proceso finalizado.")

if __name__ == "__main__":
    ejecutar_sincronizacion_macro()
