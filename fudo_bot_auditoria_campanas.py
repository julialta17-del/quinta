import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime
import numpy as np

def auditar_campanas_acumulativo():
    print("1. Conectando a Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # Prioridad a los Secrets de GitHub
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Analisis Fudo")
    
    try:
        sheet_cp = spreadsheet.worksheet("campanas")
        
        # --- 2. ZONA DE SEGURIDAD EN Z1 ---
        fecha_inicio_str = sheet_cp.acell('Z1').value 
        if not fecha_inicio_str:
            fecha_inicio_str = sheet_cp.acell('I1').value # Rescate por si quedó en I1
            if not fecha_inicio_str:
                print("❌ Error: Falta fecha de inicio en Z1 de la hoja 'campanas'.")
                return
        
        # Limpieza de la cadena de fecha por si trae espacios
        fecha_inicio_str = fecha_inicio_str.strip()
        fecha_inicio = datetime.strptime(fecha_inicio_str, '%d/%m/%Y')
        print(f"📅 Auditoría acumulativa de Big Salads Sexta desde: {fecha_inicio_str}")

        # 3. LEER DATOS EVITANDO ERROR DE CABECERAS DUPLICADAS
        all_values = sheet_cp.get_all_values()
        if not all_values:
            print("Hoja vacía.")
            return

        header = [h.strip() if h.strip() != "" else f"Vacia_{i}" for i, h in enumerate(all_values[0])]
        df_campana = pd.DataFrame(all_values[1:], columns=header)
        df_campana = df_campana.loc[:, ~df_campana.columns.str.contains('Vacia_')]

    except Exception as e:
        print(f"Error en lectura inicial: {e}")
        return

    # 4. LEER HISTORICO
    sheet_hist = spreadsheet.worksheet("Historico")
    df_h = pd.DataFrame(sheet_hist.get_all_records())
    df_h.columns = [str(c).strip() for c in df_h.columns]

    # 5. FILTRAR VENTAS POR RANGO
    df_h['Fecha_DT'] = pd.to_datetime(df_h['Fecha'], format='%d/%m/%Y', errors='coerce')
    ventas_rango = df_h[df_h['Fecha_DT'] >= fecha_inicio].copy()
    ventas_rango = ventas_rango.sort_values(['Cliente', 'Fecha_DT', 'Hora_Exacta'])

    if ventas_rango.empty:
        print(f"No hay ventas registradas desde el {fecha_inicio_str}")
        # Mantenemos la hoja igual pero nos aseguramos que Z1 siga ahí
        sheet_cp.update(values=[[fecha_inicio_str]], range_name='Z1')
        return

    # 6. CREAR COLUMNAS DINÁMICAS (Seguimiento de compras)
    ventas_rango['Nro_Compra'] = ventas_rango.groupby('Cliente').cumcount() + 1
    ventas_rango['Info_Compra'] = (
        ventas_rango['Fecha'] + " | " + 
        ventas_rango['Detalle_Productos'].str.split(',').str[0] + " | $" + 
        ventas_rango['Total'].astype(str)
    )

    df_seguimiento = ventas_rango.pivot(index='Cliente', columns='Nro_Compra', values='Info_Compra')
    df_seguimiento.columns = [f'Compra_{c}' for c in df_seguimiento.columns]
    df_seguimiento = df_seguimiento.reset_index()

    # 7. UNIR Y MARCAR ÉXITOS
    cols_viejas = [c for c in df_campana.columns if 'Compra_' in str(c) or 'Resultado' in str(c)]
    df_campana = df_campana.drop(columns=cols_viejas)

    df_final = pd.merge(df_campana, df_seguimiento, on='Cliente', how='left')
    df_final['Resultado'] = df_final['Compra_1'].apply(lambda x: "CAMPANA EXITOSA" if pd.notnull(x) and x != "" else "")

    # 8. SUBIR A DRIVE Y AJUSTAR ESPACIO
    print("Actualizando hoja 'campanas' en la nube...")
    
    # Asegurar que la hoja es lo suficientemente ancha para Z1 (Columna 26)
    columnas_finales = max(len(df_final.columns) + 1, 30)
    if sheet_cp.col_count < columnas_finales:
        sheet_cp.add_cols(columnas_finales - sheet_cp.col_count)

    sheet_cp.clear()
    df_subir = df_final.fillna("")
    datos_lista = [df_subir.columns.tolist()] + df_subir.values.tolist()
    
    sheet_cp.update(values=datos_lista, range_name='A1')
    
    # 9. RESGUARDAR FECHA EN Z1
    sheet_cp.update(values=[[fecha_inicio_str]], range_name='Z1')
    
    print(f"✅ Auditoría completada para Big Salads Sexta.")

if __name__ == "__main__":
    auditar_campanas_acumulativo()
