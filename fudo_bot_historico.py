import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def ejecutar_sincronizacion_macro():
    # 1. CONEXIÓN SEGURA A GOOGLE SHEETS
    print("Conectando con Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # Priorizar la variable de entorno de GitHub Actions
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        # Modo local para pruebas
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Quinta Analisis Fudo")

    # 2. CARGAR HOJAS
    try:
        sheet_uno = spreadsheet.worksheet("Hoja 1")
    except Exception as e:
        print(f"Error: No se encontró la 'Hoja 1'. {e}")
        return

    data_uno = pd.DataFrame(sheet_uno.get_all_records())
    if data_uno.empty:
        print("La Hoja 1 está vacía. No hay nada que sincronizar.")
        return

    # Limpiar nombres de columnas
    data_uno.columns = [str(c).strip() for c in data_uno.columns]

    # 3. GESTIÓN DEL HISTÓRICO
    try:
        sheet_hist = spreadsheet.worksheet("Historico")
        data_hist = pd.DataFrame(sheet_hist.get_all_records())
    except gspread.exceptions.WorksheetNotFound:
        print("Creando hoja 'Historico'...")
        sheet_hist = spreadsheet.add_worksheet(title="Historico", rows="50000", cols="30")
        sheet_hist.append_row(data_uno.columns.tolist())
        data_hist = pd.DataFrame(columns=data_uno.columns)

    # 4. LÓGICA DE COMPARACIÓN
    # Convertimos ID a string para evitar problemas de formato (ej: 123 vs 123.0)
    data_uno['Id_Str'] = data_uno['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if not data_hist.empty and 'Id' in data_hist.columns:
        ids_viejos = data_hist['Id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().tolist()
        nuevos_datos = data_uno[~data_uno['Id_Str'].isin(ids_viejos)].copy()
    else:
        nuevos_datos = data_uno.copy()

    # 5. ACTUALIZAR HISTÓRICO
    if not nuevos_datos.empty:
        filas_a_subir = nuevos_datos.drop(columns=['Id_Str']).fillna("")
        # Convertimos todo a string para asegurar que Google lo reciba sin errores de tipo
        sheet_hist.append_rows(filas_a_subir.astype(str).values.tolist())
        print(f"Éxito: Se agregaron {len(filas_a_subir)} filas nuevas al Historico.")
    else:
        print("El Historico ya está al día.")

    # 6. ANÁLISIS MACRO
    print("Actualizando Dashboard Macro...")
    df_macro = pd.DataFrame(sheet_hist.get_all_records())
    df_macro.columns = [str(c).strip() for c in df_macro.columns]

    col_plata = 'Total' if 'Total' in df_macro.columns else df_macro.columns[5] # Ajuste dinámico
    col_fecha = 'Fecha' if 'Fecha' in df_macro.columns else 'Fecha_Texto'
    col_pago = 'Medio de Pago'

    df_macro[col_plata] = pd.to_numeric(df_macro[col_plata], errors='coerce').fillna(0)

    # Agrupaciones
    resumen_dia = df_macro.groupby(col_fecha).agg({'Id': 'count', col_plata: 'sum'}).reset_index()
    resumen_dia.columns = ['Fecha', 'Cant_Pedidos', 'Monto_Total_$']

    resumen_pago = df_macro.groupby(col_pago).agg({col_plata: 'sum', 'Id': 'count'}).reset_index()
    resumen_pago.columns = ['Medio de Pago', 'Total $', 'Cant. Operaciones']

    # 7. ESCRIBIR EN DASHBOARD_MACRO
    try:
        sheet_dash = spreadsheet.worksheet("Dashboard_Macro")
    except:
        sheet_dash = spreadsheet.add_worksheet(title="Dashboard_Macro", rows="200", cols="20")
    
    sheet_dash.clear()
    
    # Preparar datos para subir (convertidos a string)
    datos_dia = [resumen_dia.columns.tolist()] + resumen_dia.astype(str).values.tolist()
    datos_pago = [resumen_pago.columns.tolist()] + resumen_pago.astype(str).values.tolist()
    
    sheet_dash.update(range_name='A1', values=[["HISTÓRICO TEMPORAL"]] + datos_dia)
    sheet_dash.update(range_name='E1', values=[["HISTÓRICO MEDIOS DE PAGO"]] + datos_pago)
    
    # 8. GRÁFICOS
    crear_graficos_bi(spreadsheet, sheet_dash.id, len(resumen_dia), len(resumen_pago))
    print("✅ Proceso de sincronización y dashboard finalizado.")

def crear_graficos_bi(spreadsheet, sheet_id, l_dia, l_pago):
    # Solicitud para el gráfico de barras/líneas combinado
    requests = [
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Macro: Ventas y Comandas por Día",
                        "basicChart": {
                            "chartType": "COMBO",
                            "legendPosition": "BOTTOM_LEGEND",
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1+l_dia, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1+l_dia, "startColumnIndex": 1, "endColumnIndex": 2}]}}, "type": "BAR", "targetAxis": "LEFT_AXIS"},
                                {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1+l_dia, "startColumnIndex": 2, "endColumnIndex": 3}]}}, "type": "LINE", "targetAxis": "RIGHT_AXIS"}
                            ]
                        }
                    },
                    "position": {"newSheet": False, "overlayPosition": {"anchorCell": {"sheetId": sheet_id, "rowIndex": 12, "columnIndex": 0}}}
                }
            }
        }
    ]
    try:
        spreadsheet.batch_update({"requests": requests})
    except:
        pass

if __name__ == "__main__":
    ejecutar_sincronizacion_macro()

