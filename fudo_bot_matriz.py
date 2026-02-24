import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json

def ejecutar_matriz_estrella():
    print("Calculando Matriz Estratégica...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Analisis Fudo")
    
    df_hist = pd.DataFrame(spreadsheet.worksheet("Historico").get_all_records())
    df_costos = pd.DataFrame(spreadsheet.worksheet("Maestro_Costos").get_all_records())
    
    df_hist['Lista_Prod'] = df_hist['Detalle_Productos'].str.split(',')
    df_items = df_hist.explode('Lista_Prod')
    df_items['Lista_Prod'] = df_items['Lista_Prod'].str.strip()
    
    pop = df_items['Lista_Prod'].value_counts().reset_index()
    pop.columns = ['Nombre', 'Cantidad_Vendida']
    matriz = pd.merge(pop, df_costos, on='Nombre', how='inner')
    
    med_v = matriz['Cantidad_Vendida'].median()
    med_m = matriz['Margen_$'].median()

    def bcg(row):
        if row['Cantidad_Vendida'] >= med_v and row['Margen_$'] >= med_m: return "⭐ ESTRELLA"
        if row['Cantidad_Vendida'] >= med_v: return "🐴 CABALLITO"
        if row['Margen_$'] >= med_m: return "💎 JOYA"
        return "🗑️ PERRO"

    matriz['Categoria_Estrategica'] = matriz.apply(bcg, axis=1)
    
    sheet_mat = spreadsheet.worksheet("Matriz_Productos")
    sheet_mat.clear()
    sheet_mat.update([matriz.columns.tolist()] + matriz.astype(str).values.tolist())
    print("✅ Matriz de productos actualizada.")

if __name__ == "__main__":
    ejecutar_matriz_estrella()
