import os
import time
import zipfile
import shutil
import pandas as pd
import gspread
import json
import numpy as np
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURACIÓN DE RUTAS ---
base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel")
nombre_final = "ventas.xls"
ruta_excel_final = os.path.join(temp_excel_path, nombre_final)

os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

# --- CONFIGURACIÓN CHROME (NUBE) ---
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": base_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

def ejecutar_todo():
    # --- PARTE 1: DESCARGA CON SELENIUM ---
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    wait = WebDriverWait(driver, 25)

    try:
        print("1. Iniciando sesión en Fudo...")
        driver.get("https://app-v2.fu.do/app/#!/sales")
        wait.until(EC.presence_of_element_located((By.ID, "user"))).send_keys("gestion@bigsaladsquinta")
        driver.find_element(By.ID, "password").send_keys("BigQuinta22")
        driver.find_element(By.ID, "password").submit()
        
        print("2. Solicitando exportación de ventas...")
        exportar_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")))
        exportar_btn.click()
        
        # Espera para que el archivo se descargue (ajustar si el archivo es muy pesado)
        time.sleep(15) 

        # --- PARTE 2: EXTRACCIÓN DEL ZIP ---
        archivos_zip = [os.path.join(base_path, f) for f in os.listdir(base_path) if f.lower().endswith(".zip")]
        
        if not archivos_zip:
            raise Exception("No se encontró el archivo ZIP descargado.")
        
        zip_file = max(archivos_zip, key=os.path.getctime)
        print(f"3. Extrayendo archivo: {zip_file}")

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            nombres = zip_ref.namelist()
            if nombres:
                archivo_interno = nombres[0]
                zip_ref.extract(archivo_interno, base_path)
                
                ruta_extraida = os.path.join(base_path, archivo_interno)
                if os.path.exists(ruta_excel_final): os.remove(ruta_excel_final)
                shutil.move(ruta_extraida, ruta_excel_final)
                print(f"✅ Archivo listo en: {ruta_excel_final}")

        os.remove(zip_file) # Limpieza

        # --- PARTE 3: PROCESAMIENTO CON PANDAS ---
        print("4. Procesando datos con Pandas...")
        # Cargamos las diferentes hojas
        df_v = pd.read_excel(ruta_excel_final, sheet_name='Ventas', skiprows=3)
        df_a = pd.read_excel(ruta_excel_final, sheet_name='Adiciones')
        df_d = pd.read_excel(ruta_excel_final, sheet_name='Descuentos')
        df_e = pd.read_excel(ruta_excel_final, sheet_name='Costos de Envío')

        df_v.columns = df_v.columns.str.strip()
        
        # Manejo de Fechas
        if not pd.api.types.is_datetime64_any_dtype(df_v['Creación']):
            df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], unit='D', origin='1899-12-30', errors='coerce')
        else:
            df_v['Fecha_DT'] = df_v['Creación']
        
        df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
        df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
        df_v['Hora_Int'] = df_v['Fecha_DT'].dt.hour 
        df_v['Turno'] = df_v['Hora_Int'].apply(lambda h: "Mañana" if h < 16 else "Noche")

        # Resúmenes de hojas adicionales
        prod_resumen = df_a.groupby('Id. Venta')['Producto'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
        prod_resumen.columns = ['Id', 'Detalle_Productos']

        desc_resumen = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
        desc_resumen.columns = ['Id', 'Descuento_Total']

        envio_resumen = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()
        envio_resumen.columns = ['Id', 'Costo_Envio']

        # Consolidación
        columnas_interes = ['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']
        consolidado = df_v[columnas_interes].merge(prod_resumen, on='Id', how='left')
        consolidado = consolidado.merge(desc_resumen, on='Id', how='left')
        consolidado = consolidado.merge(envio_resumen, on='Id', how='left')

        consolidado[['Descuento_Total', 'Costo_Envio']] = consolidado[['Descuento_Total', 'Costo_Envio']].fillna(0)
        consolidado['Detalle_Productos'] = consolidado['Detalle_Productos'].fillna("Sin detalle")

        # Filtro de AYER
        fecha_ayer = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
        print(f"5. Filtrando datos de ayer: {fecha_ayer}")
        consolidado = consolidado[consolidado['Fecha_Texto'] == fecha_ayer].copy()

        if consolidado.empty:
            print(f"⚠️ No se encontraron ventas para {fecha_ayer}. Fin del proceso.")
            return

        # --- PARTE 4: SUBIR A GOOGLE SHEETS ---
        print("6. Subiendo a Google Sheets...")
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        
        if creds_json:
            creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
        else:
            creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        
        client = gspread.authorize(creds)
        spreadsheet = client.open("Quinta Analisis Fudo")
        sheet_data = spreadsheet.worksheet("Hoja 1")
        
        sheet_data.clear()
        datos_finales = [consolidado.columns.values.tolist()] + consolidado.fillna("").astype(str).values.tolist()
        sheet_data.update(range_name='A1', values=datos_finales)
        
        print(f"🚀 ÉXITO: {len(consolidado)} ventas de ayer subidas a Hoja 1.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")
    finally:
        driver.quit()
        if os.path.exists(base_path):
            shutil.rmtree(base_path) # Limpiar todo al finalizar

if __name__ == "__main__":
    ejecutar_todo()
