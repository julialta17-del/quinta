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
from selenium.webdriver.support.ui import WebDriverWait, Select
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

# --- CONFIGURACIÓN CHROME (MODO NUBE) ---
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": base_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

def ejecutar_todo():
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    wait = WebDriverWait(driver, 40)

    try:
        print("1. Iniciando sesión en Fudo...")
        driver.get("https://app-v2.fu.do/app/#!/sales")
        
        # Login
        wait.until(EC.presence_of_element_located((By.ID, "user"))).send_keys("gestion@bigsaladsquinta")
        driver.find_element(By.ID, "password").send_keys("BigQuinta22")
        driver.find_element(By.ID, "password").submit()
        
        # --- NUEVA LÓGICA: FILTRADO POR RANGO ---
        print("📅 Configurando filtro por Rango (Ayer a Hoy)...")
        
        # 1. Cambiar a modo "Rango"
        select_tipo = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select[ng-model='type']")))
        Select(select_tipo).select_by_value("string:r")
        time.sleep(2)

        # 2. Definir fechas
        ayer = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') # Formato yyyy-mm-dd para el input date
        hoy = datetime.now().strftime('%Y-%m-%d')

        # 3. Cargar fecha "Desde" (model.t1)
        input_desde = driver.find_element(By.CSS_SELECTOR, "input[ng-model='model.t1']")
        input_desde.clear()
        input_desde.send_keys(ayer)

        # 4. Cargar fecha "Hasta" (model.t2)
        input_hasta = driver.find_element(By.CSS_SELECTOR, "input[ng-model='model.t2']")
        input_hasta.clear()
        input_hasta.send_keys(hoy)
        
        # 5. Forzar actualización (clic en cualquier lado o esperar)
        print(f"✅ Rango establecido: {ayer} hasta {hoy}")
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # 6. EXPORTAR
        print("2. Solicitando exportación...")
        exportar_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")))
        driver.execute_script("arguments[0].click();", exportar_btn)
        
        print("📥 Descargando archivo...")
        time.sleep(30) 

        # --- PARTE 2: EXTRACCIÓN ---
        archivos_zip = [os.path.join(base_path, f) for f in os.listdir(base_path) if f.lower().endswith(".zip")]
        if not archivos_zip:
            raise Exception("No se encontró el ZIP.")
        
        zip_file = max(archivos_zip, key=os.path.getctime)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            archivo_interno = zip_ref.namelist()[0]
            zip_ref.extract(archivo_interno, base_path)
            ruta_extraida = os.path.join(base_path, archivo_interno)
            if os.path.exists(ruta_excel_final): os.remove(ruta_excel_final)
            shutil.move(ruta_extraida, ruta_excel_final)

        # --- PARTE 3: PANDAS (PROCESA TODO EL RANGO) ---
        print("4. Procesando datos con Pandas...")
        df_v = pd.read_excel(ruta_excel_final, sheet_name='Ventas', skiprows=3)
        df_a = pd.read_excel(ruta_excel_final, sheet_name='Adiciones')
        df_d = pd.read_excel(ruta_excel_final, sheet_name='Descuentos')
        df_e = pd.read_excel(ruta_excel_final, sheet_name='Costos de Envío')

        df_v.columns = df_v.columns.str.strip()
        
        # Conversión de Fecha
        df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], errors='coerce')
        mask_nulos = df_v['Fecha_DT'].isna()
        if mask_nulos.any():
            df_v.loc[mask_nulos, 'Fecha_DT'] = pd.to_datetime(df_v.loc[mask_nulos, 'Creación'], unit='D', origin='1899-12-30', errors='coerce')
        
        df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
        df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
        df_v['Turno'] = df_v['Fecha_DT'].dt.hour.apply(lambda h: "Mañana" if h < 16 else "Noche")

        # Uniones (Adiciones, Descuentos, Envío)
        prod_resumen = df_a.groupby('Id. Venta')['Producto'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
        prod_resumen.columns = ['Id', 'Detalle_Productos']
        desc_resumen = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
        desc_resumen.columns = ['Id', 'Descuento_Total']
        envio_resumen = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()
        envio_resumen.columns = ['Id', 'Costo_Envio']

        columnas = ['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']
        consolidado = df_v[columnas].merge(prod_resumen, on='Id', how='left')
        consolidado = consolidado.merge(desc_resumen, on='Id', how='left')
        consolidado = consolidado.merge(envio_resumen, on='Id', how='left')

        consolidado[['Descuento_Total', 'Costo_Envio']] = consolidado[['Descuento_Total', 'Costo_Envio']].fillna(0)
        consolidado['Detalle_Productos'] = consolidado['Detalle_Productos'].fillna("Sin detalle")

        # --- PARTE 4: SUBIR A GOOGLE SHEETS ---
        print(f"6. Subiendo {len(consolidado)} ventas a Sheets...")
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
        
        print(f"🚀 ÉXITO TOTAL: Se subieron {len(consolidado)} pedidos (Rango Ayer-Hoy).")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()
        if os.path.exists(base_path): shutil.rmtree(base_path)

if __name__ == "__main__":
    ejecutar_todo()
