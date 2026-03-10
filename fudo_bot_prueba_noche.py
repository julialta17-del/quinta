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

# --- CONFIGURACIÓN ---
base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel")
nombre_final = "ventas.xls"
ruta_excel_final = os.path.join(temp_excel_path, nombre_final)

os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
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
        
        wait.until(EC.presence_of_element_located((By.ID, "user"))).send_keys("gestion@bigsaladsquinta")
        driver.find_element(By.ID, "password").send_keys("BigQuinta22")
        driver.find_element(By.ID, "password").submit()
        
        # --- FILTRADO POR RANGO (INYECCIÓN JS) ---
        print("📅 Forzando Rango Ayer-Hoy vía JavaScript...")
        
        # 1. Cambiar a modo Rango
        select_tipo = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select[ng-model='type']")))
        Select(select_tipo).select_by_value("string:r")
        time.sleep(3)

        # 2. Fechas en formato ISO (YYYY-MM-DD)
        ayer = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        hoy = datetime.now().strftime('%Y-%m-%d')

        # 3. Inyectamos los valores directamente en el modelo de Angular de Fudo
        # Esto salta la necesidad de usar el teclado
        script_js = f"""
            var scope = angular.element(document.querySelector('select[ng-model="type"]')).scope();
            scope.model.t1 = new Date('{ayer}T00:00:00');
            scope.model.t2 = new Date('{hoy}T23:59:59');
            scope.refreshDate();
            scope.$apply();
        """
        driver.execute_script(script_js)
        
        print(f"✅ Rango inyectado: {ayer} a {hoy}. Esperando carga...")
        time.sleep(12) 
        
        # Scroll para despertar la tabla
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # 4. EXPORTAR
        print("2. Solicitando exportación...")
        exportar_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")))
        driver.execute_script("arguments[0].click();", exportar_btn)
        
        print("📥 Descargando archivo...")
        time.sleep(35) 

        # --- PARTE 2: PROCESAMIENTO ---
        archivos_zip = [os.path.join(base_path, f) for f in os.listdir(base_path) if f.lower().endswith(".zip")]
        if not archivos_zip:
            raise Exception("No se descargó el archivo. Fudo no procesó el rango.")
        
        zip_file = max(archivos_zip, key=os.path.getctime)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            archivo_interno = zip_ref.namelist()[0]
            zip_ref.extract(archivo_interno, base_path)
            ruta_extraida = os.path.join(base_path, archivo_interno)
            if os.path.exists(ruta_excel_final): os.remove(ruta_excel_final)
            shutil.move(ruta_extraida, ruta_excel_final)

        print("4. Procesando datos con Pandas...")
        df_v = pd.read_excel(ruta_excel_final, sheet_name='Ventas', skiprows=3)
        df_v.columns = df_v.columns.str.strip()
        
        # Conversión de Fecha
        df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], errors='coerce')
        mask = df_v['Fecha_DT'].isna()
        if mask.any():
            df_v.loc[mask, 'Fecha_DT'] = pd.to_datetime(df_v.loc[mask, 'Creación'], unit='D', origin='1899-12-30', errors='coerce')
        
        df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
        df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
        df_v['Turno'] = df_v['Fecha_DT'].dt.hour.apply(lambda h: "Mañana" if h < 16 else "Noche")

        # --- SUBIR A GOOGLE SHEETS ---
        print(f"6. Subiendo {len(df_v)} ventas...")
        scope_gs = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope_gs) if creds_json else Credentials.from_service_account_file('credentials.json', scopes=scope_gs)
        
        client = gspread.authorize(creds)
        spreadsheet = client.open("Quinta Analisis Fudo")
        sheet_data = spreadsheet.worksheet("Hoja 1")
        
        sheet_data.clear()
        # Solo subimos las columnas que te interesan para no romper el histórico
        columnas_finales = ['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']
        # Si faltan datos de adiciones/descuentos en esta vuelta, procesamos solo lo principal
        datos_finales = [columnas_finales] + df_v[columnas_finales].fillna("").astype(str).values.tolist()
        sheet_data.update(range_name='A1', values=datos_finales)
        
        print(f"🚀 ÉXITO: {len(df_v)} pedidos subidos.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()
        if os.path.exists(base_path): shutil.rmtree(base_path)

if __name__ == "__main__":
    ejecutar_todo()
