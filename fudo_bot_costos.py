import os
import time
import zipfile
import shutil
import pandas as pd
import gspread
import json
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# --- CONFIGURACIÓN DE RUTAS ---
base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel2")
nombre_final = "productos.xls"

os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

# --- CONFIGURACIÓN CHROME ---
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

def ejecutar_sincronizacion_costos():
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    wait = WebDriverWait(driver, 25)

    try:
        # 1. LOGIN Y EXPORTAR DESDE FUDO
        print("Iniciando sesión en Fudo...")
        driver.get("https://app-v2.fu.do/app/#!/products")
        user_input = wait.until(EC.presence_of_element_located((By.ID, "user")))
        pass_input = driver.find_element(By.ID, "password")
        
        user_input.send_keys("gestion@bigsaladsquinta")
        pass_input.send_keys("BigQuinta22")
        pass_input.submit()
        
        print("Descargando archivo ZIP...")
        exportar_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadProducts()']")))
        exportar_btn.click()
        
        # Espera activa para la descarga
        time.sleep(15)

        # 2. PROCESAR EL ARCHIVO ZIP
        archivos_zip = [f for f in os.listdir(base_path) if f.lower().endswith(".zip")]
        
        if not archivos_zip:
            raise Exception("No se encontró el archivo ZIP descargado.")

        zip_path = os.path.join(base_path, archivos_zip[0])
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            archivo_interno = zip_ref.namelist()[0]
            zip_ref.extract(archivo_interno, base_path)
            
            ruta_excel = os.path.join(temp_excel_path, nombre_final)
            if os.path.exists(ruta_excel): os.remove(ruta_excel)
            shutil.move(os.path.join(base_path, archivo_interno), ruta_excel)
            print(f"Archivo extraído en: {ruta_excel}")

        # 3. SUBIR A GOOGLE SHEETS (MAESTROS_COSTOS)
        print("Conectando con Google Sheets...")
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        
        if creds_json:
            creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
        else:
            creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
            
        client = gspread.authorize(creds)
        spreadsheet = client.open("Quinta Analisis Fudo")
        sheet = spreadsheet.worksheet("Maestro_costos")

        # Leer el Excel (skiprows si Fudo tiene encabezados extra, usualmente no en productos)
        df = pd.read_excel(ruta_excel)
        
        # Limpiar la hoja y subir datos nuevos
        sheet.clear()
        
        # Convertimos a string para asegurar que las comas decimales no se pierdan
        datos_subir = [df.columns.values.tolist()] + df.fillna("").astype(str).values.tolist()
        sheet.update(range_name='A1', values=datos_subir)
        
        print("✅ Proceso completado: Maestros_costos actualizado en Google Sheets.")

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
    finally:
        driver.quit()
        # Limpieza de archivos temporales
        if os.path.exists(base_path):
            shutil.rmtree(base_path)

if __name__ == "__main__":
    ejecutar_sincronizacion_costos()

