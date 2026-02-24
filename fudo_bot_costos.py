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

        # 3. LOGICA DE NEGOCIO CON PANDAS
        print("Procesando lógicas de costos y descuentos...")
        df = pd.read_excel(ruta_excel)
        
        # Seleccionamos y renombramos las columnas base de Fudo
        # Ajusta 'Nombre', 'Precio' y 'Costo' según como vengan exactamente en tu Excel de Fudo
        df = df[['Nombre', 'Precio', 'Costo']].copy()
        
        # Convertimos a numérico por seguridad
        df['Precio'] = pd.to_numeric(df['Precio'], errors='coerce').fillna(0)
        df['Costo'] = pd.to_numeric(df['Costo'], errors='coerce').fillna(0)

        # Cálculos de Margen Estándar
        df['Margen_$'] = df['Precio'] - df['Costo']
        df['Margen_%'] = (df['Margen_$'] / df['Precio']).replace([float('inf'), -float('inf')], 0).fillna(0)

        # Lógica de Descuento 30%
        df['Precio_con_30%_Desc'] = df['Precio'] * 0.70
        df['Margen_$_con_Desc'] = df['Precio_con_30%_Desc'] - df['Costo']
        df['Margen_%_con_Desc'] = (df['Margen_$_con_Desc'] / df['Precio_con_30%_Desc']).replace([float('inf'), -float('inf')], 0).fillna(0)

        # 4. SUBIR A GOOGLE SHEETS
        print("Conectando con Google Sheets...")
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        
        if creds_json:
            creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
        else:
            creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
            
        client = gspread.authorize(creds)
        
        # REVISIÓN: Asegúrate que el nombre del archivo sea "Quinta Analisis Fudo" 
        # y la hoja sea "Maestros_costos" (en plural)
        spreadsheet = client.open("Quinta Analisis Fudo")
        sheet = spreadsheet.worksheet("Maestro_Costos")

        sheet.clear()
        
        # Preparamos los datos para subir
        # Formateamos los porcentajes para que se vean bien (ej: 0.35 -> 35%)
        df_subir = df.copy()
        # Puedes comentar estas dos líneas si prefieres los números brutos (0.35)
        # df_subir['Margen_%'] = (df_subir['Margen_%'] * 100).round(2).astype(str) + "%"
        # df_subir['Margen_%_con_Desc'] = (df_subir['Margen_%_con_Desc'] * 100).round(2).astype(str) + "%"

        datos_subir = [df_subir.columns.values.tolist()] + df_subir.fillna("").astype(str).values.tolist()
        sheet.update(range_name='A1', values=datos_subir)
        
        print("✅ Proceso completado: Maestro_costos actualizado con lógicas de descuento.")

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
    finally:
        driver.quit()
        if os.path.exists(base_path):
            shutil.rmtree(base_path)

if __name__ == "__main__":
    ejecutar_sincronizacion_costos()
