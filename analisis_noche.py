import os
import time
import zipfile
import shutil
import json
import pandas as pd
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- 1. CONFIGURACIÓN DE FECHAS Y RUTAS ---
ahora = datetime.now()
ayer = ahora - timedelta(days=1)
manana = ahora + timedelta(days=1)
fecha_inicio = ayer.strftime("%Y-%m-%d")
fecha_fin = manana.strftime("%Y-%m-%d")

base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel")
ruta_excel = os.path.join(temp_excel_path, "ventas.xlsx")

os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

def subir_a_google(consolidado):
    print("--- PASO: CONEXIÓN A GOOGLE SHEETS ---")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # BUSCA EL SECRETO DE GOOGLE EN GITHUB
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    
    if not creds_json:
        raise Exception("Falta la variable de entorno GOOGLE_CREDENTIALS")

    # Carga las credenciales directamente desde la memoria (sin archivo físico)
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=scope)
    client = gspread.authorize(creds)
    
    try:
        spreadsheet = client.open("Quinta Analisis Fudo")
        sheet_data = spreadsheet.worksheet("Hoja 1")
        
        print("🧹 Limpiando Hoja 1...")
        sheet_data.clear()
        
        print("📝 Preparando datos...")
        datos_finales = [consolidado.columns.values.tolist()] + \
                         consolidado.fillna("").astype(str).values.tolist()
        
        sheet_data.update(range_name='A1', values=datos_finales)
        print("🚀 ¡DATOS PEGADOS CON ÉXITO EN GOOGLE SHEETS!")
        
    except Exception as e:
        print(f"❌ ERROR EN GOOGLE SHEETS: {e}")

# --- 2. SELENIUM: CONFIGURACIÓN HEADLESS ---
chrome_options = Options()
chrome_options.add_argument('--headless') 
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920,1080')

prefs = {
    "download.default_directory": base_path,
    "download.prompt_for_download": False,
}
chrome_options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 40)

try:
    print("--- PASO: DESCARGA FUDO ---")
    driver.get("https://app-v2.fu.do/app/#!/sales")
    
    # BUSCA USUARIO Y PASS EN LOS SECRETOS DE GITHUB
    fudo_user = os.getenv("FUDO_USER")
    fudo_pass = os.getenv("FUDO_PASS")

    if not fudo_user or not fudo_pass:
        raise Exception("Faltan las credenciales FUDO_USER o FUDO_PASS")

    # Login
    wait.until(EC.presence_of_element_located((By.ID, "user"))).send_keys(fudo_user)
    driver.find_element(By.ID, "password").send_keys(fudo_pass)
    driver.find_element(By.ID, "password").submit()

    print(f"Cargando rango: {fecha_inicio} a {fecha_fin}")
    time.sleep(10)

    # Selección de rango y exportación
    select_tipo = wait.until(EC.presence_of_element_located((By.XPATH, "//select[@ng-model='type']")))
    Select(select_tipo).select_by_value("string:r")
    time.sleep(3)

    input_desde = driver.find_element(By.XPATH, "//input[@ng-model='model.t1']")
    input_hasta = driver.find_element(By.XPATH, "//input[@ng-model='model.t2']")
    
    driver.execute_script("arguments[0].value = arguments[1];", input_desde, fecha_inicio)
    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", input_desde)
    driver.execute_script("arguments[0].value = arguments[1];", input_hasta, fecha_fin)
    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", input_hasta)
    
    print("Exportando reporte...")
    export_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")))
    driver.execute_script("arguments[0].click();", export_btn)

    # Esperar descarga del ZIP
    found_zip = None
    for i in range(45):
        zips = [f for f in os.listdir(base_path) if f.lower().endswith(".zip")]
        if zips:
            found_zip = zips[0]
            break
        time.sleep(1)
    
    if not found_zip: raise Exception("No se encontró el ZIP descargado.")

    # Extraer y procesar
    zip_path = os.path.join(base_path, found_zip)
    with zipfile.ZipFile(zip_path, 'r') as z:
        archivo_interno = z.namelist()[0]
        z.extract(archivo_interno, temp_excel_path)
        ruta_extraida = os.path.join(temp_excel_path, archivo_interno)
        if os.path.exists(ruta_excel): os.remove(ruta_excel)
        os.rename(ruta_extraida, ruta_excel)
    
    print("--- PASO: PROCESAMIENTO PANDAS ---")
    df_v = pd.read_excel(ruta_excel, sheet_name='Ventas', skiprows=3)
    df_v.columns = df_v.columns.str.strip()
    
    df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], unit='D', origin='1899-12-30', errors='coerce')
    df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
    df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
    df_v['Turno'] = df_v['Fecha_DT'].dt.hour.apply(lambda h: "Mañana" if h < 16 else "Noche")

    df_a = pd.read_excel(ruta_excel, sheet_name='Adiciones')
    df_d = pd.read_excel(ruta_excel, sheet_name='Descuentos')
    df_e = pd.read_excel(ruta_excel, sheet_name='Costos de Envío')

    prod = df_a.groupby('Id. Venta')['Producto'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
    desc = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
    env = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()

    cols = ['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']
    consolidado = df_v[cols].merge(prod, left_on='Id', right_on='Id. Venta', how='left')
    consolidado = consolidado.merge(desc, on='Id. Venta', how='left')
    consolidado = consolidado.merge(env, on='Id. Venta', how='left')
    
    consolidado.drop(columns=['Id. Venta'], inplace=True, errors='ignore')
    consolidado.fillna(0, inplace=True)
    consolidado.rename(columns={'Producto': 'Detalle_Productos', 'Valor_x': 'Descuento', 'Valor_y': 'Envio'}, inplace=True)

    subir_a_google(consolidado)

except Exception as e:
    print(f"❌ ERROR: {e}")
    driver.save_screenshot("error_fudo.png")
finally:
    driver.quit()
    print("--- PROCESO TERMINADO ---")
