import os
import time
import zipfile
import shutil
import json
import pandas as pd
import gspread
from datetime import datetime
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
ruta_excel = os.path.join(temp_excel_path, "ventas.xls")
os.makedirs(temp_excel_path, exist_ok=True)

def limpiar_a_entero_string(serie):
    temp = pd.to_numeric(
        serie.astype(str).str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0)
    return temp.round(0).astype(int).astype(str)

def subir_a_google(consolidado):
    print("--- PASO: CONEXIÓN A GOOGLE SHEETS ---")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")

    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)

    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open("Quinta Analisis Fudo")
        sheet_data = spreadsheet.worksheet("Hoja 1")
        sheet_data.clear()

        # Red de seguridad: cualquier numérico que haya escapado se convierte a int string
        for col in consolidado.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns:
            consolidado[col] = consolidado[col].fillna(0).round(0).astype(int).astype(str)

        datos_finales = [consolidado.columns.values.tolist()] + \
                         consolidado.fillna("").astype(str).values.tolist()

        sheet_data.update(range_name='A1', values=datos_finales)
        print("🚀 ¡DATOS ACTUALIZADOS EN HOJA 1!")

    except Exception as e:
        print(f"❌ ERROR EN GOOGLE SHEETS: {e}")

# --- SELENIUM ---
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_experimental_option("prefs", {"download.default_directory": base_path})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 30)

try:
    print("--- PASO: DESCARGA FUDO ---")
    driver.get("https://app-v2.fu.do/app/#!/sales")

    wait.until(EC.presence_of_element_located((By.ID, "user"))).send_keys("gestion@bigsaladsquinta")
    driver.find_element(By.ID, "password").send_keys("BigQuinta22")
    driver.find_element(By.ID, "password").submit()

    time.sleep(10)

    export_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")))
    driver.execute_script("arguments[0].click();", export_btn)

    found_zip = False
    for _ in range(30):
        zips = [f for f in os.listdir(base_path) if f.lower().endswith(".zip")]
        if zips:
            found_zip = True
            break
        time.sleep(1)

    if not found_zip: raise Exception("No se descargó el ZIP")

    zip_path = os.path.join(base_path, zips[0])
    with zipfile.ZipFile(zip_path, 'r') as z:
        archivo_interno = z.namelist()[0]
        z.extract(archivo_interno, base_path)
        if os.path.exists(ruta_excel): os.remove(ruta_excel)
        shutil.move(os.path.join(base_path, archivo_interno), ruta_excel)

    # --- PROCESAMIENTO ---
    print("--- PASO: PROCESAMIENTO Y LIMPIEZA TOTAL ---")
    df_v = pd.read_excel(ruta_excel, sheet_name='Ventas', skiprows=3)
    df_v.columns = df_v.columns.str.strip()

    if pd.api.types.is_datetime64_any_dtype(df_v['Creación']):
        df_v['Fecha_DT'] = df_v['Creación']
    else:
        df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], unit='D', origin='1899-12-30', errors='coerce')

    df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
    df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
    df_v['Turno'] = df_v['Fecha_DT'].dt.hour.apply(lambda h: "Mañana" if h < 16 else "Noche")

    # Filtro: solo filas con fecha de hoy
    hoy = datetime.now().date()
    df_v = df_v[df_v['Fecha_DT'].dt.date == hoy]

    if df_v.empty:
        raise Exception(f"No hay ventas para el día de hoy ({hoy.strftime('%d/%m/%Y')})")

    print(f"✅ Filas de hoy ({hoy.strftime('%d/%m/%Y')}): {len(df_v)}")

    # --- HOJAS ADICIONALES ---
    df_a = pd.read_excel(ruta_excel, sheet_name='Adiciones')
    df_d = pd.read_excel(ruta_excel, sheet_name='Descuentos')
    df_e = pd.read_excel(ruta_excel, sheet_name='Costos de Envío')

    # Detalle de productos por venta
    prod = df_a.groupby('Id. Venta')['Producto'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
    prod.columns = ['Id', 'Detalle_Productos']

    # Total bruto de productos por venta (suma de precios en Adiciones)
    total_bruto = df_a.groupby('Id. Venta')['Precio'].sum().reset_index()
    total_bruto.columns = ['Id', 'Total_Productos_Bruto']

    # Descuentos por venta
    desc = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
    desc.columns = ['Id', 'Descuento_Total']

    # Costos de envío por venta
    env = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()
    env.columns = ['Id', 'Costo_Envio']

    # --- CONSOLIDADO ---
    consolidado = df_v[['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']].copy()
    consolidado = consolidado.merge(prod,        on='Id', how='left')
    consolidado = consolidado.merge(total_bruto, on='Id', how='left')
    consolidado = consolidado.merge(desc,        on='Id', how='left')
    consolidado = consolidado.merge(env,         on='Id', how='left')
    consolidado = consolidado.fillna(0)

    # --- LIMPIEZA DE DECIMALES ---
    cols_enteras = ['Id', 'Total', 'Total_Productos_Bruto', 'Descuento_Total', 'Costo_Envio']
    for col in cols_enteras:
        if col in consolidado.columns:
            consolidado[col] = limpiar_a_entero_string(consolidado[col])

    # --- ORDEN FINAL DE COLUMNAS ---
    orden = [
        'Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente',
        'Total', 'Origen', 'Medio de Pago', 'Detalle_Productos',
        'Total_Productos_Bruto', 'Descuento_Total', 'Costo_Envio'
    ]
    consolidado = consolidado[orden]

    subir_a_google(consolidado)

except Exception as e:
    print(f"❌ ERROR: {e}")
finally:
    driver.quit()
    print("--- PROCESO TERMINADO ---")
