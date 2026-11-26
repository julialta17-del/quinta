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
        # ⚠️ RECUERDA PONER EL ID DE TU GOOGLE SHEET AQUÍ
        spreadsheet = client.open_by_key("PEGA_AQUI_EL_ID_DE_TU_DOCUMENTO")
        sheet_data = spreadsheet.worksheet("Hoja 1")
        sheet_data.clear()

        # Red de seguridad para números
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
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('--remote-debugging-port=9222')

# --- CONFIGURACIÓN ANTI-BOTS ---
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option("useAutomationExtension", False)

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": base_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "profile.default_content_settings.popups": 0
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# PERMISO FORZADO PARA DESCARGAS EN HEADLESS
driver.execute_cdp_cmd('Page.setDownloadBehavior', {
    'behavior': 'allow',
    'downloadPath': base_path
})

driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
    'source': '''
        Object.defineProperty(navigator, 'webdriver', {
          get: () => undefined
        })
    '''
})

wait = WebDriverWait(driver, 30)

try:
    print("--- PASO: DESCARGA FUDO ---")
    driver.get("https://app-v2.fu.do/app/#!/sales")
    
    time.sleep(5) 

    print("Buscando formulario de login...")
    campo_usuario = wait.until(EC.presence_of_element_located((By.ID, "user")))
    campo_usuario.send_keys("gestion@bigsaladsquinta")
    
    driver.find_element(By.ID, "password").send_keys("BigQuinta22")
    driver.find_element(By.ID, "password").submit()
    
    print("Login enviado, esperando carga de ventas (puede demorar en la nube)...")
    wait_largo = WebDriverWait(driver, 90)
    
    # 1. Tocar el botón Exportar
    export_btn = wait_largo.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[ng-click='exportSalesAsync()']")))
    time.sleep(3)
    print("Solicitando generación del reporte asíncrono...")
    try:
        export_btn.click()
    except:
        driver.execute_script("arguments[0].click();", export_btn)

    # 2. Esperar 10 segundos
    print("Esperando 10 segundos para que el servidor de Fudo genere el reporte...")
    time.sleep(10)

    # 3. Ir a notificaciones (Campanita)
    print("Abriendo el panel de notificaciones...")
    notif_btn = wait_largo.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.icon-system-notifications-navbar")))
    
    # Clic normal para evitar bugs en la página
    try:
        notif_btn.click()
    except:
        driver.execute_script("arguments[0].click();", notif_btn)
    
    time.sleep(3) 
    
    # 4. Hacer clic en el mensaje de la notificación
    print("Abriendo el detalle de la notificación...")
    notif_item = wait_largo.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'reporte de ventas está listo')]")))
    
    # Usamos clic normal (físico) para que la página de Fudo no crashee
    try:
        notif_item.click()
    except:
        # Si algo lo tapa, clickeamos su contenedor padre por seguridad
        driver.execute_script("arguments[0].parentNode.click();", notif_item)
        
    time.sleep(5) 

    # 5. Buscar el enlace de descarga
    print("Buscando el enlace de descarga en el panel...")
    try:
        # Busca cualquier link que vaya al servidor de reportes de Fudo
        download_link = wait_largo.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='fudo-reports.s3']")))
    except Exception as fallback_e:
        print("⚠️ La pantalla de Fudo se trabó. Refrescando para forzar la carga...")
        driver.refresh() # Refresca la página en la notificación
        time.sleep(8)
        download_link = wait_largo.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='fudo-reports.s3']")))
    
    link_url = download_link.get_attribute("href")
    print("¡Enlace encontrado! Iniciando descarga directa...")
    driver.get(link_url)

    # 6. Esperar a que el archivo se guarde físicamente
    print("Esperando que finalice la descarga del archivo...")
    archivo_descargado = None
    
    for _ in range(60): 
        archivos = [f for f in os.listdir(base_path) 
                    if not f.lower().endswith('.crdownload') 
                    and not f.lower().endswith('.tmp') 
                    and os.path.isfile(os.path.join(base_path, f))]
        
        if archivos:
            time.sleep(2) 
            archivos_final = [f for f in os.listdir(base_path) 
                              if not f.lower().endswith('.crdownload') 
                              and not f.lower().endswith('.tmp') 
                              and os.path.isfile(os.path.join(base_path, f))]
            if archivos_final:
                archivo_descargado = archivos_final[0]
                break
        time.sleep(1)

    if not archivo_descargado: 
        raise Exception("No se descargó ningún archivo.")

    ruta_descarga = os.path.join(base_path, archivo_descargado)
    
    if archivo_descargado.lower().endswith('.zip'):
        print("📁 Archivo ZIP descargado exitosamente. Descomprimiendo...")
        with zipfile.ZipFile(ruta_descarga, 'r') as z:
            archivo_interno = z.namelist()[0]
            z.extract(archivo_interno, base_path)
            if os.path.exists(ruta_excel): os.remove(ruta_excel)
            shutil.move(os.path.join(base_path, archivo_interno), ruta_excel)
    else:
        print("📊 Archivo Excel directo descargado exitosamente.")
        if os.path.exists(ruta_excel): os.remove(ruta_excel)
        shutil.move(ruta_descarga, ruta_excel)

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

    hoy = datetime.now().date()
    df_v = df_v[df_v['Fecha_DT'].dt.date == hoy]

    if df_v.empty:
        print(f"⚠️ No hay ventas para el día de hoy ({hoy.strftime('%d/%m/%Y')}). Deteniendo ejecución sin error.")
    else:
        print(f"✅ Filas de hoy ({hoy.strftime('%d/%m/%Y')}): {len(df_v)}")

        # Extracción y cruce de datos hoja por hoja
        df_a = pd.read_excel(ruta_excel, sheet_name='Adiciones')
        df_d = pd.read_excel(ruta_excel, sheet_name='Descuentos')
        df_e = pd.read_excel(ruta_excel, sheet_name='Costos de Envío')

        prod = df_a.groupby('Id. Venta')['Producto'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
        prod.columns = ['Id', 'Detalle_Productos']

        total_bruto = df_a.groupby('Id. Venta')['Precio'].sum().reset_index()
        total_bruto.columns = ['Id', 'Total_Productos_Bruto']

        desc = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
        desc.columns = ['Id', 'Descuento_Total']

        env = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()
        env.columns = ['Id', 'Costo_Envio']

        consolidado = df_v[['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']].copy()
        consolidado = consolidado.merge(prod,        on='Id', how='left')
        consolidado = consolidado.merge(total_bruto, on='Id', how='left')
        consolidado = consolidado.merge(desc,        on='Id', how='left')
        consolidado = consolidado.merge(env,         on='Id', how='left')
        consolidado = consolidado.fillna(0)

        cols_enteras = ['Id', 'Total', 'Total_Productos_Bruto', 'Descuento_Total', 'Costo_Envio']
        for col in cols_enteras:
            if col in consolidado.columns:
                consolidado[col] = limpiar_a_entero_string(consolidado[col])

        orden = [
            'Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente',
            'Total', 'Origen', 'Medio de Pago', 'Detalle_Productos',
            'Total_Productos_Bruto', 'Descuento_Total', 'Costo_Envio'
        ]
        consolidado = consolidado[orden]

        subir_a_google(consolidado)

except Exception as e:
    print(f"❌ ERROR EN SELENIUM:")
    print(f"Tipo de error: {type(e).__name__}")
    print(f"URL en el momento del error: {driver.current_url}")
    print(f"Detalle del error: {e}")
    
    print("\n" + "="*50)
    print("👀 ¿QUÉ ESTÁ VIENDO EL BOT EN LA PANTALLA AHORA MISMO?")
    print("="*50)
    try:
        texto_pantalla = driver.find_element(By.TAG_NAME, "body").text
        if not texto_pantalla.strip():
            print("[La pantalla está completamente en blanco]")
        else:
            print(texto_pantalla[:3000]) 
    except Exception as ex:
        print(f"No se pudo extraer el texto: {ex}")
    print("="*50 + "\n")
    
finally:
    driver.quit()
    print("--- PROCESO TERMINADO ---")
