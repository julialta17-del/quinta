import os
import time
import zipfile
import shutil
import pandas as pd
import gspread
import json
import pytz
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# -------------------------------------------------------
# FECHAS AUTOMÁTICAS EN ZONA ARGENTINA
# -------------------------------------------------------
tz_arg = pytz.timezone("America/Argentina/Buenos_Aires")
ahora_arg = datetime.now(tz_arg)
fecha_desde = ahora_arg.strftime("%Y-%m-%d")
fecha_hoy_texto = ahora_arg.strftime("%d/%m/%Y")

if ahora_arg.hour >= 21:
    fecha_hasta = (ahora_arg + timedelta(days=1)).strftime("%Y-%m-%d")
    hora_hasta = "03:00"
    print(f"Modo NOCHE: {fecha_desde} 00:00 → {fecha_hasta} {hora_hasta}")
else:
    fecha_hasta = fecha_desde
    hora_hasta = "23:59"
    print(f"Modo DÍA: {fecha_desde} 00:00 → {fecha_hasta} {hora_hasta}")

# -------------------------------------------------------
# RUTAS
# -------------------------------------------------------
base_path = os.path.abspath(os.path.join(os.getcwd(), "descargas"))
temp_excel_path = os.path.join(base_path, "temp_excel")
nombre_final = "ventas.xls"
ruta_excel_final = os.path.join(temp_excel_path, nombre_final)
os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

# -------------------------------------------------------
# CHROME HEADLESS
# -------------------------------------------------------
chrome_options = Options()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": base_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})


def esperar_descarga(carpeta, timeout=90):
    print("Esperando que termine la descarga...")
    fin = time.time() + timeout
    while time.time() < fin:
        archivos = os.listdir(carpeta)
        print(f"  Archivos en carpeta: {archivos}")  # debug
        zips = [f for f in archivos if f.lower().endswith(".zip")]
        en_curso = [f for f in archivos if f.endswith(".crdownload")]
        if zips and not en_curso:
            print(f"Descarga completa: {zips[0]}")
            return True
        time.sleep(5)
    return False


def ejecutar_todo():
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Fix crítico: ruta absoluta para que Chrome sepa dónde descargar
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": base_path
    })

    wait = WebDriverWait(driver, 30)

    try:
        # -------------------------------------------------------
        # PARTE 1: LOGIN
        # -------------------------------------------------------
        print("1. Iniciando sesión en Fudo...")
        driver.get("https://app-v2.fu.do/app/#!/sales")
        wait.until(EC.presence_of_element_located((By.ID, "user"))).send_keys(os.environ["FUDO_USER"])
        driver.find_element(By.ID, "password").send_keys(os.environ["FUDO_PASS"])
        driver.find_element(By.ID, "password").submit()

        # -------------------------------------------------------
        # PARTE 2: FILTRO RANGO CON FECHAS AUTOMÁTICAS
        # -------------------------------------------------------
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select[ng-model="type"]')))
        time.sleep(2)

        print("2. Configurando filtro tipo Rango...")
        driver.execute_script("""
            var select = document.querySelector('select[ng-model="type"]');
            var scope = angular.element(select).scope();
            scope.$apply(function() {
                scope.type = 'r';
                scope.refreshType();
            });
        """)
        time.sleep(1)

        print(f"3. Seteando rango: {fecha_desde} 00:00 → {fecha_hasta} {hora_hasta}")
        driver.execute_script("""
            var input = document.querySelector('input[ng-model="model.t1"]');
            var scope = angular.element(input).scope();
            scope.$apply(function() {
                scope.model.t1 = arguments[0];
                scope.model.t2 = arguments[1];
                scope.t1 = '00:00';
                scope.t2 = arguments[2];
                scope.refreshDate();
            });
        """, fecha_desde, fecha_hasta, hora_hasta)
        time.sleep(3)  # Angular necesita tiempo para aplicar el filtro

        # -------------------------------------------------------
        # PARTE 3: EXPORTAR
        # -------------------------------------------------------
        print("4. Solicitando exportación...")
        exportar_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")
        ))

        # Cerrar el iframe de Userpilot/onboarding si existe
        try:
            driver.execute_script("""
                var iframe = document.getElementById('userpilotIframeContainer');
                if (iframe) iframe.remove();
            """)
            time.sleep(1)
            print("Modal de Userpilot cerrado.")
        except Exception:
            pass

        # Click directo vía JS para ignorar cualquier overlay
        driver.execute_script("arguments[0].click();", exportar_btn)

        # -------------------------------------------------------
        # PARTE 4: ESPERAR Y EXTRAER ZIP
        # -------------------------------------------------------
        if not esperar_descarga(base_path, timeout=90):
            raise Exception("Timeout: la descarga no se completó en 90 segundos")

        archivos_zip = [
            os.path.join(base_path, f)
            for f in os.listdir(base_path)
            if f.lower().endswith(".zip")
        ]
        if not archivos_zip:
            raise Exception("No se encontró el archivo ZIP descargado.")

        zip_file = max(archivos_zip, key=os.path.getmtime)
        print(f"5. Extrayendo: {zip_file}")

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            nombres = zip_ref.namelist()
            if not nombres:
                raise Exception("El ZIP está vacío")
            archivo_interno = nombres[0]
            zip_ref.extract(archivo_interno, base_path)
            ruta_extraida = os.path.join(base_path, archivo_interno)
            if os.path.exists(ruta_excel_final):
                os.remove(ruta_excel_final)
            shutil.move(ruta_extraida, ruta_excel_final)
            print(f"✅ Archivo listo en: {ruta_excel_final}")

        os.remove(zip_file)

        # -------------------------------------------------------
        # PARTE 5: PROCESAMIENTO CON PANDAS
        # -------------------------------------------------------
        print("6. Procesando datos con Pandas...")
        df_v = pd.read_excel(ruta_excel_final, sheet_name='Ventas', skiprows=3)
        df_a = pd.read_excel(ruta_excel_final, sheet_name='Adiciones')
        df_d = pd.read_excel(ruta_excel_final, sheet_name='Descuentos')
        df_e = pd.read_excel(ruta_excel_final, sheet_name='Costos de Envío')

        df_v.columns = df_v.columns.str.strip()

        if not pd.api.types.is_datetime64_any_dtype(df_v['Creación']):
            df_v['Fecha_DT'] = pd.to_datetime(df_v['Creación'], unit='D', origin='1899-12-30', errors='coerce')
        else:
            df_v['Fecha_DT'] = df_v['Creación']

        df_v['Fecha_Texto'] = df_v['Fecha_DT'].dt.strftime('%d/%m/%Y')
        df_v['Hora_Exacta'] = df_v['Fecha_DT'].dt.strftime('%H:%M')
        df_v['Hora_Int'] = df_v['Fecha_DT'].dt.hour
        df_v['Turno'] = df_v['Hora_Int'].apply(lambda h: "Mañana" if h < 16 else "Noche")

        prod_resumen = df_a.groupby('Id. Venta')['Producto'].apply(
            lambda x: ', '.join(x.astype(str))
        ).reset_index()
        prod_resumen.columns = ['Id', 'Detalle_Productos']

        desc_resumen = df_d.groupby('Id. Venta')['Valor'].sum().reset_index()
        desc_resumen.columns = ['Id', 'Descuento_Total']

        envio_resumen = df_e.groupby('Id. Venta')['Valor'].sum().reset_index()
        envio_resumen.columns = ['Id', 'Costo_Envio']

        columnas_interes = ['Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago']
        consolidado = df_v[columnas_interes].merge(prod_resumen, on='Id', how='left')
        consolidado = consolidado.merge(desc_resumen, on='Id', how='left')
        consolidado = consolidado.merge(envio_resumen, on='Id', how='left')
        consolidado[['Descuento_Total', 'Costo_Envio']] = consolidado[['Descuento_Total', 'Costo_Envio']].fillna(0)
        consolidado['Detalle_Productos'] = consolidado['Detalle_Productos'].fillna("Sin detalle")

        print(f"7. Filtrando datos de HOY: {fecha_hoy_texto}")
        consolidado = consolidado[consolidado['Fecha_Texto'] == fecha_hoy_texto].copy()

        if consolidado.empty:
            print(f"⚠️ No se encontraron ventas para hoy {fecha_hoy_texto}. Fin del proceso.")
            return

        # -------------------------------------------------------
        # PARTE 6: SUBIR A GOOGLE SHEETS
        # -------------------------------------------------------
        print("8. Subiendo a Google Sheets...")
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
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

        print(f"🚀 ÉXITO: {len(consolidado)} ventas de HOY subidas a Hoja 1.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        raise

    finally:
        driver.quit()
        if os.path.exists(base_path):
            shutil.rmtree(base_path)


if __name__ == "__main__":
    ejecutar_todo()
