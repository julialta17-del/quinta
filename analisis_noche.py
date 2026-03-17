import os
import time
import zipfile
import shutil
import pytz
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- FECHAS EN ZONA ARGENTINA ---
tz_arg = pytz.timezone("America/Argentina/Buenos_Aires")
ahora_arg = datetime.now(tz_arg)
fecha_desde = ahora_arg.strftime("%Y-%m-%d")                        # ej: 2026-03-16
fecha_hasta = (ahora_arg + timedelta(days=1)).strftime("%Y-%m-%d")  # ej: 2026-03-17

print(f"Rango de descarga: {fecha_desde} → {fecha_hasta}")
print(f"Hora actual Argentina: {ahora_arg.strftime('%d/%m/%Y %H:%M')}")

# --- RUTAS ---
base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel")
nombre_final = "ventas.xls"
os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

# --- CHROME HEADLESS ---
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

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# FIX CRÍTICO: habilitar descargas en modo headless
driver.execute_cdp_cmd("Page.setDownloadBehavior", {
    "behavior": "allow",
    "downloadPath": base_path
})

wait = WebDriverWait(driver, 30)


def esperar_descarga(carpeta, timeout=90):
    """Espera hasta que aparezca un ZIP completo (sin .crdownload)"""
    print("Esperando descarga...")
    fin = time.time() + timeout
    while time.time() < fin:
        archivos = os.listdir(carpeta)
        zips = [f for f in archivos if f.lower().endswith(".zip")]
        en_curso = [f for f in archivos if f.endswith(".crdownload")]
        if zips and not en_curso:
            print(f"Descarga completa: {zips[0]}")
            return True
        time.sleep(2)
    return False


try:
    # --- CREDENCIALES desde GitHub Secrets ---
    fudo_user = os.environ["FUDO_USER"]
    fudo_pass = os.environ["FUDO_PASS"]

    # --- LOGIN ---
    print("Iniciando sesión en Fudo...")
    driver.get("https://app-v2.fu.do/app/#!/sales")
    user_input = wait.until(EC.presence_of_element_located((By.ID, "user")))
    pass_input = driver.find_element(By.ID, "password")
    user_input.send_keys(fudo_user)
    pass_input.send_keys(fudo_pass)
    pass_input.submit()

    # --- ESPERAR QUE CARGUE EL FILTRO ---
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select[ng-model="type"]')))
    time.sleep(2)  # Angular necesita inicializar el scope

    # --- PASO 1: Cambiar tipo a "Rango" vía Angular scope ---
    print("Configurando filtro tipo Rango...")
    driver.execute_script("""
        var select = document.querySelector('select[ng-model="type"]');
        var scope = angular.element(select).scope();
        scope.$apply(function() {
            scope.type = 'r';
            scope.refreshType();
        });
    """)
    time.sleep(1)

    # --- PASO 2: Setear fechas ---
    # Desde: hoy a las 00:00 ART
    # Hasta: mañana a las 03:00 ART (= 00:00 ART del día siguiente = cierre de jornada)
    print(f"Seteando rango: {fecha_desde} 00:00 → {fecha_hasta} 03:00")
    driver.execute_script("""
        var input = document.querySelector('input[ng-model="model.t1"]');
        var scope = angular.element(input).scope();
        scope.$apply(function() {
            scope.model.t1 = arguments[0];
            scope.model.t2 = arguments[1];
            scope.t1 = '00:00';
            scope.t2 = '03:00';
            scope.refreshDate();
        });
    """, fecha_desde, fecha_hasta)
    time.sleep(1)

    # --- EXPORTAR ---
    print("Haciendo click en exportar...")
    exportar_btn = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "a[ert-download-file='downloadSales()']")
    ))
    exportar_btn.click()

    # --- ESPERAR DESCARGA ---
    if not esperar_descarga(base_path, timeout=90):
        raise Exception("Timeout: la descarga no se completó en 90 segundos")

    # --- PROCESAR ZIP ---
    archivos_zip = [
        os.path.join(base_path, f)
        for f in os.listdir(base_path)
        if f.lower().endswith(".zip")
    ]
    zip_file = max(archivos_zip, key=os.path.getmtime)  # getmtime = compatible con Linux
    print(f"Extrayendo: {zip_file}")

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        nombres = zip_ref.namelist()
        if not nombres:
            raise Exception("El ZIP está vacío")
        archivo_interno = nombres[0]
        zip_ref.extract(archivo_interno, base_path)
        ruta_extraida = os.path.join(base_path, archivo_interno)
        ruta_destino_final = os.path.join(temp_excel_path, nombre_final)
        if os.path.exists(ruta_destino_final):
            os.remove(ruta_destino_final)
        shutil.move(ruta_extraida, ruta_destino_final)
        print(f"¡ÉXITO! Archivo guardado en: {ruta_destino_final}")

    os.remove(zip_file)
    print("ZIP temporal borrado.")

except Exception as e:
    print(f"Error crítico: {e}")
    raise  # Que GitHub marque el job como fallido

finally:
    driver.quit()
    print("Proceso terminado.")
