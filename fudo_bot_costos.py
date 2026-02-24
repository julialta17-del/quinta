import os
import time
import zipfile
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# --- RUTAS RELATIVAS (Funcionan en GitHub y Windows) ---
base_path = os.path.join(os.getcwd(), "descargas")
temp_excel_path = os.path.join(base_path, "temp_excel2")
nombre_final = "productos.xls"

os.makedirs(base_path, exist_ok=True)
os.makedirs(temp_excel_path, exist_ok=True)

chrome_options = Options()
chrome_options.add_argument('--headless') # Modo nube
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": base_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 25)

try:
    # 1. LOGIN Y EXPORTAR
    driver.get("https://app-v2.fu.do/app/#!/products")
    user_input = wait.until(EC.presence_of_element_located((By.ID, "user")))
    pass_input = driver.find_element(By.ID, "password")
    user_input.send_keys("gestion@bigsaladsquinta")
    pass_input.send_keys("BigQuinta22")
    pass_input.submit()
    
    exportar_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[ert-download-file='downloadProducts()']")))
    exportar_btn.click()
    print("Exportación de productos iniciada...")
    
    time.sleep(10)

    # 2. PROCESAR ZIP
    archivos_zip = [f for f in os.listdir(base_path) if f.lower().endswith(".zip")]
    
    if not archivos_zip:
        print("Error: No se encontró el ZIP de productos.")
    else:
        zip_path = os.path.join(base_path, archivos_zip[0])
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            archivo_interno = zip_ref.namelist()[0]
            zip_ref.extract(archivo_interno, base_path)
            
            ruta_destino = os.path.join(temp_excel_path, nombre_final)
            if os.path.exists(ruta_destino): os.remove(ruta_destino)
            shutil.move(os.path.join(base_path, archivo_interno), ruta_destino)
            print(f"Éxito: Productos guardados en {ruta_destino}")

        os.remove(zip_path)

except Exception as e:
    print(f"Error crítico: {e}")
finally:
    driver.quit()

