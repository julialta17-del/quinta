from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
from webdriver_manager.chrome import ChromeDriverManager
import gspread
from google.oauth2.service_account import Credentials

# =====================
# GOOGLE SHEETS
# =====================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

import os
import json

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=SCOPES
)


client = gspread.authorize(creds)
sheet = client.open("Quinta clientes PEYA").get_worksheet(0)

print("Conectado a Google Sheets OK")

# =====================
# CHROME (APTO GITHUB)
# =====================
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-gpu")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

wait = WebDriverWait(driver, 40)

# =====================
# 1. LOGIN
# =====================
driver.get("https://app-v2.fu.do/app/#!/delivery")

user_input = wait.until(EC.presence_of_element_located((By.ID, "user")))
pass_input = driver.find_element(By.ID, "password")

user_input.send_keys("gestion@bigsaladsquinta")
pass_input.send_keys("BigQuinta22")
pass_input.submit()

print("Login OK")

# =====================
# 2. REFRESH
# =====================
time.sleep(6)
print("Actualizando página...")
driver.refresh()
time.sleep(12)

# =====================
# 3. IR A ENTREGADOS
# =====================
try:
    entregados = wait.until(
        EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'ENTREGADOS')]"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", entregados)
    time.sleep(2)
    driver.execute_script("arguments[0].click();", entregados)
    print("Pestaña ENTREGADOS abierta.")
    time.sleep(6)
except:
    print("No se pudo clickear ENTREGADOS, continuando...")

# =====================
# 4. MOSTRAR MÁS (1 CLIC)
# =====================
try:
    btn_mas = driver.find_elements(By.XPATH, "//*[contains(text(),'Mostrar más')]")
    if btn_mas and btn_mas[0].is_displayed():
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_mas[0])
        time.sleep(2)
        driver.execute_script("arguments[0].click();", btn_mas[0])
        print("Botón 'Mostrar más' presionado.")
        time.sleep(10)
except:
    print("No se encontró botón extra.")

# =====================
# 5. TRANSCRIPCIÓN
# =====================
print("Iniciando transcripción reforzada...")

filas = driver.find_elements(By.XPATH, "//tr[td]")
print(f"Pedidos detectados: {len(filas)}")

for fila in filas:
    try:
        celdas = fila.find_elements(By.TAG_NAME, "td")
        if len(celdas) >= 5:

            id_p = celdas[0].text.strip()
            hora = celdas[1].text.strip()
            cli = celdas[4].text.strip()
            tot = celdas[-1].text.strip()

            telefono = "No encontrado"

            # Buscar +54 en cualquier celda
            for celda in celdas:
                texto_celda = celda.text.strip()
                if "+54" in texto_celda:
                    telefono = texto_celda
                    break

            if id_p.lower() == "id" or id_p == "":
                continue

            sheet.append_row([id_p, hora, telefono, cli, tot])
            print(f"ÉXITO: Guardado pedido {id_p} | Tel: {telefono}")

    except Exception as e:
        print(f"Error en fila: {e}")

driver.quit()
print("PROCESO TERMINADO")
