import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def ejecutar_bot_clientes():
    # --- CONEXIÓN SEGURA A GOOGLE ---
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    
    if creds_json:
        # Modo GitHub
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        # Modo Local
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        
    client = gspread.authorize(creds)
    sheet = client.open("Quinta clientes PEYA").get_worksheet(0)

    # --- CONFIGURACIÓN CHROME (MODO NUBE) ---
    chrome_options = Options()
    chrome_options.add_argument('--headless') # Indispensable para GitHub
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    service = Service(ChromeDriverManager().install()) 
    driver = webdriver.Chrome(service=service, options=chrome_options)
    wait = WebDriverWait(driver, 30)

    try:
        # 1. LOGIN
        driver.get("https://app-v2.fu.do/app/#!/delivery")
        user_input = wait.until(EC.presence_of_element_located((By.ID, "user")))
        pass_input = driver.find_element(By.ID, "password")
        user_input.send_keys("gestion@bigsaladsquinta")
        pass_input.send_keys("BigQuinta22")
        pass_input.submit()
        print("Login OK")

        time.sleep(5)
        driver.refresh()
        time.sleep(15) 

        # 2. ENTREGADOS
        try:
            entregados = wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'ENTREGADOS')]")))
            driver.execute_script("arguments[0].click();", entregados)
            print("Sección ENTREGADOS abierta.")
        except:
            print("Sección entregados no clickeable, continuando...")

        # 3. TRANSCRIBIR
        time.sleep(5)
        filas = driver.find_elements(By.XPATH, "//tr[td]")
        print(f"Pedidos detectados: {len(filas)}")

        for fila in filas:
            celdas = fila.find_elements(By.TAG_NAME, "td")
            if len(celdas) >= 5:
                id_p = celdas[0].text.strip()
                hora = celdas[1].text.strip()
                tel = celdas[3].text.strip()
                cli = celdas[4].text.strip()
                tot = celdas[-1].text.strip()

                if id_p.lower() == "id" or not id_p:
                    continue

                sheet.append_row([id_p, hora, tel, cli, tot])
                print(f"Guardado pedido {id_p}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    ejecutar_bot_clientes()
