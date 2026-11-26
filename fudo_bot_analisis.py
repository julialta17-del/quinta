import os
import time
import re
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

def limpiar_numero(val):
    if not val or str(val).strip() == "": return "0"
    match = re.search(r'([\d\.\,]+)', str(val))
    if match:
        v = match.group(1).replace('.', '').replace(',', '.')
        try:
            return str(int(round(float(v))))
        except:
            pass
    return "0"

def subir_a_google(df_consolidado):
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

        datos_finales = [df_consolidado.columns.values.tolist()] + \
                         df_consolidado.fillna("").astype(str).values.tolist()

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
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Configurar hora Argentina en la nube
driver.execute_cdp_cmd('Emulation.setTimezoneOverride', {
    'timezoneId': 'America/Argentina/Buenos_Aires'
})

# Aumentamos el tiempo máximo de espera a 60 segundos
wait_largo = WebDriverWait(driver, 60)

try:
    print("--- PASO: EXTRACCIÓN DE DATOS EN PANTALLA ---")
    driver.get("https://app-v2.fu.do/app/#!/sales")
    time.sleep(5) 

    print("Iniciando sesión...")
    wait_largo.until(EC.presence_of_element_located((By.ID, "user"))).send_keys("gestion@bigsaladsquinta")
    driver.find_element(By.ID, "password").send_keys("BigQuinta22")
    driver.find_element(By.ID, "password").submit()
    
    print("Esperando que cargue la tabla de ventas...")
    time.sleep(15) 
    
    filas_xpath = "//table/tbody/tr"
    filas = driver.find_elements(By.XPATH, filas_xpath)
    total_filas = len(filas)
    print(f"Se encontraron {total_filas} registros en total. Analizando...")

    datos_ventas = []

    for i in range(total_filas):
        try: # SISTEMA ANTI-CHOQUES: Si falla una fila, anota el error y sigue con la próxima
            tr_list = driver.find_elements(By.XPATH, filas_xpath)
            if i >= len(tr_list): break
            fila = tr_list[i]
            celdas = fila.find_elements(By.TAG_NAME, "td")
            
            if len(celdas) < 8:
                continue
                
            estado = celdas[2].text.strip()
            if estado.lower() == "en curso":
                continue
                
            hora_inicio_str = celdas[0].text.strip()
            cliente_str = celdas[5].text.strip()
            total_tabla_str = celdas[7].text.strip() 
            
            # 1. Scrolleamos con cuidado y hacemos clic
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", fila)
            time.sleep(1) # Pausa extra para que el scroll termine
            driver.execute_script("arguments[0].click();", fila)
            
            # 2. Esperamos al panel lateral correcto (ert-sales-show)
            panel = wait_largo.until(EC.presence_of_element_located((By.TAG_NAME, "ert-sales-show")))
            time.sleep(3) # Damos tiempo a Angular a cargar el texto interno
            
            # 3. Hacemos Scroll hasta el final del panel para forzar la carga de los Pagos
            driver.execute_script("""
                var panel_content = document.querySelector('ert-sales-show .content');
                if (panel_content) { panel_content.scrollTo(0, panel_content.scrollHeight); }
            """)
            time.sleep(1.5)
            
            url_actual = driver.current_url
            id_venta = url_actual.split('/')[-1] if '/sales/' in url_actual else ""
            
            # --- EXTRACCIÓN QUIRÚRGICA POR HTML (DOM) ---
            origen = ""
            medio_pago = ""
            detalle_productos = ""
            descuento = "0"
            costo_envio = "0"
            total_venta = total_tabla_str # Por defecto usamos el de la tabla
            
            # A. TOTAL
            try:
                total_venta = panel.find_element(By.CSS_SELECTOR, ".total strong").text
            except: pass

            # B. ORIGEN / TIPO
            try:
                labels = panel.find_elements(By.CSS_SELECTOR, "ul.list.show li label")
                for label in labels:
                    texto_label = label.text
                    if "Tipo" in texto_label:
                        if "Delivery" in texto_label: origen = "Pedidos Ya"
                        elif "Mostrador" in texto_label: origen = ""
                    if "Origen" in texto_label and "Pedidos" in texto_label:
                        origen = "Pedidos Ya"
            except: pass

            # C. ADICIONES (PRODUCTOS)
            try:
                adiciones = panel.find_elements(By.CSS_SELECTOR, "ul.additions li.addition")
                prods = []
                for ad in adiciones:
                    if "canceled" not in ad.get_attribute("class"):
                        try:
                            qty = ad.find_element(By.CSS_SELECTOR, ".count").text.strip()
                            name = ad.find_element(By.CSS_SELECTOR, ".item-label").text.strip()
                            name = name.split('\n')[0].strip()
                            prods.append(f"{qty} {name}".strip())
                        except: pass
                detalle_productos = " | ".join(prods)
            except: pass

            # D. DESCUENTOS Y ENVÍOS
            try:
                modifiers = panel.find_elements(By.CSS_SELECTOR, "li.bill-modifier")
                for mod in modifiers:
                    text = mod.text.lower()
                    try:
                        price = mod.find_element(By.CSS_SELECTOR, ".price").text
                        if "descuento" in text:
                            descuento = price
                        elif "envío" in text or "envio" in text:
                            costo_envio = price
                    except: pass
            except: pass

            # E. MEDIOS DE PAGO
            try:
                pagos_seccion = panel.find_element(By.CSS_SELECTOR, "ui-view[name='edit_payments']")
                pagos_items = pagos_seccion.find_elements(By.CSS_SELECTOR, "li")
                medios = []
                for p in pagos_items:
                    if "canceled" not in p.get_attribute("class"):
                        try:
                            nombre_pago = p.find_element(By.CSS_SELECTOR, ".item-label").text.strip()
                            if nombre_pago:
                                medios.append(nombre_pago)
                        except: pass
                medio_pago = " | ".join(medios)
            except: pass

            # --- FECHAS Y TURNOS ---
            try:
                dt_obj = datetime.strptime(hora_inicio_str, "%d/%m/%y %H:%M:%S")
                fecha_texto = dt_obj.strftime("%d/%m/%Y")
                hora_exacta = dt_obj.strftime("%H:%M")
                turno = "Mañana" if dt_obj.hour < 16 else "Noche"
            except:
                fecha_texto = ""
                hora_exacta = hora_inicio_str
                turno = ""

            # --- GUARDAR FILA ---
            datos_ventas.append({
                'Id': id_venta,
                'Fecha_Texto': fecha_texto,
                'Hora_Exacta': hora_exacta,
                'Turno': turno,
                'Cliente': cliente_str,
                'Total': limpiar_numero(total_venta),
                'Origen': origen,
                'Medio de Pago': medio_pago,
                'Detalle_Productos': detalle_productos,
                'Descuento_Total': limpiar_numero(descuento),
                'Costo_Envio': limpiar_numero(costo_envio),
                'Costo_Total_Venta': "",
                'Comision_PeYa_$': "",
                'Comision_Tienda_Online_$': "",
                'Margen_Neto_$': "",
                'Margen_Neto_%': "",
                'Costo_MO_$': "",
                'Pedidos_en_Turno': ""
            })

        except Exception as error_fila:
            print(f"⚠️ Error procesando la fila {i+1} de {total_filas}. Saltando a la siguiente. Detalle: {error_fila}")
            continue

    print(f"✅ Se extrajeron {len(datos_ventas)} ventas cerradas con éxito.")
    
    columnas_orden = [
        'Id', 'Fecha_Texto', 'Hora_Exacta', 'Turno', 'Cliente', 'Total', 'Origen', 'Medio de Pago', 
        'Detalle_Productos', 'Descuento_Total', 'Costo_Envio', 'Costo_Total_Venta', 
        'Comision_PeYa_$', 'Comision_Tienda_Online_$', 'Margen_Neto_$', 'Margen_Neto_%', 
        'Costo_MO_$', 'Pedidos_en_Turno'
    ]
    
    if datos_ventas:
        df_consolidado = pd.DataFrame(datos_ventas, columns=columnas_orden)
        subir_a_google(df_consolidado)
    else:
        print("⚠️ No se generó el Excel porque no había ventas válidas.")

except Exception as e:
    print(f"❌ ERROR CRÍTICO EN EXTRACCIÓN:")
    print(f"Tipo de error: {type(e).__name__}")
    print(f"Detalle: {e}")
    
finally:
    driver.quit()
    print("--- PROCESO TERMINADO ---")
