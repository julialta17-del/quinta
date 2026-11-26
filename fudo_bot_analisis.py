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
        spreadsheet = client.open_by_key("19ldgGkRWMylSAJNpcCOWN3tvsyYQ-4L6Rt8cZ1F3Jxw")
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

# Engañamos a GitHub para que use la hora de Mendoza, Argentina
driver.execute_cdp_cmd('Emulation.setTimezoneOverride', {
    'timezoneId': 'America/Argentina/Buenos_Aires'
})

wait_largo = WebDriverWait(driver, 40)

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
    print(f"Se encontraron {total_filas} registros en pantalla. Analizando ventas cerradas...")

    datos_ventas = []

    for i in range(total_filas):
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
        total_str = celdas[7].text.strip() 
        
        driver.execute_script("arguments[0].scrollIntoView(true);", fila)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", fila)
        time.sleep(2.5) # Espera a que el panel lateral derecho se dibuje
        
        url_actual = driver.current_url
        id_venta = url_actual.split('/')[-1] if '/sales/' in url_actual else ""
        
        try:
            panel = driver.find_element(By.CSS_SELECTOR, "aside.page__content__aside")
            texto_panel = panel.text
        except:
            texto_panel = ""
            
        # ==========================================
        # 1. ORIGEN INTELIGENTE
        # ==========================================
        origen = ""
        if re.search(r'Tipo\s+Mostrador', texto_panel, re.IGNORECASE):
            origen = ""
        elif re.search(r'Tipo\s+Delivery', texto_panel, re.IGNORECASE):
            origen = "Pedidos Ya"
        else:
            origen_match = re.search(r'Origen\s+(.+)', texto_panel)
            origen = origen_match.group(1).strip() if origen_match else ""

        # ==========================================
        # 2. MEDIO DE PAGO INTELIGENTE
        # ==========================================
        medio_pago = ""
        pagos_match = re.search(r'PAGOS\n(.*?)(?=\nPROPINAS|\nTOTAL|\n|$)', texto_panel, re.DOTALL | re.IGNORECASE)
        if pagos_match:
            lineas_pagos = [l.strip() for l in pagos_match.group(1).split('\n') if l.strip()]
            medios = []
            for l in lineas_pagos:
                if re.match(r'^\$?\s*[\d\.,]+$', l): continue # Ignorar precios sueltos
                l_clean = re.sub(r'\s*\$?\s*\d+[\d\.,]*$', '', l).strip() # Quitar el monto final
                if l_clean:
                    medios.append(l_clean)
            medio_pago = " | ".join(medios)

        # ==========================================
        # 3. DETALLE PRODUCTOS INTELIGENTE
        # ==========================================
        detalle_productos = ""
        adiciones_match = re.search(r'ADICIONES\n(.*?)(?=\nTotal:|\nSubtotal|\nPAGOS)', texto_panel, re.DOTALL | re.IGNORECASE)
        if adiciones_match:
            lineas_ad = [l.strip() for l in adiciones_match.group(1).split('\n') if l.strip()]
            productos_brutos = []
            for l in lineas_ad:
                if re.match(r'^\$?\s*[\d\.,]+$', l) and not re.match(r'^\d+$', l):
                    continue # Ignorar precio suelto
                l_clean = re.sub(r'\s*\$?\s*\d+[\d\.,]*$', '', l).strip()
                if l_clean:
                    productos_brutos.append(l_clean)
            
            prod_finales = []
            temp_qty = ""
            for p in productos_brutos:
                if re.match(r'^\d+$', p): 
                    temp_qty = p # Guarda el número de cantidad
                else:
                    if temp_qty:
                        prod_finales.append(f"{temp_qty} {p}") # Une cantidad + nombre
                        temp_qty = ""
                    else:
                        prod_finales.append(p)
            detalle_productos = " | ".join(prod_finales)

        # ==========================================
        # Descuentos y Envíos
        # ==========================================
        desc_match = re.search(r'Descuento[^\n]*\s+-?\$?([\d\.\,]+)', texto_panel, re.IGNORECASE)
        descuento = desc_match.group(1) if desc_match else "0"
        
        envio_match = re.search(r'Envío[^\n]*\s+\$?([\d\.\,]+)', texto_panel, re.IGNORECASE)
        costo_envio = envio_match.group(1) if envio_match else "0"
        
        # Procesamiento de Fechas
        try:
            dt_obj = datetime.strptime(hora_inicio_str, "%d/%m/%y %H:%M:%S")
            fecha_texto = dt_obj.strftime("%d/%m/%Y")
            hora_exacta = dt_obj.strftime("%H:%M")
            turno = "Mañana" if dt_obj.hour < 16 else "Noche"
        except:
            fecha_texto = ""
            hora_exacta = hora_inicio_str
            turno = ""

        # Agregar el registro
        datos_ventas.append({
            'Id': id_venta,
            'Fecha_Texto': fecha_texto,
            'Hora_Exacta': hora_exacta,
            'Turno': turno,
            'Cliente': cliente_str,
            'Total': limpiar_numero(total_str),
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

    print(f"✅ Se extrajeron {len(datos_ventas)} ventas correctamente.")
    
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
        print("⚠️ No se generó el Excel porque no había ventas cerradas.")

except Exception as e:
    print(f"❌ ERROR EN EXTRACCIÓN:")
    print(f"Tipo de error: {type(e).__name__}")
    print(f"Detalle: {e}")
    
finally:
    driver.quit()
    print("--- PROCESO TERMINADO ---")
