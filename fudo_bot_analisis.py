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
    if not val: return "0"
    # Quita puntos de miles, cambia coma por punto para decimales, redondea y devuelve entero en texto
    v = str(val).replace('$', '').strip().replace('.', '').replace(',', '.')
    try:
        return str(int(round(float(v))))
    except:
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
    time.sleep(15) # Tiempo para que cargue la info del día
    
    filas_xpath = "//table/tbody/tr"
    filas = driver.find_elements(By.XPATH, filas_xpath)
    total_filas = len(filas)
    print(f"Se encontraron {total_filas} registros visibles. Analizando uno a uno...")

    datos_ventas = []

    for i in range(total_filas):
        # Refrescamos la lista de elementos en cada ciclo para que no se "rompan" al hacer clic
        tr_list = driver.find_elements(By.XPATH, filas_xpath)
        if i >= len(tr_list):
            break
        
        fila = tr_list[i]
        celdas = fila.find_elements(By.TAG_NAME, "td")
        
        if len(celdas) < 6:
            continue
            
        estado = celdas[2].text.strip()
        # Omitimos las ventas que dicen "En curso" porque aún no están cerradas
        if estado.lower() == "en curso":
            continue
            
        hora_inicio_str = celdas[0].text.strip()
        cliente_str = celdas[5].text.strip()
        
        # Clic en la fila para abrir el panel derecho
        driver.execute_script("arguments[0].click();", fila)
        time.sleep(1.5) # Espera rápida para que el panel derecho se dibuje
        
        # 1. Obtener el ID de la URL
        url_actual = driver.current_url
        id_venta = url_actual.split('/')[-1] if '/' in url_actual else ""
        
        # 2. Leer todo el texto del panel derecho
        try:
            panel = driver.find_element(By.CSS_SELECTOR, "aside.page__content__aside")
            texto_panel = panel.text
        except:
            texto_panel = ""
            
        # 3. Procesar datos del panel con Regex (busca los patrones de texto)
        origen_match = re.search(r'Origen\s+(.+)', texto_panel)
        origen = origen_match.group(1).strip() if origen_match else ""
        
        pago_match = re.search(r'PAGOS\n(.+?)\s*\$', texto_panel)
        medio_pago = pago_match.group(1).strip() if pago_match else ""
        
        # Extraer productos de la sección ADICIONES
        try:
            adic_part = texto_panel.split('ADICIONES')[1]
            prod_part = adic_part.split('Subtotal')[0]
            # Limpiamos las líneas vacías
            productos = [p.strip() for p in prod_part.split('\n') if p.strip()]
            detalle_productos = " | ".join(productos)
        except:
            detalle_productos = ""
            
        desc_match = re.search(r'Descuento[^\n]*\s+-?\$?([\d\.\,]+)', texto_panel)
        descuento = desc_match.group(1) if desc_match else "0"
        
        envio_match = re.search(r'Envío[^\n]*\s+\$?([\d\.\,]+)', texto_panel, re.IGNORECASE)
        costo_envio = envio_match.group(1) if envio_match else "0"
        
        total_match = re.search(r'Total:\s+\$?([\d\.\,]+)', texto_panel)
        total = total_match.group(1) if total_match else "0"
        
        # 4. Acomodar fechas y turnos
        try:
            # Fudo muestra "25/11/26 23:05:41"
            dt_obj = datetime.strptime(hora_inicio_str, "%d/%m/%y %H:%M:%S")
            fecha_texto = dt_obj.strftime("%d/%m/%Y")
            hora_exacta = dt_obj.strftime("%H:%M")
            turno = "Mañana" if dt_obj.hour < 16 else "Noche"
        except:
            fecha_texto = ""
            hora_exacta = hora_inicio_str
            turno = ""

        # Agregar el registro al listado final
        datos_ventas.append({
            'Id': id_venta,
            'Fecha_Texto': fecha_texto,
            'Hora_Exacta': hora_exacta,
            'Turno': turno,
            'Cliente': cliente_str,
            'Total': limpiar_numero(total),
            'Origen': origen,
            'Medio de Pago': medio_pago,
            'Detalle_Productos': detalle_productos,
            'Descuento_Total': limpiar_numero(descuento),
            'Costo_Envio': limpiar_numero(costo_envio),
            'Costo_Total_Venta': 0,
            'Comision_PeYa_$': 0,
            'Comision_Tienda_Online_$': 0,
            'Margen_Neto_$': 0,
            'Margen_Neto_%': 0,
            'Costo_MO_$': 0,
            'Pedidos_en_Turno': 0
        })

    print(f"✅ Se extrajeron {len(datos_ventas)} ventas cerradas correctamente.")
    
    # Armar la tabla final en Pandas respetando exactamente el orden pedido
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
