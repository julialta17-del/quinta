import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# --- CONFIGURACIÓN ---
MAIL_REMITENTE = "julialta17@gmail.com"
MAIL_DESTINATARIOS = ["julialta17@gmail.com", "matiasgabrielrebolledo@gmail.com"]
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
URL_DASHBOARD = "https://docs.google.com/spreadsheets/d/1uEFRm_0zEhsRGUX9PIomjUhiijxWVnCXnSMQuUJK5a8/edit"

def limpiar_dinero_blindado(serie):
    """
    Normaliza montos detectando inteligentemente separadores de miles y decimales.
    Evita que 1.500 se convierta en 1.5 o que los decimales se sumen como enteros.
    """
    def procesar(val):
        val = str(val).replace('$', '').replace(' ', '').strip()
        if not val or val.lower() in ['nan', 'none', '0', '0.0', '']:
            return 0.0
        
        # Caso 1: Formato estándar Arg con miles (1.250,50)
        if '.' in val and ',' in val:
            val = val.replace('.', '').replace(',', '.')
        
        # Caso 2: Solo coma (1250,50)
        elif ',' in val:
            val = val.replace(',', '.')
            
        # Caso 3: Solo punto (Puede ser 1.250 o 1250.50)
        elif '.' in val:
            partes = val.split('.')
            # Si tiene más de 2 dígitos tras el punto, es separador de miles (ej: 1.500)
            if len(partes[-1]) != 2: 
                val = val.replace('.', '')
            # Si tiene 2, lo tratamos como decimal (ej: 1250.50)
        
        try:
            return float(val)
        except:
            return 0.0
            
    return serie.apply(procesar)

def enviar_reporte_pro(datos):
    mensaje = MIMEMultipart()
    mensaje["From"] = f"Big Salads Quinta<{MAIL_REMITENTE}>"
    mensaje["To"] = ", ".join(MAIL_DESTINATARIOS)
    mensaje["Subject"] = f"🥗 Resumen Ejecutivo Quinta Big Salads: {datos['fecha']}"

    # Formateo visual para el mail (Estilo $ 1.250,50)
    def fmt(n):
        return "{:,.2f}".format(n).replace(',', 'X').replace('.', ',').replace('X', '.')

    venta_f = fmt(datos['total_v'])
    margen_f = fmt(datos['margen_real'])
    tkt_f = fmt(datos['ticket'])

    cuerpo = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 25px; border-radius: 10px;">
            <h2 style="color: #2c3e50; text-align: center; border-bottom: 2px solid #27ae60; padding-bottom: 10px;">🥗 Big Salads Sexta</h2>
            
            <div style="background-color: #f9f9f9; padding: 15px; border-radius: 10px; margin-bottom: 20px;">
                <p style="font-size: 18px; margin: 5px 0;">💰 <strong>Ventas Totales:</strong> ${venta_f}</p>
                <p style="font-size: 18px; margin: 5px 0; color: #27ae60;">💵 <strong>Margen Neto Real:</strong> ${margen_f}</p>
                <p style="font-size: 16px; margin: 5px 0;">🎫 <strong>Ticket Promedio:</strong> ${tkt_f}</p>
                <hr style="border: 0; border-top: 1px solid #ddd; margin: 15px 0;">
                <p style="margin: 5px 0;">🌐 <strong>Mix de Origen:</strong> {datos['origen_str']}</p>
            </div>

            <h3 style="color: #2c3e50;">🕒 Pedidos por Turno:</h3>
            <div style="background: #f4f4f4; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
                <table width="100%" style="text-align: center;">
                    <tr>{datos['turnos_str']}</tr>
                </table>
            </div>

            <h3 style="color: #2c3e50;">💳 Medios de Pago:</h3>
            <ul style="list-style: none; padding-left: 0;">
                {datos['pagos_str']}
            </ul>

            <h3 style="color: #2c3e50; margin-top: 25px;">🔥 Top Productos Estrella</h3>
            <ul style="padding-left: 20px;">
                {datos['top_html']}
            </ul>
            
            <div style="text-align: center; margin-top: 35px;">
                <a href="{URL_DASHBOARD}" style="background-color: #27ae60; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; font-weight: bold;">📊 ABRIR DASHBOARD</a>
            </div>
        </div>
      </body>
    </html>
    """
    mensaje.attach(MIMEText(cuerpo, "html"))
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(MAIL_REMITENTE, MAIL_PASSWORD)
    server.sendmail(MAIL_REMITENTE, MAIL_DESTINATARIOS, mensaje.as_string())
    server.quit()

def ejecutar():
    print("🚀 Iniciando reporte ejecutivo...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    client = gspread.authorize(creds)
    
    sheet = client.open("Quinta Analisis Fudo").worksheet("Hoja 1")
    
    # Leemos datos crudos para evitar que la API formatee mal los números
    data = sheet.get_all_values()
    if len(data) < 2:
        print("⚠️ La hoja está vacía.")
        return

    headers = [h.strip() for h in data[0]]
    df = pd.DataFrame(data[1:], columns=headers)

    # Limpieza de montos con la nueva lógica blindada
    df['Total_Num'] = limpiar_dinero_blindado(df['Total'])
    
    # Buscar columna de margen dinámicamente
    col_margen = next((c for c in df.columns if 'Margen' in c), None)
    if col_margen:
        df['Margen_Num'] = limpiar_dinero_blindado(df[col_margen])
    else:
        df['Margen_Num'] = 0.0
        print("⚠️ No se encontró columna de Margen.")

    # Filtrar solo ventas reales del día
    df_v = df[df['Total_Num'] > 0].copy()
    
    if df_v.empty:
        print("⚠️ No hay ventas para procesar en el reporte.")
        return

    # Cálculos finales
    total_v = df_v['Total_Num'].sum()
    margen_t = df_v['Margen_Num'].sum()
    ticket = total_v / len(df_v)

    # Estadísticas
    turnos_str = "".join([f"<td><strong>{k}</strong><br>{v} pedidos</td>" for k, v in df_v['Turno'].value_counts().items()])
    
    origen_stats = df_v.groupby('Origen')['Total_Num'].sum()
    origen_str = ", ".join([f"{(v/total_v*100):.1f}% {k}" for k, v in origen_stats.items()])

    # Formateo de medios de pago para el mail
    pagos_resumen = df_v.groupby('Medio de Pago')['Total_Num'].sum().sort_values(ascending=False)
    pagos_str = ""
    for k, v in pagos_resumen.items():
        v_f = "{:,.2f}".format(v).replace(',', 'X').replace('.', ',').replace('X', '.')
        pagos_str += f"<li style='margin-bottom: 5px;'>🔹 <strong>{k}:</strong> ${v_f}</li>"
    
    # Top Productos
    df_v['Principal'] = df_v['Detalle_Productos'].astype(str).str.split(',').str[0].str.strip()
    top_html = "".join([f"<li>{k}: <b>{v} vendidos</b></li>" for k, v in df_v['Principal'].value_counts().head(5).items()])

    datos = {
        'total_v': total_v, 'margen_real': margen_t, 'ticket': ticket,
        'fecha': datetime.now().strftime('%d/%m/%Y'),
        'turnos_str': turnos_str, 'origen_str': origen_str,
        'pagos_str': pagos_str, 'top_html': top_html
    }

    enviar_reporte_pro(datos)
    print(f"✅ Reporte enviado con éxito. Total: ${total_v:.2f} | Margen: ${margen_t:.2f}")

if __name__ == "__main__":
    ejecutar()
