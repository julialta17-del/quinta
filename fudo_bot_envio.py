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
    Normaliza montos para Argentina sin errores de magnitud.
    Transforma formatos como 12.376,95 o 12376,95 en decimales de Python.
    """
    def procesar(val):
        val = str(val).replace('$', '').strip()
        if not val or val.lower() in ['nan', 'none', '0', '']:
            return 0.0
        if '.' in val and ',' in val:
            val = val.replace('.', '').replace(',', '.')
        elif ',' in val:
            val = val.replace(',', '.')
        elif '.' in val:
            partes = val.split('.')
            if len(partes[-1]) != 2: 
                val = val.replace('.', '')
        try:
            return float(val)
        except:
            return 0.0
    return serie.apply(procesar)

def enviar_reporte_pro(datos):
    mensaje = MIMEMultipart()
    mensaje["From"] = MAIL_REMITENTE
    mensaje["To"] = ", ".join(MAIL_DESTINATARIOS)
    mensaje["Subject"] = f"🥗 Resumen Ejecutivo Big Salads QUINTA: {datos['fecha']}"

    # Formateo de moneda para el mail (Estilo Arg)
    venta_f = "{:,.2f}".format(datos['total_v']).replace(',', 'X').replace('.', ',').replace('X', '.')
    margen_f = "{:,.2f}".format(datos['margen_real']).replace(',', 'X').replace('.', ',').replace('X', '.')
    tkt_f = "{:,.2f}".format(datos['ticket']).replace(',', 'X').replace('.', ',').replace('X', '.')

    cuerpo = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 25px; border-radius: 10px;">
            <h2 style="color: #2c3e50; text-align: center; border-bottom: 2px solid #27ae60; padding-bottom: 10px;">🥗 Big Salads Quinta</h2>
            
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
    print("Conectando a Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    client = gspread.authorize(creds)
    
    sheet = client.open("Quinta Analisis Fudo").worksheet("Hoja 1")
    # Forzamos lectura como texto para evitar errores de magnitud automáticos
    df = pd.DataFrame(sheet.get_all_records(numericise_ignore=['all']))
    df.columns = df.columns.str.strip()

    # Limpieza de montos
    df['Total_Num'] = limpiar_dinero_blindado(df['Total'])
    col_margen = 'Margen_Neto_$' if 'Margen_Neto_$' in df.columns else 'Margen_Neto'
    df['Margen_Num'] = limpiar_dinero_blindado(df[col_margen])

    # Filtro de ventas reales
    df_v = df[df['Total_Num'] > 0].copy()
    
    total_v = df_v['Total_Num'].sum()
    margen_t = df_v['Margen_Num'].sum()
    ticket = total_v / len(df_v) if len(df_v) > 0 else 0

    # Estadísticas de Turnos y Pagos
    turnos_str = "".join([f"<td><strong>{k}</strong><br>{v} pedidos</td>" for k, v in df_v['Turno'].value_counts().items()])
    
    origen_stats = df_v.groupby('Origen')['Total_Num'].sum()
    origen_str = ", ".join([f"{(v/total_v*100):.1f}% {k}" for k, v in origen_stats.items()]) if total_v > 0 else "N/D"

    pagos_resumen = df_v.groupby('Medio de Pago')['Total_Num'].sum().sort_values(ascending=False)
    pagos_str = "".join([f"<li style='margin-bottom: 5px;'>🔹 <strong>{i}:</strong> ${v:,.2f}</li>" for i, v in pagos_resumen.items()]).replace(',', 'X').replace('.', ',').replace('X', '.')
    
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
    print(f"✅ Reporte completo enviado. Venta: {total_v} | Margen: {margen_t}")

if __name__ == "__main__":
    ejecutar()


