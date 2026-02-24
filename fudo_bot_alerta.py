import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar_alerta(titulo, html):
    msg = MIMEMultipart()
    msg["From"] = "julialta17@gmail.com"
    msg["To"] = "julialta17@gmail.com"
    msg["Subject"] = f"🚨 ALERTA: {titulo}"
    msg.attach(MIMEText(html, "html"))
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587); server.starttls()
        server.login("julialta17@gmail.com", "flns hgiy nwyw rzda")
        server.sendmail("julialta17@gmail.com", ["julialta17@gmail.com"], msg.as_string()); server.quit()
    except Exception as e: print(f"Error mail: {e}")

def ejecutar_alertas():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope) if creds_json else Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    df = pd.DataFrame(client.open("Quinta Analisis Fudo").worksheet("Hoja 1").get_all_records())
    df_reales = df[pd.to_numeric(df['Total'], errors='coerce') > 0].copy()

    alertas = ""
    negativos = df_reales[pd.to_numeric(df_reales['Margen_Neto_$'], errors='coerce') < 0]
    if not negativos.empty:
        alertas += "<h3>❌ Margen Negativo Detectado</h3>" + negativos.to_html()

    if alertas: enviar_alerta("Anomalías detectadas", alertas)

if __name__ == "__main__":
    ejecutar_alertas()

