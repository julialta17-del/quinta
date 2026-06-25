import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def ejecutar_analisis_fidelizacion():
    print("1. Conectando a Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open("Quinta Analisis Fudo")
    
    # LEER HISTORICO
    try:
        sheet_hist = spreadsheet.worksheet("Historico")
        df_h = pd.DataFrame(sheet_hist.get_all_records())
    except Exception as e:
        print(f"Error al acceder al Historico: {e}")
        return

    if df_h.empty:
        print("El Historico está vacío.")
        return

    df_h.columns = [str(c).strip() for c in df_h.columns]

    # --- TRATAMIENTO DE DATOS ---
    col_fecha = 'Fecha' if 'Fecha' in df_h.columns else 'Fecha_Texto'
    df_h['Fecha_DT'] = pd.to_datetime(df_h[col_fecha], dayfirst=True, errors='coerce')

    df_h['Id'] = pd.to_numeric(df_h['Id'], errors='coerce')

    # Limpiar formato numérico
    df_h['Total_Num'] = (
        df_h['Total']
        .astype(str)
        .str.strip()
        .str.replace(r'[$\s]', '', regex=True)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .pipe(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    print("2. Calculando promedios, ventanas de tiempo y Turno Habitual...")

    def obtener_moda(serie):
        if serie.empty: return "N/A"
        m = serie.mode()
        return m.iloc[0] if not m.empty else "N/A"

    # A) HÁBITOS
    habitos = df_h.groupby('Cliente').agg({
        'Turno': obtener_moda,
        'Origen': obtener_moda,
        'Medio de Pago': obtener_moda
    }).reset_index()
    habitos.columns = ['Cliente', 'Turno_Habitual', 'Canal_Habitual', 'Pago_Habitual']

    # B) MÉTRICAS HISTÓRICAS BÁSICAS
    metricas = df_h.groupby('Cliente').agg({
        'Id': 'count',
        'Total_Num': 'mean',
        'Fecha_DT': 'max',
        'Detalle_Productos': 'last'
    }).reset_index()
    metricas.columns = ['Cliente', 'Cant_Pedidos_Total', 'Ticket_Promedio', 'Ultima_Visita', 'Ultimo_Pedido']

    # --- NUEVO: CÁLCULOS DE VENTANAS DE TIEMPO (60 DÍAS) ---
    hoy = pd.Timestamp.now()
    df_h['Dias_Desde_Pedido'] = (hoy - df_h['Fecha_DT']).dt.days

    # 1. ¿Cuántos pedidos hicieron estrictamente en los ÚLTIMOS 60 días?
    pedidos_60d = df_h[df_h['Dias_Desde_Pedido'] <= 60].groupby('Cliente').size().reset_index(name='Cant_Pedidos_60D')

    # 2. ¿Alguna vez llegaron a hacer 6 pedidos en CUALQUIER ventana de 60 días?
    # Configuramos la fecha como índice para poder usar ventanas móviles (.rolling)
    temp_df = df_h.dropna(subset=['Fecha_DT']).sort_values('Fecha_DT').set_index('Fecha_DT')
    max_60d = (
        temp_df.groupby('Cliente')['Id']
        .rolling('60D').count()
        .groupby('Cliente').max()
        .reset_index(name='Max_Pedidos_60D')
    )

    # C) FECHA PRIMER PEDIDO
    primer_pedido = (
        df_h.sort_values('Id')
            .groupby('Cliente')
            .first()
            .reset_index()[['Cliente', 'Fecha_DT']]
    )
    primer_pedido.columns = ['Cliente', 'Fecha_Primer_Pedido']

    # Unimos todas las métricas
    resultado = pd.merge(metricas, habitos, on='Cliente', how='left')
    resultado = pd.merge(resultado, primer_pedido, on='Cliente', how='left')
    resultado = pd.merge(resultado, pedidos_60d, on='Cliente', how='left')
    resultado = pd.merge(resultado, max_60d, on='Cliente', how='left')

    # Rellenamos con 0 a los clientes que no tienen pedidos recientes
    resultado['Cant_Pedidos_60D'] = resultado['Cant_Pedidos_60D'].fillna(0)
    resultado['Max_Pedidos_60D'] = resultado['Max_Pedidos_60D'].fillna(0)

    # --- SEGMENTACIÓN ---
    resultado['Dias_Inactivo'] = (hoy - resultado['Ultima_Visita']).dt.days
    resultado['Ticket_Promedio'] = resultado['Ticket_Promedio'].round(2)

    # ✅ NUEVA LÓGICA DE SEGMENTACIÓN RESPETANDO LOS DÍAS
    def segmentar(fila):
        # ⭐ VIP: Tienen 6 o más pedidos DENTRO de los últimos 60 días
        if fila['Cant_Pedidos_60D'] >= 6:
            return "⭐ VIP"
        
        # ✅ Frecuente: Tienen 3 a 5 pedidos DENTRO de los últimos 60 días
        elif fila['Cant_Pedidos_60D'] >= 3:
            return "✅ Frecuente"
        
        # ⚠️ VIP en Riesgo: Históricamente hicieron 6 pedidos en 60 días, pero llevan MÁS de 60 días sin pedir
        elif fila['Max_Pedidos_60D'] >= 6 and fila['Dias_Inactivo'] > 60:
            if fila['Dias_Inactivo'] <= 120:  # Si llevan entre 61 y 120 días sin venir
                return "⚠️ VIP en Riesgo"
            else:
                return "💤 Dormido"  # Ya pasó demasiado tiempo (más de 120 días)

        # 💤 Dormido: Cualquier otro cliente que lleve más de 90 días inactivo
        elif fila['Dias_Inactivo'] > 90:
            return "💤 Dormido"
            
        # 🆕 Nuevo / Casual: Tienen menos de 3 pedidos recientes, pero vinieron hace poco
        else:
            if fila['Cant_Pedidos_Total'] == 1:
                return "🆕 Nuevo"
            else:
                return "🚶 Casual"

    resultado['Segmento'] = resultado.apply(segmentar, axis=1)
    
    # Formatear Fechas para que se vean bien en Google Sheets
    resultado['Ultima_Visita'] = resultado['Ultima_Visita'].dt.strftime('%d/%m/%Y')
    resultado['Fecha_Primer_Pedido'] = resultado['Fecha_Primer_Pedido'].dt.strftime('%d/%m/%Y')

    columnas_finales = [
        'Cliente', 'Segmento', 'Cant_Pedidos_Total', 'Cant_Pedidos_60D', 'Ticket_Promedio',
        'Turno_Habitual', 'Canal_Habitual', 'Pago_Habitual',
        'Ultimo_Pedido', 'Ultima_Visita', 'Dias_Inactivo',
        'Fecha_Primer_Pedido'
    ]

    # Ordenamos a los clientes dándole prioridad a los que más pedidos tienen en los últimos 60 días
    df_final = resultado[columnas_finales].sort_values(by=['Cant_Pedidos_60D', 'Cant_Pedidos_Total'], ascending=[False, False])

    # --- SUBIR RESULTADOS ---
    print("3. Actualizando hoja 'Analisis_Clientes'...")
    try:
        sheet_cli = spreadsheet.worksheet("Analisis_Clientes")
    except:
        sheet_cli = spreadsheet.add_worksheet(title="Analisis_Clientes", rows="5000", cols="15")

    sheet_cli.clear()
    df_final = df_final.fillna("N/A")
    datos_subir = [df_final.columns.tolist()] + df_final.astype(str).values.tolist()
    sheet_cli.update(values=datos_subir, range_name='A1')

    print(f"✅ ¡Hecho! Clientes analizados: {len(df_final)}")

if __name__ == "__main__":
    ejecutar_analisis_fidelizacion()
