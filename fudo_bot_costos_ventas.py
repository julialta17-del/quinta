import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np


def parsear_numero_ar(s):
    """
    Convierte un string con formato argentino a entero redondeado.
    '4.712,67' → 4713  |  '4712,67' → 4713  |  '4712' → 4712
    """
    if isinstance(s, (int, float)):
        return round(float(s))
    s = str(s).strip()
    if not s or s.lower() in ('nan', ''):
        return 0
    # Tiene punto Y coma → punto=miles, coma=decimal
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    # Solo coma → es decimal
    elif ',' in s:
        s = s.replace(',', '.')
    # Solo punto con exactamente 3 dígitos después → separador de miles ("4.712")
    elif '.' in s:
        partes = s.split('.')
        if len(partes) == 2 and len(partes[1]) == 3:
            s = s.replace('.', '')
    try:
        return round(float(s))
    except ValueError:
        return 0


def calcular_margen_detallado_big_salads():
    print("1. Conectando a Google Sheets...")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)

    client = gspread.authorize(creds)
    spreadsheet = client.open("Quinta Analisis Fudo")

    sheet_ventas = spreadsheet.worksheet("Hoja 1")
    sheet_costos = spreadsheet.worksheet("Maestro_Costos")

    # -------------------------------------------------------
    # 2. PROCESAR COSTOS
    # -------------------------------------------------------
    print("2. Leyendo costos...")

    # ✅ CLAVE: get_all_values() trae los strings TAL COMO los muestra Sheets
    # get_all_records() convierte "4.712,67" → 471267 (entero), perdiendo decimales
    raw = sheet_costos.get_all_values()
    encabezados = raw[0]
    idx_nombre = encabezados.index('Nombre')
    idx_costo  = encabezados.index('Costo')

    dict_costos = {}
    for fila in raw[1:]:
        if len(fila) <= max(idx_nombre, idx_costo):
            continue
        nombre = fila[idx_nombre].strip()
        costo  = parsear_numero_ar(fila[idx_costo])
        if nombre:
            dict_costos[nombre.lower()] = costo

    print(f"   Productos en Maestro_Costos: {len(dict_costos)}")
    print("   Muestra de claves del diccionario:")
    for k, v in list(dict_costos.items())[:5]:
        print(f"     '{k}' → {v}")

    # -------------------------------------------------------
    # 3. LEER VENTAS
    # -------------------------------------------------------
    print("3. Leyendo ventas...")
    df_ventas = pd.DataFrame(sheet_ventas.get_all_records())

    if df_ventas.empty:
        print("⚠️ No hay ventas en Hoja 1. Fin del proceso.")
        return

    # -------------------------------------------------------
    # 4. DEBUG
    # -------------------------------------------------------
    print("\n--- DEBUG Detalle_Productos (primeras 5 filas) ---")
    no_encontrados = set()

    for i, fila in df_ventas.head(5).iterrows():
        celda = str(fila.get('Detalle_Productos', ''))
        print(f"  Fila {i}: repr={repr(celda)}")
        for item in celda.split(','):
            key = item.strip().lower()
            costo_debug = dict_costos.get(key, 'NO ENCONTRADO')
            print(f"    → '{item.strip()}' → costo={costo_debug}")

    print("--- FIN DEBUG ---\n")

    # -------------------------------------------------------
    # 5. COSTO DE INSUMOS POR PEDIDO
    # -------------------------------------------------------
    def calcular_costo_acumulado(fila):
        celda_productos = fila.get('Detalle_Productos', '')
        venta = parsear_numero_ar(fila.get('Total', 0))

        if not celda_productos or str(celda_productos).strip().lower() in ('', 'nan'):
            return round(venta * 0.35)

        lista_items = [item.strip() for item in str(celda_productos).split(',')]

        costo = 0
        alguno_no_encontrado = False

        for producto in lista_items:
            key = producto.strip().lower()
            if key not in dict_costos:
                no_encontrados.add(producto.strip())
                alguno_no_encontrado = True
            else:
                costo += dict_costos[key]

        # Fallback SOLO si ningún producto fue encontrado en el maestro
        if alguno_no_encontrado and costo == 0:
            return round(venta * 0.35)

        return round(costo)

    df_ventas['Costo_Total_Venta'] = df_ventas.apply(calcular_costo_acumulado, axis=1)

    if no_encontrados:
        print(f"⚠️  Productos en ventas SIN costo en Maestro_Costos ({len(no_encontrados)}):")
        for p in sorted(no_encontrados):
            print(f"     '{p}'")
        print("   → Para esos pedidos se usó el 35% de la venta como estimado.")
    else:
        print("✅ Todos los productos encontrados en Maestro_Costos.")

    # -------------------------------------------------------
    # 6. COSTO DE MANO DE OBRA POR PEDIDO
    # -------------------------------------------------------
    print("4. Calculando costo de mano de obra por pedido...")

    COSTO_TURNO = 3600 * 2 * 4  # $28.800 por turno completo (2 empleados)

    df_ventas['_fecha_turno'] = (
        df_ventas['Fecha_Texto'].astype(str).str.strip() + " | " +
        df_ventas['Turno'].astype(str).str.strip()
    )

    pedidos_por_turno = (
        df_ventas.groupby('_fecha_turno')['_fecha_turno']
        .transform('count')
    )

    df_ventas['Costo_MO_$']       = (COSTO_TURNO / pedidos_por_turno).round(0).astype(int)
    df_ventas['Pedidos_en_Turno'] = pedidos_por_turno.astype(int)

    df_ventas.drop(columns=['_fecha_turno'], inplace=True)

    # -------------------------------------------------------
    # 7. COMISIONES Y MARGEN NETO
    # -------------------------------------------------------
    def procesar_finanzas(fila):
        venta         = parsear_numero_ar(fila.get('Total', 0))
        envio         = parsear_numero_ar(fila.get('Costo_Envio', 0))
        descuento     = parsear_numero_ar(fila.get('Descuento_Total', 0))
        costo_insumos = parsear_numero_ar(fila.get('Costo_Total_Venta', 0))
        costo_mo      = parsear_numero_ar(fila.get('Costo_MO_$', 0))

        origen_raw       = str(fila.get('Origen', '')).lower().strip()
        origen_sin_tilde = origen_raw.replace('é', 'e').replace('ú', 'u')

        comision_peya   = 0
        comision_online = 0

        if "pedidos ya" in origen_sin_tilde:
            comision_peya = round(venta * 0.30)
        elif "menu online" in origen_sin_tilde:
            comision_online = round((venta + envio + descuento) * 0.023)

        margen = round(venta - costo_insumos - comision_peya - comision_online - costo_mo)

        return pd.Series([comision_peya, comision_online, margen])

    print("5. Calculando márgenes y comisiones...")
    df_ventas[['Comision_PeYa_$', 'Comision_Tienda_Online_$', 'Margen_Neto_$']] = \
        df_ventas.apply(procesar_finanzas, axis=1)

    total_numerico = df_ventas['Total'].apply(parsear_numero_ar)

    df_ventas['Margen_Neto_%'] = np.where(
        total_numerico > 0,
        (df_ventas['Margen_Neto_$'] / total_numerico * 100).round(1),
        0
    )

    # -------------------------------------------------------
    # 8. REORDENAMIENTO FINAL
    # -------------------------------------------------------
    columnas_al_final = [
        'Costo_Total_Venta',
        'Comision_PeYa_$',
        'Comision_Tienda_Online_$',
        'Margen_Neto_$',
        'Margen_Neto_%',
        'Costo_MO_$',
        'Pedidos_en_Turno',
    ]
    columnas_principales = [c for c in df_ventas.columns if c not in columnas_al_final]
    df_final = df_ventas[columnas_principales + columnas_al_final].copy()

    # -------------------------------------------------------
    # 9. LIMPIEZA Y FORMATEO FINAL
    # -------------------------------------------------------
    df_final = df_final.replace([np.nan, np.inf, -np.inf], 0)

    cols_enteras = [
        'Costo_Total_Venta',
        'Costo_MO_$',
        'Pedidos_en_Turno',
        'Comision_PeYa_$',
        'Comision_Tienda_Online_$',
        'Margen_Neto_$'
    ]
    for col in cols_enteras:
        if col in df_final.columns:
            df_final[col] = (
                pd.to_numeric(df_final[col], errors='coerce')
                .fillna(0)
                .round(0)
                .astype(int)
                .astype(str)
            )

    if 'Margen_Neto_%' in df_final.columns:
        def formatear_pct(x):
            val = pd.to_numeric(x, errors='coerce')
            if pd.isna(val):
                return '0'
            val = round(val, 1)
            return str(int(val)) if val == int(val) else str(val)
        df_final['Margen_Neto_%'] = df_final['Margen_Neto_%'].apply(formatear_pct)

    # -------------------------------------------------------
    # 10. SUBIR A GOOGLE SHEETS
    # -------------------------------------------------------
    print("6. Actualizando Hoja 1 con costos y márgenes...")
    datos_subir = [df_final.columns.tolist()] + df_final.astype(str).values.tolist()

    sheet_ventas.clear()
    sheet_ventas.update(values=datos_subir, range_name='A1')

    print("✅ ¡Proceso completado!")
    print(f"   Columnas agregadas: {', '.join(columnas_al_final)}")


if __name__ == "__main__":
    calcular_margen_detallado_big_salads()
