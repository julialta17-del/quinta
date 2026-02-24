def limpiar_dinero_pro(serie):
    """
    Convierte montos a números reales sin romper la magnitud.
    Maneja correctamente: 1.250,50 | 1250,50 | 1.250 | 1250.50
    """
    # Convertimos a string y quitamos el símbolo $
    serie = serie.astype(str).str.replace('$', '', regex=False).str.strip()
    
    def procesar_valor(val):
        if not val or val.lower() in ['nan', 'none', '', '0']: 
            return 0.0
        
        # 1. Si tiene ambos (. y ,) el último es el decimal
        if '.' in val and ',' in val:
            if val.find('.') < val.find(','): # Estilo 1.250,50
                return float(val.replace('.', '').replace(',', '.'))
            else: # Estilo 1,250.50
                return float(val.replace(',', '').replace('.', '.'))
        
        # 2. Si solo tiene COMA
        if ',' in val:
            partes = val.split(',')
            # Si después de la coma hay 2 dígitos, es decimal (1250,50)
            if len(partes[-1]) <= 2:
                return float(val.replace(',', '.'))
            # Si hay 3 dígitos, era un separador de miles (1,250)
            else:
                return float(val.replace(',', ''))

        # 3. Si solo tiene PUNTO
        if '.' in val:
            partes = val.split('.')
            # Si después del punto hay 2 dígitos, es decimal (1250.50)
            if len(partes[-1]) <= 2:
                return float(val)
            # Si hay 3 dígitos, es separador de miles (1.250)
            else:
                return float(val.replace('.', ''))
        
        # 4. Es un número limpio
        try:
            return float(val)
        except:
            return 0.0

    return serie.apply(procesar_valor)
