import pandas as pd


# ============================================================
# UTILIDAD: Normalizar y limpiar encabezados
# ============================================================
def limpiar_encabezados(df):
    df.columns = (
        df.columns
        .str.replace("ï»¿", "", regex=False)     # BOM
        .str.replace("\ufeff", "", regex=False)  # UTF BOM
        .str.replace("\xa0", " ", regex=False)   # NBSP
        .str.replace('"', "", regex=False)       # comillas
        .str.strip()
    )
    return df


# ============================================================
# UTILIDAD: Convertir fechas
# ============================================================
def parse_date_series(s, dayfirst=False):
    return pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)


# ============================================================
# PROCESAMIENTO DE VENTAS
# ============================================================
def process_ventas(df):
    df = df.copy()
    df = limpiar_encabezados(df)

    # Fecha final oficial
    df["fecha"] = parse_date_series(df["tm_start_local_at"], dayfirst=False)

    # Monto
    try:
        df["qt_price_local"] = pd.to_numeric(
            df["qt_price_local"].astype(str)
            .str.replace(",", "")
            .str.replace(".", ""),
            errors="coerce"
        )
    except:
        df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")

    df["Venta_Total"] = df["qt_price_local"]
    df["Venta_Compartida"] = df.apply(
        lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_compartida" else 0,
        axis=1
    )
    df["Venta_Exclusiva"] = df.apply(
        lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_exclusive" else 0,
        axis=1
    )

    df_group = df.groupby("fecha", as_index=False).agg({
        "Venta_Total": "sum",
        "Venta_Compartida": "sum",
        "Venta_Exclusiva": "sum"
    })

    return df_group


# ============================================================
# PROCESAMIENTO DE PERFORMANCE (con autodetección FIRT/FURT)
# ============================================================
def process_performance(df):
    df = df.copy()
    df = limpiar_encabezados(df)

    print("ENCABEZADOS PERFORMANCE:", list(df.columns))

    # Detectar columna fecha
    posibles_fechas = ["Fecha de Referencia", "fecha", "Date", "Reference Date"]
    col_fecha = next((c for c in posibles_fechas if c in df.columns), None)

    if col_fecha is None:
        raise ValueError(f"No se encontró columna fecha en Performance. Columnas: {list(df.columns)}")

    df["fecha"] = parse_date_series(df[col_fecha], dayfirst=False)

    # Filtrar solo C_Ops Support
    df = df[df["Group Support Service"] == "C_Ops Support"]

    # ----------------------------------------------
    # DETECCIÓN AUTOMÁTICA DE % FIRT
    # ----------------------------------------------
    posibles_firt = ["% Firt", "%Firt", "Firt %", "Firt%", "FIRT", "% FIRT"]
    col_firt = next((c for c in posibles_firt if c in df.columns), None)

    if col_firt is None:
        raise ValueError(f"No existe columna equivalente a % Firt. Columnas: {list(df.columns)}")

    df[col_firt] = pd.to_numeric(df[col_firt], errors="coerce")
    df = df.rename(columns={col_firt: "% Firt"})

    # ----------------------------------------------
    # DETECCIÓN AUTOMÁTICA DE % FURT
    # ----------------------------------------------
    posibles_furt = ["% Furt", "%Furt", "Furt %", "Furt%", "FURT", "% FURT"]
    col_furt = next((c for c in posibles_furt if c in df.columns), None)

    if col_furt is None:
        raise ValueError(f"No existe columna equivalente a % Furt. Columnas: {list(df.columns)}")

    df[col_furt] = pd.to_numeric(df[col_furt], errors="coerce")
    df = df.rename(columns={col_furt: "% Furt"})

    # Encuestas
    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score")) else 0,
        axis=1
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(lambda x: 1 if str(x).lower() == "solved" else 0)

    perf_group = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas": "sum",
        "% Firt": "mean",
        "% Furt": "mean",
        "Firt (h)": "mean",
        "Furt (h)": "mean",
        "NPS Score": "mean",
        "CSAT": "mean",
        "Reopen": "sum",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum"
    })

    return perf_group


# ============================================================
# PROCESAMIENTO DE AUDITORÍAS (con autodetección de fecha)
# ============================================================
def process_auditorias(df):
    df = df.copy()
    df = limpiar_encabezados(df)

    print("ENCABEZADOS AUDITORÍAS:", list(df.columns))

    posibles_fechas = [
        "Date Time",
        "DateTime",
        "Date_Time",
        "Date time",
        "Date",
        "Fecha",
        "Audited At UTC Dt",
        "Date Time Reference",
        "Submission Audit Dttm UTC"
    ]

    col_fecha = next((c for c in posibles_fechas if c in df.columns), None)

    if col_fecha is None:
        raise ValueError(
            f"No se encontró columna de fecha en Auditorías. Columnas: {list(df.columns)}"
        )

    df["fecha"] = parse_date_series(df[col_fecha], dayfirst=True)

    df = df[~df["fecha"].isna()]

    df["Nota_Auditoria"] = pd.to_numeric(df.get("Total Audit Score", None), errors="coerce")
    df["Q_Auditorias"] = pd.to_numeric(df.get("# Audits by Agent", 1), errors="coerce").fillna(1)

    out = df.groupby("fecha", as_index=False).agg({
        "Nota_Auditoria": "mean",
        "Q_Auditorias": "sum"
    })

    return out


# ============================================================
# PROCESAMIENTO OFF TIME
# ============================================================
def process_offtime(df):
    df = df.copy()
    df = limpiar_encabezados(df)

    df["fecha"] = parse_date_series(df["tm_start_local_at"], dayfirst=False)

    df["OFFTIME"] = df.apply(
        lambda x: 1 if x["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)" else 0,
        axis=1
    )

    out = df.groupby("fecha", as_index=False).agg({"OFFTIME": "sum"})
    return out


# ============================================================
# PROCESAMIENTO VIAJES > 90 MINUTOS
# ============================================================
def process_mayor90(df):
    df = df.copy()
    df = limpiar_encabezados(df)

    df["fecha"] = parse_date_series(df["Start At Local Dt"], dayfirst=False)
    df["LARGOS"] = df["Duration (Minutes)"].apply(lambda x: 1 if x > 90 else 0)

    out = df.groupby("fecha", as_index=False).agg({"LARGOS": "sum"})
    return out


# ============================================================
# CONSOLIDADO GLOBAL FINAL
# ============================================================
def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to):
    ventas = process_ventas(df_ventas)
    perf = process_performance(df_perf)
    aud = process_auditorias(df_aud)
    off = process_offtime(df_off)
    dur = process_mayor90(df_dur)

    # Unión por fecha
    df = ventas.merge(perf, on="fecha", how="outer")
    df = df.merge(aud, on="fecha", how="outer")
    df = df.merge(off, on="fecha", how="outer")
    df = df.merge(dur, on="fecha", how="outer")

    # Filtro fechas
    fmin = pd.to_datetime(date_from)
    fmax = pd.to_datetime(date_to)
    df = df[(df["fecha"] >= fmin) & (df["fecha"] <= fmax)]

    # Q con 0
    cantidades = ["Q_Encuestas", "Q_Tickets", "Q_Tickets_Resueltos", "Reopen", "Q_Auditorias", "OFFTIME", "LARGOS"]
    for c in cantidades:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    return df.sort_values("fecha")



