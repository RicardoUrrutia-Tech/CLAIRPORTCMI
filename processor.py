import pandas as pd


# ============================================================
# UTILIDAD: Normalizar formatos de fecha
# ============================================================
def parse_date_series(s, dayfirst=False):
    return pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)


# ============================================================
# PROCESAMIENTO DE VENTAS
# ============================================================
def process_ventas(df):
    df = df.copy()

    # Convertir fecha
    df["fecha"] = parse_date_series(df["tm_start_local_at"], dayfirst=False)

    # Normalización monto
    try:
        df["qt_price_local"] = pd.to_numeric(
            df["qt_price_local"].astype(str).str.replace(",", "").str.replace(".", ""),
            errors="coerce"
        )
    except:
        df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")

    df["Venta_Total"] = df["qt_price_local"]
    df["Venta_Compartida"] = df.apply(lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_compartida" else 0, axis=1)
    df["Venta_Exclusiva"] = df.apply(lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_exclusive" else 0, axis=1)

    df_group = df.groupby("fecha", as_index=False).agg({
        "Venta_Total": "sum",
        "Venta_Compartida": "sum",
        "Venta_Exclusiva": "sum"
    })

    return df_group


# ============================================================
# PROCESAMIENTO DE PERFORMANCE
# ============================================================
def process_performance(df):
    df = df.copy()

    df["fecha"] = parse_date_series(df["Fecha de Referencia"], dayfirst=False)

    # Filtrar solo C_Ops Support
    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if not pd.isna(x["CSAT"]) or not pd.isna(x["NPS Score"]) else 0,
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
# PROCESAMIENTO AUDITORÍAS (MÓDULO COMPLETAMENTE CORREGIDO)
# ============================================================
def process_auditorias(df):
    df = df.copy()

    # Eliminar UTF-BOM oculto
    df.columns = df.columns.str.replace("ï»¿", "", regex=False).str.strip()

    # Posibles columnas de fecha
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

    col_fecha = None
    for c in posibles_fechas:
        if c in df.columns:
            col_fecha = c
            break

    if col_fecha is None:
        raise ValueError(f"No se encontró columna de fecha válida en Auditorías. Columnas: {list(df.columns)}")

    df["fecha"] = parse_date_series(df[col_fecha], dayfirst=True)

    df = df[~df["fecha"].isna()]

    if "Total Audit Score" in df.columns:
        df["Nota_Auditoria"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")
    else:
        df["Nota_Auditoria"] = None

    # Q auditorías
    if "# Audits by Agent" in df.columns:
        df["Q_Auditorias"] = pd.to_numeric(df["# Audits by Agent"], errors="coerce")
    else:
        df["Q_Auditorias"] = 1

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

    df["fecha"] = parse_date_series(df["tm_start_local_at"], dayfirst=False)

    df["OFFTIME"] = df.apply(
        lambda x: 1 if x["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)" else 0,
        axis=1
    )

    out = df.groupby("fecha", as_index=False).agg({
        "OFFTIME": "sum"
    })

    return out


# ============================================================
# PROCESAMIENTO VIAJES > 90 MINUTOS
# ============================================================
def process_mayor90(df):
    df = df.copy()

    df["fecha"] = parse_date_series(df["Start At Local Dt"], dayfirst=False)
    df["LARGOS"] = df["Duration (Minutes)"].apply(lambda x: 1 if x > 90 else 0)

    out = df.groupby("fecha", as_index=False).agg({
        "LARGOS": "sum"
    })

    return out


# ============================================================
# CONSOLIDADO GLOBAL
# ============================================================
def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to):
    ventas = process_ventas(df_ventas)
    perf = process_performance(df_perf)
    aud = process_auditorias(df_aud)
    off = process_offtime(df_off)
    dur = process_mayor90(df_dur)

    # Unir todo por fecha
    df = ventas.merge(perf, on="fecha", how="outer")
    df = df.merge(aud, on="fecha", how="outer")
    df = df.merge(off, on="fecha", how="outer")
    df = df.merge(dur, on="fecha", how="outer")

    # Filtrar rango fechas
    rango_min = pd.to_datetime(date_from)
    rango_max = pd.to_datetime(date_to)

    df = df[(df["fecha"] >= rango_min) & (df["fecha"] <= rango_max)]

    # Rellenar cantidades Q con 0
    for c in ["Q_Encuestas", "Q_Tickets", "Q_Tickets_Resueltos", "Reopen", "Q_Auditorias", "OFFTIME", "LARGOS"]:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    # Mantener métricas con null si no existieron
    return df.sort_values("fecha")


