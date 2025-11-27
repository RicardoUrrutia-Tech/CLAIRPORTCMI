import pandas as pd

# ============================================
# LIMPIAR UTF-8 BOM SI EXISTE
# ============================================
def limpiar_bom(df):
    df.columns = [col.replace("\ufeff", "") for col in df.columns]
    return df


# ============================================
# PROCESAR VENTAS
# ============================================
def process_ventas(df):
    df = limpiar_bom(df)

    # Convertir fecha
    df["fecha"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Convertir valor de venta correctamente (sin dividir por 1000)
    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(".", "", regex=False)   # 11.990 → 11990
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    df["Ventas_Totales"] = df["qt_price_local"]

    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_compartida" else 0,
        axis=1
    )

    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_exclusive" else 0,
        axis=1
    )

    out = df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum"
    })

    return out


# ============================================
# PROCESAR PERFORMANCE
# ============================================
def process_performance(df):
    df = limpiar_bom(df)

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.date

    df["Q_Encuestas"] = df.apply(
        lambda r: 1 if (pd.notna(r["CSAT"]) or pd.notna(r["NPS Score"])) else 0,
        axis=1
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(lambda x: 1 if str(x).lower() == "solved" else 0)

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas": "sum",
        "CSAT": "mean",
        "NPS Score": "mean",
        "% Firt": "mean",
        "% Furt": "mean",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum",
        "Reopen": "sum",
        "Firt (h)": "mean",
        "Furt (h)": "mean"
    })

    return out


# ============================================
# PROCESAR AUDITORÍAS
# ============================================
def process_auditorias(df):
    df = limpiar_bom(df)

    df["fecha"] = pd.to_datetime(df["Date Time"], errors="coerce").dt.date

    df["Q_Auditorias"] = 1

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Total Audit Score": "mean"
    })

    return out


# ============================================
# PROCESAR RESERVAS OFF-TIME
# ============================================
def process_offtime(df):
    df = limpiar_bom(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.date

    df["Reserva_OffTime"] = df["Segment Arrived to Airport vs Requested"].apply(
        lambda x: 0 if x == "02. A tiempo (0-20 min antes)" else 1
    )

    out = df.groupby("fecha", as_index=False).agg({
        "Reserva_OffTime": "sum"
    })

    return out


# ============================================
# PROCESAR DURACIÓN >90 MIN
# ============================================
def process_duracion(df):
    df = limpiar_bom(df)

    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce").dt.date

    df["Viajes_Largos"] = df["Duration (Minutes)"].apply(lambda x: 1 if float(x) > 90 else 0)

    out = df.groupby("fecha", as_index=False).agg({
        "Viajes_Largos": "sum"
    })

    return out


# ============================================
# UNIFICACIÓN GLOBAL
# ============================================
def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to):

    ventas = process_ventas(df_ventas)
    perf = process_performance(df_perf)
    aud = process_auditorias(df_aud)
    off = process_offtime(df_off)
    dur = process_duracion(df_dur)

    df = ventas.merge(perf, on="fecha", how="outer")
    df = df.merge(aud, on="fecha", how="outer")
    df = df.merge(off, on="fecha", how="outer")
    df = df.merge(dur, on="fecha", how="outer")

    df = df.sort_values("fecha")

    if date_from:
        df = df[df["fecha"] >= date_from]
    if date_to:
        df = df[df["fecha"] <= date_to]

    # Si no existen


