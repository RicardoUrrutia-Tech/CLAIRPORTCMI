import pandas as pd

# ======================================================
# LIMPIAR BOM Y UTF-8 RARO EN COLUMNAS
# ======================================================
def clean_columns(df):
    df.columns = [c.replace("ï»¿", "").replace("\ufeff", "") for c in df.columns]
    return df


# ======================================================
# PROCESAR VENTAS
# ======================================================
def process_ventas(df):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(".", "", regex=False)
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

    return df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum"
    })


# ======================================================
# PROCESAR PERFORMANCE
# ======================================================
def process_performance(df):
    df = clean_columns(df)

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.date

    df["Q_Encuestas"] = df.apply(
        lambda r: 1 if (pd.notna(r["CSAT"]) or pd.notna(r["NPS Score"])) else 0,
        axis=1
    )
    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(lambda x: 1 if str(x).lower() == "solved" else 0)

    return df.groupby("fecha", as_index=False).agg({
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


# ======================================================
# PROCESAR AUDITORÍAS
# ======================================================
def process_auditorias(df):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["Date Time"], errors="coerce").dt.date
    df["Q_Auditorias"] = 1

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Total Audit Score": "mean"
    })

    # Sin auditorías → mostrar "-"
    out["Total Audit Score"] = out["Total Audit Score"].apply(
        lambda x: "-" if pd.isna(x) else x
    )
    return out


# ======================================================
# PROCESAR OFF-TIME
# ======================================================
def process_offtime(df):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.date

    df["Reserva_OffTime"] = df["Segment Arrived to Airport vs Requested"].apply(
        lambda x: 0 if x == "02. A tiempo (0-20 min antes)" else 1
    )

    return df.groupby("fecha", as_index=False).agg({
        "Reserva_OffTime": "sum"
    })


# ======================================================
# PROCESAR DURACIÓN >90 MINUTOS
# ======================================================
def process_duracion(df):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce").dt.date

    df["Viajes_Largos"] = df["Duration (Minutes)"].apply(
        lambda x: 1 if float(x) > 90 else 0
    )

    return df.groupby("fecha", as_index=False).agg({
        "Viajes_Largos": "sum"
    })


# ======================================================
# UNIFICACIÓN GLOBAL
# ======================================================
def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to):

    ventas = process_ventas(df_ventas)
    perf = process_performance(df_perf)
    aud = process_auditorias(df_aud)
    off = process_offtime(df_off)
    dur = process_duracion(df_dur)

    df = ventas.merge(perf, on="fecha", how="outer") \
               .merge(aud, on="fecha", how="outer") \
               .merge(off, on="fecha", how="outer") \
               .merge(dur, on="fecha", how="outer")

    df = df.sort_values("fecha")

    if date_from:
        df = df[df["fecha"] >= date_from]
    if date_to:
        df = df[df["fecha"] <= date_to]

    return df

