import pandas as pd
import numpy as np

# ---------------------------------------------------------------
# LIMPIAR BOM / ESPACIOS / RENOMBRAR COLUMNAS CLAVES
# ---------------------------------------------------------------

def clean_columns(df: pd.DataFrame):
    df.columns = df.columns.str.replace("ï»¿", "", regex=False)
    df.columns = df.columns.str.strip()
    return df


# ---------------------------------------------------------------
# PROCESAR VENTAS
# ---------------------------------------------------------------

def process_ventas(df: pd.DataFrame):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce")

    df["qt_price_local"] = pd.to_numeric(
        df["qt_price_local"].astype(str).str.replace(",", "").str.replace(".", "", regex=False),
        errors="coerce"
    ) / 1000

    ventas = df.groupby(df["fecha"].dt.date).agg(
        Ventas_Totales=("qt_price_local", "sum"),
        Ventas_Compartidas=("ds_product_name", lambda x: df.loc[x.index, "qt_price_local"][x == "van_compartida"].sum()),
        Ventas_Exclusivas=("ds_product_name", lambda x: df.loc[x.index, "qt_price_local"][x == "van_exclusive"].sum()),
    ).reset_index()

    return ventas



# ---------------------------------------------------------------
# PROCESAR PERFORMANCE
# ---------------------------------------------------------------

def process_performance(df: pd.DataFrame):
    df = clean_columns(df)

    # BOM fixes
    if "ï»¿% Firt" in df.columns:
        df = df.rename(columns={"ï»¿% Firt": "% Firt"})

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce")

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = np.where(df["Status"] == "solved", 1, 0)
    df["Q_Reopen"] = np.where(df["Reopen"] == 1, 1, 0)
    df["Q_Encuestas"] = np.where(df["CSAT"].notna() | df["NPS Score"].notna(), 1, 0)

    perf = df.groupby(df["fecha"].dt.date).agg(
        Q_Encuestas=("Q_Encuestas", "sum"),
        NPS=("NPS Score", "mean"),
        CSAT=("CSAT", "mean"),
        FIRT_H=("Firt (h)", "mean"),
        FURT_H=("Furt (h)", "mean"),
        P_FIRT=("% Firt", "mean"),
        P_FURT=("% Furt", "mean"),
        Q_Reopen=("Q_Reopen", "sum"),
        Q_Tickets=("Q_Tickets", "sum"),
        Q_Tickets_Resueltos=("Q_Tickets_Resueltos", "sum"),
    ).reset_index()

    return perf


# ---------------------------------------------------------------
# PROCESAR AUDITORÍAS
# ---------------------------------------------------------------

def process_auditorias(df: pd.DataFrame):
    df = clean_columns(df)

    if "ï»¿Date Time" in df.columns:
        df = df.rename(columns={"ï»¿Date Time": "Date Time"})

    df["fecha"] = pd.to_datetime(df["Date Time"], format="%d-%m-%Y", errors="coerce")

    df["Q_Auditorias"] = 1
    df["Nota_Auditoria"] = df["Total Audit Score"]

    aud = df.groupby(df["fecha"].dt.date).agg(
        Q_Auditorias=("Q_Auditorias", "sum"),
        Nota_Auditoria=("Nota_Auditoria", "mean"),
    ).reset_index()

    return aud


# ---------------------------------------------------------------
# PROCESAR OFF-TIME
# ---------------------------------------------------------------

def process_offtime(df: pd.DataFrame):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce")

    df["OFF_TIME"] = np.where(df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)", 1, 0)

    off = df.groupby(df["fecha"].dt.date).agg(
        Q_OFF_TIME=("OFF_TIME", "sum")
    ).reset_index()

    return off


# ---------------------------------------------------------------
# PROCESAR DURACIÓN > 90 MINUTOS
# ---------------------------------------------------------------

def process_duracion(df: pd.DataFrame):
    df = clean_columns(df)

    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce")

    df["Viajes_Mayor_90"] = np.where(df["Duration (Minutes)"] > 90, 1, 0)

    dur = df.groupby(df["fecha"].dt.date).agg(
        Q_Viajes_Mayor_90=("Viajes_Mayor_90", "sum")
    ).reset_index()

    return dur


# ---------------------------------------------------------------
# PROCESAR CONSOLIDADO GLOBAL
# ---------------------------------------------------------------

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

    cantidades = [
        "Q_Encuestas","Q_Reopen","Q_Tickets","Q_Tickets_Resueltos",
        "Q_Auditorias","Q_OFF_TIME","Q_Viajes_Mayor_90"
    ]
    for c in cantidades:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    promedios_nulos = [
        "NPS","CSAT","FIRT_H","FURT_H","P_FIRT","P_FURT","Nota_Auditoria",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]
    for c in promedios_nulos:
        if c in df.columns:
            df[c] = df[c].replace({np.nan: None})

    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]
    df = df.sort_values("fecha").reset_index(drop=True)

    # Semana humana: Ej. “24–30 Noviembre”
    df["fecha_dt"] = pd.to_datetime(df["fecha"])
    df["Semana"] = df["fecha_dt"].dt.isocalendar().week

    semana_label = []
    for w in df["Semana"]:
        dias = df[df["Semana"] == w]["fecha_dt"]
        semana_label.append(f"{dias.min().day}–{dias.max().day} {dias.min():%B}")

    df["Semana_Label"] = semana_label

    semanal = df.groupby("Semana_Label").sum(numeric_only=True).reset_index()
    mensual = df.groupby(df["fecha_dt"].dt.to_period("M")).sum(numeric_only=True).reset_index()

    return df, semanal, mensual

