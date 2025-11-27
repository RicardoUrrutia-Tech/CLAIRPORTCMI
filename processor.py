import pandas as pd
import numpy as np


# ============================================================
# PARSEO ROBUSTO DE FECHAS
# ============================================================
def parse_date_safe(series, formats):
    for fmt in formats:
        try:
            return pd.to_datetime(series, format=fmt, errors="coerce")
        except:
            pass

    return pd.to_datetime(series, errors="coerce")


# ============================================================
# PROCESO DE VENTAS
# ============================================================
def process_ventas(df):
    if df.empty:
        return pd.DataFrame(columns=[
            "fecha", "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"
        ])

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")

    df["fecha"] = parse_date_safe(
        df["tm_start_local_at"],
        ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S.%f"]
    ).dt.date

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = np.where(df["ds_product_name"] == "van_compartida", df["qt_price_local"], 0)
    df["Ventas_Exclusivas"] = np.where(df["ds_product_name"] == "van_exclusive", df["qt_price_local"], 0)

    out = df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum"
    })

    return out


# ============================================================
# PROCESO DE PERFORMANCE
# ============================================================
def process_performance(df):
    if df.empty:
        return pd.DataFrame(columns=[
            "fecha", "Q_Encuestas", "NPS", "CSAT", "FIRT", "%FIRT",
            "FURT", "%FURT", "Q_Reopen", "Q_Tickets", "Q_Tickets_Resueltos"
        ])

    df = df[df["Group Support Service"] == "C_Ops Support"].copy()

    numeric_cols = [
        "% Firt", "% Furt", "CSAT", "Firt (h)", "Furt (h)",
        "NPS Score", "Reopen"
    ]

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = np.where(df["Status"] == "solved", 1, 0)
    df["Q_Encuestas"] = np.where(df["CSAT"].notna() | df["NPS Score"].notna(), 1, 0)

    df["fecha"] = parse_date_safe(
        df["Fecha de Referencia"],
        ["%m/%d/%Y", "%Y-%m-%d"]
    ).dt.date

    agg_dict = {
        "Q_Encuestas": "sum",
        "CSAT": "mean",
        "NPS Score": "mean",
        "Firt (h)": "mean",
        "% Firt": "mean",
        "Furt (h)": "mean",
        "% Furt": "mean",
        "Reopen": "sum",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum"
    }

    out = df.groupby("fecha", as_index=False).agg(agg_dict)

    # Forzar inclusión de %FIRT si desapareció por puro NaN
    if "% Firt" not in out.columns:
        out["% Firt"] = np.nan

    return out.rename(columns={
        "NPS Score": "NPS",
        "Firt (h)": "FIRT",
        "% Firt": "%FIRT",
        "Furt (h)": "FURT",
        "% Furt": "%FURT",
        "Reopen": "Q_Reopen"
    })


# ============================================================
# PROCESO DE AUDITORÍAS
# ============================================================
def process_auditorias(df):
    if df.empty:
        return pd.DataFrame(columns=[
            "fecha", "Q_Auditorias", "Nota_Auditorias"
        ])

    df["fecha"] = parse_date_safe(
        df["Date Time"],
        ["%d-%m-%Y", "%Y-%m-%d"]
    ).dt.date

    df["Q_Auditorias"] = 1

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Total Audit Score": "mean"
    })

    out = out.rename(columns={"Total Audit Score": "Nota_Auditorias"})
    out["Nota_Auditorias"] = out["Nota_Auditorias"].round(2)

    return out


# ============================================================
# PROCESO OFF-TIME
# ============================================================
def process_offtime(df):
    if df.empty:
        return pd.DataFrame(columns=["fecha", "Q_OffTime"])

    df["fecha"] = parse_date_safe(
        df["tm_start_local_at"],
        ["%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"]
    ).dt.date

    df["Q_OffTime"] = np.where(
        df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)",
        1,
        0
    )

    return df.groupby("fecha", as_index=False).agg({"Q_OffTime": "sum"})


# ============================================================
# PROCESO DURACIÓN > 90 MINUTOS
# ============================================================
def process_duration(df):
    if df.empty:
        return pd.DataFrame(columns=["fecha", "Q_Duracion90"])

    df["fecha"] = parse_date_safe(
        df["Start At Local Dttm"],
        ["%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"]
    ).dt.date

    df["Q_Duracion90"] = np.where(df["Duration (Minutes)"] > 90, 1, 0)

    return df.groupby("fecha", as_index=False).agg({"Q_Duracion90": "sum"})


# ============================================================
# PROCESO GLOBAL FINAL
# ============================================================
def procesar_global(dfv, dfp, dfa, dfo, dfd, date_from=None, date_to=None):
    ventas = process_ventas(dfv)
    perf = process_performance(dfp)
    aud = process_auditorias(dfa)
    off = process_offtime(dfo)
    dur = process_duration(dfd)

    df = ventas \
        .merge(perf, on="fecha", how="outer") \
        .merge(aud, on="fecha", how="outer") \
        .merge(off, on="fecha", how="outer") \
        .merge(dur, on="fecha", how="outer")

    # Rango de fechas
    if date_from:
        df = df[df["fecha"] >= date_from]
    if date_to:
        df = df[df["fecha"] <= date_to]

    df = df.sort_values("fecha")

    indicadores_q = [
        "Q_Encuestas",
        "Q_Reopen",
        "Q_Tickets",
        "Q_Tickets_Resueltos",
        "Q_Auditorias",
        "Q_OffTime",
        "Q_Duracion90"
    ]

    for col in indicadores_q:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].fillna(0).astype(int)

    df["Nota_Auditorias"] = df["Nota_Auditorias"].fillna("-")
    df["FIRT"] = df["FIRT"].round(2)
    df["FURT"] = df["FURT"].round(2)
    df["NPS"] = df["NPS"].round(2)
    df["CSAT"] = df["CSAT"].round(2)

    return df


