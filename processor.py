import pandas as pd
import numpy as np

# ===========================================================
# UTILIDAD – Normalizar fechas
# ===========================================================
def to_date(series):
    return pd.to_datetime(series, errors="coerce").dt.date


# ===========================================================
# PROCESAR VENTAS
# ===========================================================
def process_ventas(df):
    df = df.copy()

    # Fecha oficial
    df["fecha"] = to_date(df["tm_start_local_at"])

    # Venta total
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")
    df["Venta_Total"] = df["qt_price_local"]

    # Venta compartida
    df["Venta_Compartida"] = np.where(
        df["ds_product_name"].str.contains("shared", case=False, na=False),
        df["qt_price_local"], 0
    )

    # Venta exclusiva
    df["Venta_Exclusiva"] = np.where(
        df["ds_product_name"].str.contains("van_exclusive", case=False, na=False),
        df["qt_price_local"], 0
    )

    out = df.groupby("fecha", as_index=False).agg({
        "Venta_Total": "sum",
        "Venta_Compartida": "sum",
        "Venta_Exclusiva": "sum"
    })

    return out


# ===========================================================
# PROCESAR PERFORMANCE
# ===========================================================
def process_performance(df):

    df = df.copy()

    df["fecha"] = to_date(df["Fecha de Referencia"])

    df["Q_Encuestas"] = np.where(df["Origin Contact"] == "Survey", 1, 0)
    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = np.where(df["Status"].isin(["solved","closed"]), 1, 0)

    # Numeric conversion
    numeric_cols = ["% Firt", "% Furt", "Firt (h)", "Furt (h)", "NPS Score", "CSAT"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas": "sum",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum",
        "% Firt": "mean",
        "% Furt": "mean",
        "Firt (h)": "mean",
        "Furt (h)": "mean",
        "NPS Score": "mean",
        "CSAT": "mean",
        "Reopen": "sum"
    })

    return out


# ===========================================================
# PROCESAR AUDITORÍAS
# ===========================================================
def process_auditorias(df):
    df = df.copy()

    df["fecha"] = to_date(df["Date Time"])

    df["Q_Auditorias"] = df["# Audits by Agent"].fillna(0)
    df["Nota_Auditoria"] = df["Total Audit Score"].replace(0, np.nan)

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditoria": "mean"
    })

    return out


# ===========================================================
# PROCESAR OFF-TIME
# ===========================================================
def process_offtime(df):
    df = df.copy()

    df["fecha"] = to_date(df["tm_start_local_at"])

    df["OFFTIME"] = np.where(
        df["Segment Arrived to Airport vs Requested"].str.contains("A tiempo", na=False),
        0,
        1
    )

    out = df.groupby("fecha", as_index=False).agg({
        "OFFTIME": "sum"
    })

    return out


# ===========================================================
# PROCESAR DURACIÓN >90
# ===========================================================
def process_duracion(df):
    df = df.copy()

    df["fecha"] = to_date(df["Start At Local Dt"])

    df["LARGOS"] = np.where(df["Duration (Minutes)"] > 90, 1, 0)

    out = df.groupby("fecha", as_index=False).agg({
        "LARGOS": "sum"
    })

    return out


# ===========================================================
# CONSOLIDADO DIARIO
# ===========================================================
def procesar_global(df_v, df_p, df_a, df_off, df_dur, date_from=None, date_to=None):

    v = process_ventas(df_v)
    p = process_performance(df_p)
    a = process_auditorias(df_a)
    o = process_offtime(df_off)
    d = process_duracion(df_dur)

    df = v.merge(p, on="fecha", how="outer")
    df = df.merge(a, on="fecha", how="outer")
    df = df.merge(o, on="fecha", how="outer")
    df = df.merge(d, on="fecha", how="outer")

    # Ordenar
    df = df.sort_values("fecha")

    # FILTRO DE FECHAS
    if date_from:
        df = df[df["fecha"] >= date_from]
    if date_to:
        df = df[df["fecha"] <= date_to]

    return df


# ===========================================================
# RESUMEN GENERAL DEL PERIODO
# ===========================================================
def resumen_periodo(df):

    sum_cols = [
        "Venta_Total","Venta_Compartida","Venta_Exclusiva",
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos",
        "Reopen","Q_Auditorias","OFFTIME","LARGOS"
    ]

    mean_cols = [
        "% Firt","% Furt","Firt (h)","Furt (h)",
        "NPS Score","CSAT","Nota_Auditoria"
    ]

    resumen = {}

    for c in sum_cols:
        if c in df:
            resumen[c] = df[c].sum()

    for c in mean_cols:
        if c in df:
            resumen[c] = df[c].mean()

    return pd.DataFrame([resumen])


# ===========================================================
# RESUMEN SEMANAL — ETIQUETAS HUMANAS
# ===========================================================
def resumen_semanal(df):
    df = df.copy()

    df["fecha"] = pd.to_datetime(df["fecha"])

    df["semana_inicio"] = df["fecha"] - pd.to_timedelta(df["fecha"].dt.weekday, unit='D')
    df["semana_termino"] = df["semana_inicio"] + pd.Timedelta(days=6)

    meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
             "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

    df["Semana"] = df.apply(
        lambda x: f"{x['semana_inicio'].day} – {x['semana_termino'].day} {meses[x['semana_termino'].month-1]}",
        axis=1
    )

    sum_cols = [
        "Venta_Total","Venta_Compartida","Venta_Exclusiva",
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos",
        "Reopen","Q_Auditorias","OFFTIME","LARGOS"
    ]

    mean_cols = ["% Firt","% Furt","Firt (h)","Furt (h)","NPS Score","CSAT","Nota_Auditoria"]

    agg_dict = {c: "sum" for c in sum_cols if c in df}
    agg_dict.update({c: "mean" for c in mean_cols if c in df})

    semanal = df.groupby("Semana", as_index=False).agg(agg_dict)

    return semanal



