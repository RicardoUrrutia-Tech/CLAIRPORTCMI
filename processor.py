# ===============================================================
#   ⬛⬛⬛  PROCESSOR_GLOBAL.PY - CONSOLIDADO SIN AGENTES  ⬛⬛⬛
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ------------------------------
# Función para transformar fechas
# ------------------------------
def to_date(x):
    if pd.isna(x):
        return None
    s = str(x).strip()

    # YYYY/MM/DD
    if "/" in s and len(s.split("/")[0]) == 4:
        try: return datetime.strptime(s, "%Y/%m/%d").date()
        except: pass

    # DD-MM-YYYY
    if "-" in s and len(s.split("-")[2]) == 4 and len(s.split("-")[0]) <= 2:
        try: return datetime.strptime(s, "%d-%m-%Y").date()
        except: pass

    # MM/DD/YYYY
    if "/" in s and len(s.split("/")[2]) == 4:
        try: return datetime.strptime(s, "%m/%d/%Y").date()
        except: pass

    try: return pd.to_datetime(s).date()
    except: return None

# ------------------------------
# Normalizar encabezados
# ------------------------------
def normalize_headers(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace('"', '')
        .str.replace("﻿", "")
        .str.replace("\t", " ")
        .str.replace("\n", "")
        .str.replace("  ", " ")
    )
    return df

# =========================================================
# PROCESO VENTAS
# =========================================================
def process_ventas(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())
    df["fecha"] = df["date"].apply(to_date)

    df["qt_price_local"] = (
        df["qt_price_local"].astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
        .str.strip()
    )

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]

    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).strip().lower() == "van_compartida" else 0,
        axis=1
    )

    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).strip().lower() == "van_exclusive" else 0,
        axis=1
    )

    return df.groupby("fecha", as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()

# =========================================================
# PROCESO PERFORMANCE
# =========================================================
def process_performance(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if ((not pd.isna(x.get("CSAT"))) or (not pd.isna(x.get("NPS Score")))) else 0,
        axis=1
    )
    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )
    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    # Convertir numéricos
    convertibles = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for col in convertibles:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas":"sum",
        "CSAT":"mean",
        "NPS Score":"mean",
        "Firt (h)":"mean",
        "% Firt":"mean",
        "Furt (h)":"mean",
        "% Furt":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum"
    })

    return out.rename(columns={
        "NPS Score":"NPS",
        "Firt (h)":"FIRT",
        "% Firt":"%FIRT",
        "Furt (h)":"FURT",
        "% Furt":"%FURT"
    })

# =========================================================
# PROCESO AUDITORÍAS
# =========================================================
def process_auditorias(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())
    df["fecha"] = df["Date Time"].apply(to_date)

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean"
    })

    return out

# =========================================================
# CONSOLIDADO DIARIO GLOBAL
# =========================================================
def build_daily_global(dfs):
    merged = None
    for df in dfs:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(
                merged, df, on="fecha", how="outer"
            )

    if merged is None:
        return pd.DataFrame()

    merged = merged.sort_values("fecha")

    # Rellenar solo los indicadores de cantidad
    Q_cols = [
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos",
        "Q_Reopen","Q_Auditorias",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]
    for col in Q_cols:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    # Promedios → dejar NaN (se formatearán en app)
    return merged

# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def procesar_global(df_ventas, df_performance, df_auditorias):

    ventas = process_ventas(df_ventas)
    performance = process_performance(df_performance)
    auditorias = process_auditorias(df_auditorias)

    diario = build_daily_global([ventas, performance, auditorias])

    return diario

