import pandas as pd
import numpy as np
from datetime import datetime

# =======================================================
# HELPERS
# =======================================================
def normalize(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("﻿", "")
        .str.replace('"', "")
        .str.strip()
    )
    return df


def to_date(x):
    if pd.isna(x):
        return None

    s = str(x).strip()

    # YYYY/MM/DD
    if "/" in s and len(s.split("/")[0]) == 4:
        try: return datetime.strptime(s, "%Y/%m/%d").date()
        except: pass

    # DD-MM-YYYY
    if "-" in s and len(s.split("-")[2]) == 4:
        try: return datetime.strptime(s, "%d-%m-%Y").date()
        except: pass

    # MM/DD/YYYY
    if "/" in s and len(s.split("/")[2]) == 4:
        try: return datetime.strptime(s, "%m/%d/%Y").date()
        except: pass

    try:
        return pd.to_datetime(s).date()
    except:
        return None


# =======================================================
# MÓDULOS DE PROCESO
# =======================================================
def process_ventas(df):
    print("DEBUG PROCESS VENTAS")
    print(df.head())

    df = normalize(df.copy())
    df["fecha"] = df["date"].apply(to_date)

    df["qt_price_local"] = (
        df["qt_price_local"].astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
    )
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = df.apply(lambda x: x["qt_price_local"] if str(x["ds_product_name"]).lower() == "van_compartida" else 0, axis=1)
    df["Ventas_Exclusivas"] = df.apply(lambda x: x["qt_price_local"] if str(x["ds_product_name"]).lower() == "van_exclusive" else 0, axis=1)

    return df.groupby("fecha", as_index=False)[["Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"]].sum()


def process_performance(df):
    print("DEBUG PROCESS PERFORMANCE")
    print(df.head())

    df = normalize(df.copy())
    df = df[df["Group Support Service"] == "C_Ops Support"]
    df["fecha"] = df["Fecha de Referencia"].apply(to_date)

    df["Q_Encuestas"] = df.apply(lambda x: 1 if ((not pd.isna(x.get("CSAT"))) or (not pd.isna(x.get("NPS Score")))) else 0, axis=1)
    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(lambda x: 1 if str(x).strip().lower() == "solved" else 0)

    convert = ["CSAT","NPS Score","Firt (h)","Furt (h)","% Firt","% Furt","Reopen"]
    for col in convert:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas":"sum",
        "CSAT":"mean",
        "NPS Score":"mean",
        "Firt (h)":"mean",
        "% Firt":"mean",
        "Furt (h)":"mean",
        "% Furt":"mean",
        "Reopen":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum"
    })

    return out.rename(columns={
        "NPS Score":"NPS",
        "Firt (h)":"FIRT",
        "% Firt":"%FIRT",
        "Furt (h)":"FURT",
        "% Furt":"%FURT",
        "Reopen":"Q_Reopen"
    })


def process_auditorias(df):
    print("DEBUG PROCESS AUDITORIAS")
    print(df.head())

    df = normalize(df.copy())
    df["fecha"] = df["Date Time"].apply(to_date)
    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    return df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean"
    })


def process_off_time(df):
    print("DEBUG PROCESS OFFTIME")
    print(df.head())

    df = normalize(df.copy())
    df["fecha"] = pd.to_datetime(df["tm_airport_arrival_requested_local_at"], errors="coerce").dt.date

    df["Q_Reservas_Off_Time"] = df["Segment Arrived to Airport vs Requested"].apply(
        lambda x: 1 if str(x).strip() != "02. A tiempo (0-20 min antes)" else 0
    )

    return df.groupby("fecha", as_index=False)[["Q_Reservas_Off_Time"]].sum()


# =======================================================
# CONSOLIDADO
# =======================================================
def build_daily_global(dfs):

    merged = None
    for df in dfs:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(merged, df, on="fecha", how="outer")

    if merged is None:
        return pd.DataFrame()

    merged = merged.sort_values("fecha")

    Qcols = ["Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
             "Q_Auditorias","Q_Reservas_Off_Time","Ventas_Totales",
             "Ventas_Compartidas","Ventas_Exclusivas"]

    for col in Qcols:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    return merged


# =======================================================
# FUNCIÓN PRINCIPAL
# =======================================================
def procesar_global(df_ventas, df_performance, df_auditorias, df_offtime):

    print("=== DEBUG GLOBAL INPUTS ===")
    print("VENTAS RAW:", df_ventas.head())
    print("PERFORMANCE RAW:", df_performance.head())
    print("AUDITORIAS RAW:", df_auditorias.head())
    print("OFFTIME RAW:", df_offtime.head())

    ventas = process_ventas(df_ventas)
    performance = process_performance(df_performance)
    auditorias = process_auditorias(df_auditorias)
    offtime = process_off_time(df_offtime)

    return build_daily_global([ventas, performance, auditorias, offtime])
