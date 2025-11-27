import pandas as pd
import numpy as np
from datetime import datetime

def log(msg):
    print("\n" + "="*60)
    print(msg)
    print("="*60 + "\n")


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

    for fmt in ("%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass

    try:
        return pd.to_datetime(s).date()
    except:
        return None


# ================================
# PROCESOS
# ================================

def process_ventas(df, date_from, date_to):
    log("PROCESS VENTAS")
    log(df.columns)

    df = normalize(df.copy())
    df["fecha"] = df["date"].apply(to_date)

    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    df["qt_price_local"] = (
        df["qt_price_local"].astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
        .str.strip()
    )
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]).lower() == "van_compartida" else 0,
        axis=1,
    )
    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]).lower() == "van_exclusive" else 0,
        axis=1,
    )

    return df.groupby("fecha", as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()


def process_performance(df, date_from, date_to):
    log("PROCESS PERFORMANCE")
    log(df.columns)

    df = normalize(df.copy())
    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if ((not pd.isna(x.get("CSAT"))) or (not pd.isna(x.get("NPS Score")))) else 0,
        axis=1,
    )
    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )

    convertibles = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt", "Reopen"]
    for col in convertibles:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas": "sum",
        "CSAT": "mean",
        "NPS Score": "mean",
        "Firt (h)": "mean",
        "% Firt": "mean",
        "Furt (h)": "mean",
        "% Furt": "mean",
        "Reopen": "sum",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum",
    })

    return out.rename(columns={
        "NPS Score": "NPS",
        "Firt (h)": "FIRT",
        "% Firt": "%FIRT",
        "Furt (h)": "FURT",
        "% Furt": "%FURT",
        "Reopen": "Q_Reopen"
    })


def process_auditorias(df, date_from, date_to):
    log("PROCESS AUDITORIAS")
    log(df.columns)

    df = normalize(df.copy())
    df["fecha"] = df["Date Time"].apply(to_date)
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    return df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })


def process_off_time(df, date_from, date_to):
    log("PROCESS OFFTIME")
    log(df.columns)

    df = normalize(df.copy())

    df["fecha"] = pd.to_datetime(df["tm_airport_arrival_requested_local_at"], errors="coerce").dt.date
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    df["Q_Reservas_Off_Time"] = df["Segment Arrived to Airport vs Requested"].apply(
        lambda x: 1 if str(x).strip() != "02. A tiempo (0-20 min antes)" else 0
    )

    return df.groupby("fecha", as_index=False)[["Q_Reservas_Off_Time"]].sum()


def process_duracion(df, date_from, date_to):
    log("PROCESS DURACION >90")
    log(df.columns)

    df = normalize(df.copy())

    df["fecha"] = df["Start At Local Dt"].apply(to_date)
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    df["Duration (Minutes)"] = pd.to_numeric(df["Duration (Minutes)"], errors="coerce")

    df["Q_Viajes_90mas"] = df["Duration (Minutes)"].apply(
        lambda x: 1 if (pd.notna(x) and x > 90) else 0
    )

    return df.groupby("fecha", as_index=False)[["Q_Viajes_90mas"]].sum()


# ============================================================
# CONSOLIDADO
# ============================================================
def build_daily_global(dfs):
    merged = None
    for df in dfs:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(merged, df, on="fecha", how="outer")

    if merged is None:
        return pd.DataFrame()

    merged = merged.sort_values("fecha")

    for col in merged.columns:
        if col.startswith("Q_") or col.startswith("Ventas"):
            merged[col] = merged[col].fillna(0)

    return merged


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================
def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to):

    log("GLOBAL INPUT – COLUMNAS DE ENTRADA")
    log(df_ventas.columns)
    log(df_perf.columns)
    log(df_aud.columns)
    log(df_off.columns)
    log(df_dur.columns)

    ventas = process_ventas(df_ventas, date_from, date_to)
    performance = process_performance(df_perf, date_from, date_to)
    auditorias = process_auditorias(df_aud, date_from, date_to)
    offtime = process_off_time(df_off, date_from, date_to)
    duracion = process_duracion(df_dur, date_from, date_to)

    log("GLOBAL – MERGE FINAL")
    return build_daily_global([ventas, performance, auditorias, offtime, duracion])

