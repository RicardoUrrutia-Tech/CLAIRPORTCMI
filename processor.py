import pandas as pd
import numpy as np

# ============================================================
# 🔧 LIMPIEZA UNIVERSAL DE COLUMNAS (UTF, BOM, ÍCONOS ROTOS)
# ============================================================

def clean_cols(df):
    df.columns = df.columns.str.replace("ï»¿", "", regex=False).str.strip()
    return df


# ============================================================
# 🟦 PROCESO VENTAS
# ============================================================
def process_ventas(df):
    df = clean_cols(df)

    # Fecha oficial del reporte
    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce")

    # Filtro por fechas se aplica después en procesar_global

    # Conversión segura del monto
    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")

    # Indicadores
    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = np.where(df["ds_product_name"] == "van_compartida", df["qt_price_local"], 0)
    df["Ventas_Exclusivas"] = np.where(df["ds_product_name"] == "van_exclusive", df["qt_price_local"], 0)

    diario = df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
    })

    return diario


# ============================================================
# 🟩 PROCESO PERFORMANCE
# ============================================================
def process_performance(df):
    df = clean_cols(df)

    # Renombrar % Firt => firt_pct
    df = df.rename(columns={"% Firt": "firt_pct", "% Furt": "furt_pct"})

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce")

    df["Q_Ticket"] = 1
    df["Q_Tickets_Resueltos"] = np.where(df["Status"].str.lower() == "solved", 1, 0)

    df["Q_Encuestas"] = np.where(df["CSAT"].notna() | df["NPS Score"].notna(), 1, 0)

    # Promedios diarios
    diario = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas": "sum",
        "CSAT": "mean",
        "NPS Score": "mean",
        "Firt (h)": "mean",
        "firt_pct": "mean",
        "Furt (h)": "mean",
        "furt_pct": "mean",
        "Reopen": "sum",
        "Q_Ticket": "sum",
        "Q_Tickets_Resueltos": "sum"
    })

    return diario


# ============================================================
# 🟪 PROCESO AUDITORÍAS
# ============================================================
def process_auditorias(df):
    df = clean_cols(df)

    df["fecha"] = pd.to_datetime(df["Date Time"], format="%d-%m-%Y", errors="coerce")

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = df["Total Audit Score"]

    diario = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })

    return diario


# ============================================================
# 🟧 PROCESO OFF-TIME
# ============================================================
def process_offtime(df):
    df = clean_cols(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce")

    df["OFF_TIME"] = np.where(
        df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)",
        1,
        0
    )

    diario = df.groupby("fecha", as_index=False).agg({
        "OFF_TIME": "sum"
    })

    return diario


# ============================================================
# 🟥 PROCESO DURACIÓN >90
# ============================================================
def process_duracion(df):
    df = clean_cols(df)

    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce")

    df["Duracion_90"] = np.where(df["Duration (Minutes)"] > 90, 1, 0)

    diario = df.groupby("fecha", as_index=False).agg({
        "Duracion_90": "sum"
    })

    return diario


# ============================================================
# 📅 FORMATEO SEMANAS HUMANAS "24–30 Noviembre"
# ============================================================
def semana_humana(fecha):
    lunes = fecha - pd.Timedelta(days=fecha.weekday())
    domingo = lunes + pd.Timedelta(days=6)

    meses = {
        1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
        7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"
    }

    return f"{lunes.day}-{domingo.day} {meses[domingo.month]}"


# ============================================================
# 🟦 FUNCIÓN MAESTRA – CONSOLIDADO GLOBAL
# ============================================================
def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to):

    # Procesar todos los módulos
    v = process_ventas(df_ventas)
    p = process_performance(df_perf)
    a = process_auditorias(df_aud)
    o = process_offtime(df_off)
    d = process_duracion(df_dur)

    # Merge maestro
    df = (
        v.merge(p, on="fecha", how="outer")
         .merge(a, on="fecha", how="outer")
         .merge(o, on="fecha", how="outer")
         .merge(d, on="fecha", how="outer")
    )

    # Ordenar y filtrar
    df = df.sort_values("fecha")
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    # Reemplazar nulos en indicadores numéricos
    q_cols = ["Q_Encuestas", "Reopen", "Q_Ticket", "Q_Tickets_Resueltos",
              "Q_Auditorias", "OFF_TIME", "Duracion_90",
              "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]

    for c in q_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    # Los promedios deben ser “–” si no hubo datos
    avg_cols = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)",
                "firt_pct", "furt_pct", "Nota_Auditorias"]

    for c in avg_cols:
        if c in df.columns:
            df[c] = df[c].replace({0: np.nan})
            df[c] = df[c].fillna("–")

    # ===== RESUMEN SEMANAL =====
    df_sem = df.copy()
    df_sem["Semana"] = df_sem["fecha"].apply(semana_humana)

    df_sem = df_sem.groupby("Semana", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
        "Q_Encuestas": "sum",
        "Reopen": "sum",
        "Q_Ticket": "sum",
        "Q_Tickets_Resueltos": "sum",
        "Q_Auditorias": "sum",
        "OFF_TIME": "sum",
        "Duracion_90": "sum"
    })

    # ===== RESUMEN DEL PERIODO =====
    df_per = df_sem.copy()
    df_per["Periodo"] = f"{date_from.date()} → {date_to.date()}"
    df_per = df_per.groupby("Periodo", as_index=False).sum()

    return df, df_sem, df_per

