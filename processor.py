import pandas as pd
import numpy as np

# ============================================================
# 🔧 LIMPIEZA DE COLUMNAS
# ============================================================

def clean_cols(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    return df


# ============================================================
# 🟦 PROCESAR VENTAS
# ============================================================

def process_ventas(df):
    df = clean_cols(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()

    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("$", "", regex=False)
    )
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = np.where(
        df["ds_product_name"] == "van_compartida",
        df["qt_price_local"],
        0
    )
    df["Ventas_Exclusivas"] = np.where(
        df["ds_product_name"] == "van_exclusive",
        df["qt_price_local"],
        0
    )

    return df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
    })


# ============================================================
# 🟩 PROCESAR PERFORMANCE
# ============================================================

def process_performance(df):
    df = clean_cols(df)

    df = df.rename(columns={"% Firt": "firt_pct", "% Furt": "furt_pct"})

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.normalize()

    df["Q_Ticket"] = 1
    df["Q_Tickets_Resueltos"] = np.where(df["Status"].str.lower() == "solved", 1, 0)
    df["Q_Encuestas"] = np.where(df["CSAT"].notna() | df["NPS Score"].notna(), 1, 0)

    return df.groupby("fecha", as_index=False).agg({
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


# ============================================================
# 🟪 PROCESAR AUDITORÍAS (ROBUSTO)
# ============================================================

def process_auditorias(df):
    df = clean_cols(df)

    candidates = ["Date Time Reference", "Date Time", "ï»¿Date Time"]
    col_fecha = next((c for c in candidates if c in df.columns), None)

    if col_fecha is None:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    def to_date(x):
        if pd.isna(x): return None
        s = str(x).strip()

        for fmt in ("%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
            try: return pd.to_datetime(s, format=fmt).date()
            except: pass
        try: return pd.to_datetime(s).date()
        except: return None

    df["fecha"] = df[col_fecha].apply(to_date)
    df = df[df["fecha"].notna()]
    df["fecha"] = pd.to_datetime(df["fecha"])

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    return df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })


# ============================================================
# 🟧 PROCESAR OFF-TIME
# ============================================================

def process_offtime(df):
    df = clean_cols(df)

    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()

    df["OFF_TIME"] = np.where(
        df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)",
        1, 0
    )

    return df.groupby("fecha", as_index=False).agg({"OFF_TIME": "sum"})


# ============================================================
# 🟥 PROCESAR DURACIÓN >90 MINUTOS
# ============================================================

def process_duracion(df):
    df = clean_cols(df)

    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce").dt.normalize()
    df["Duracion_90"] = np.where(df["Duration (Minutes)"] > 90, 1, 0)

    return df.groupby("fecha", as_index=False).agg({"Duracion_90": "sum"})


# ============================================================
# 🟦 PROCESAR INSPECCIONES VEHICULARES (NUEVO)
# ============================================================

def process_inspecciones(df):
    df = clean_cols(df)

    if "Fecha" not in df.columns:
        return pd.DataFrame(columns=[
            "fecha", "Q_Inspecciones",
            "Cumplimiento_Exterior", "Cumplimiento_Interior", "Cumplimiento_Conductor"
        ])

    df["fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.normalize()

    df["Q_Inspecciones"] = 1

    df["Cumplimiento_Exterior"] = pd.to_numeric(df["Cumplimiento Exterior"], errors="coerce")
    df["Cumplimiento_Interior"] = pd.to_numeric(df["Cumplimiento Interior"], errors="coerce")
    df["Cumplimiento_Conductor"] = pd.to_numeric(df["Cumplimiento Conductor"], errors="coerce")

    return df.groupby("fecha", as_index=False).agg({
        "Q_Inspecciones": "sum",
        "Cumplimiento_Exterior": "mean",
        "Cumplimiento_Interior": "mean",
        "Cumplimiento_Conductor": "mean"
    })


# ============================================================
# 📅 SEMANA HUMANA
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
# 🔵 PROCESAR GLOBAL – DIARIO + SEMANAL + PERIODO
# ============================================================

def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, df_insp, date_from, date_to):

    v = process_ventas(df_ventas)
    p = process_performance(df_perf)
    a = process_auditorias(df_aud)
    o = process_offtime(df_off)
    d = process_duracion(df_dur)
    insp = process_inspecciones(df_insp)

    df = (
        v.merge(p, on="fecha", how="outer")
         .merge(a, on="fecha", how="outer")
         .merge(o, on="fecha", how="outer")
         .merge(d, on="fecha", how="outer")
         .merge(insp, on="fecha", how="outer")
    )

    df = df.sort_values("fecha")
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]

    sum_cols = [
        "Q_Encuestas", "Reopen", "Q_Ticket", "Q_Tickets_Resueltos",
        "Q_Auditorias", "OFF_TIME", "Duracion_90", "Q_Inspecciones",
        "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"
    ]
    for c in sum_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    mean_cols = [
        "CSAT", "NPS Score", "Firt (h)", "Furt (h)",
        "firt_pct", "furt_pct", "Nota_Auditorias",
        "Cumplimiento_Exterior", "Cumplimiento_Interior", "Cumplimiento_Conductor"
    ]
    for c in mean_cols:
        if c in df.columns:
            df[c] = df[c].replace({0: np.nan})

    # ============================================================
    # 📅 SEMANAL
    # ============================================================

    df_sem = df.copy()
    df_sem["Semana"] = df_sem["fecha"].apply(semana_humana)

    agg_dict = {c: "sum" for c in sum_cols}
    agg_dict.update({c: "mean" for c in mean_cols if c in df_sem.columns})

    df_sem = df_sem.groupby("Semana", as_index=False).agg(agg_dict)

    for c in mean_cols:
        if c in df_sem.columns:
            df_sem[c] = df_sem[c].round(2)

    # ============================================================
    # 📅 PERIODO
    # ============================================================

    df_per = df.copy()
    df_per["Periodo"] = f"{date_from.date()} → {date_to.date()}"

    agg_dict_p = {c: "sum" for c in sum_cols}
    agg_dict_p.update({c: "mean" for c in mean_cols if c in df_per.columns})

    df_per = df_per.groupby("Periodo", as_index=False).agg(agg_dict_p)

    for c in mean_cols:
        if c in df_per.columns:
            df_per[c] = df_per[c].round(2)

    return df, df_sem, df_per

