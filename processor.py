import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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

    # tm_start_local_at → fecha
    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()

    # Monto / precio
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
        df["qt_price_local"], 0
    )
    df["Ventas_Exclusivas"] = np.where(
        df["ds_product_name"] == "van_exclusive",
        df["qt_price_local"], 0
    )

    diario = df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
    })

    return diario

# ============================================================
# 🟩 PROCESAR PERFORMANCE
# ============================================================

def process_performance(df):
    df = clean_cols(df)

    df = df.rename(columns={"% Firt": "firt_pct", "% Furt": "furt_pct"})

    # Fecha de Referencia (MM/DD/YYYY)
    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.normalize()

    df["Q_Ticket"] = 1

    # Resueltos = todo menos pending
    status = df["Status"].astype(str).str.lower().str.strip()
    df["Q_Tickets_Resueltos"] = np.where(status != "pending", 1, 0)

    # Encuestas
    df["Q_Encuestas"] = np.where(
        df["CSAT"].notna() | df["NPS Score"].notna(),
        1, 0
    )

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
# 🟪 PROCESAR AUDITORÍAS (ROBUSTO + NOTA SIEMPRE DISPONIBLE)
# ============================================================

def process_auditorias(df):
    df = clean_cols(df)

    # Columnas posibles de fecha
    candidates = ["Date Time Reference", "Date Time", "ï»¿Date Time"]
    col_fecha = next((c for c in candidates if c in df.columns), None)

    if col_fecha is None:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    def to_date(x):
        """Parseo robusto DAYFIRST como en la versión que sí funcionaba completamente."""
        if pd.isna(x):
            return None

        # Número de Excel
        if isinstance(x, (int, float)):
            try:
                if x > 30000:
                    return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
            except:
                pass

        s = str(x).strip()

        # Formatos explícitos
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except:
                pass

        # Fallback general
        try:
            return pd.to_datetime(s, dayfirst=True).date()
        except:
            return None

    df["fecha"] = df[col_fecha].apply(to_date)
    df = df[df["fecha"].notna()]
    df["fecha"] = pd.to_datetime(df["fecha"])

    # Limpieza de puntaje
    if "Total Audit Score" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    score_raw = (
        df["Total Audit Score"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )

    df["Nota_Auditorias"] = pd.to_numeric(score_raw, errors="coerce")

    # Si no se pudo parsear alguna nota, poner 0 para evitar días vacíos
    df["Nota_Auditorias"] = df["Nota_Auditorias"].fillna(0)

    df["Q_Auditorias"] = 1

    diario = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })

    return diario

# ============================================================
# 🟧 OTROS PROCESAD0RES
# ============================================================

def process_offtime(df):
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()
    df["OFF_TIME"] = np.where(
        df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)",
        1, 0
    )
    return df.groupby("fecha", as_index=False).agg({"OFF_TIME": "sum"})


def process_duracion(df):
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce").dt.normalize()
    df["Duracion_90"] = np.where(df["Duration (Minutes)"] > 90, 1, 0)
    return df.groupby("fecha", as_index=False).agg({"Duracion_90": "sum"})


def process_duracion30(df):
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Day of tm_start_local_at"], errors="coerce").dt.normalize()
    df["Duracion_30"] = 1
    return df.groupby("fecha", as_index=False).agg({"Duracion_30": "sum"})


def process_inspecciones(df):
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.normalize()

    df["Cumplimiento_Exterior"] = pd.to_numeric(df["Cumplimiento Exterior"], errors="coerce")
    df["Cumplimiento_Interior"] = pd.to_numeric(df["Cumplimiento Interior"], errors="coerce")
    df["Cumplimiento_Conductor"] = pd.to_numeric(df["Cumplimiento Conductor"], errors="coerce")

    df["Inspecciones_Q"] = 1

    df["Cump_Exterior"] = (df["Cumplimiento_Exterior"] == 100).astype(int)
    df["Incump_Exterior"] = (
        (df["Cumplimiento_Exterior"] < 100) & df["Cumplimiento_Exterior"].notna()
    ).astype(int)

    df["Cump_Interior"] = (df["Cumplimiento_Interior"] == 100).astype(int)
    df["Incump_Interior"] = (
        (df["Cumplimiento_Interior"] < 100) & df["Cumplimiento_Interior"].notna()
    ).astype(int)

    df["Cump_Conductor"] = (df["Cumplimiento_Conductor"] == 100).astype(int)
    df["Incump_Conductor"] = (
        (df["Cumplimiento_Conductor"] < 100) & df["Cumplimiento_Conductor"].notna()
    ).astype(int)

    diario = df.groupby("fecha", as_index=False).agg({
        "Inspecciones_Q": "sum",
        "Cump_Exterior": "sum",
        "Incump_Exterior": "sum",
        "Cump_Interior": "sum",
        "Incump_Interior": "sum",
        "Cump_Conductor": "sum",
        "Incump_Conductor": "sum",
    })

    return diario


def process_abandonados(df):
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Marca temporal"], errors="coerce").dt.normalize()
    df["Abandonados"] = 1
    return df.groupby("fecha", as_index=False).agg({"Abandonados": "sum"})


def process_rescates(df):
    df = clean_cols(df)
    if "Start At Local Dttm" not in df.columns or "User Email" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Rescates"])

    df["User Email"] = df["User Email"].astype(str).str.lower().str.strip()
    df = df[df["User Email"] == "emergencias.excellence.cl@cabify.com"]

    df["fecha"] = pd.to_datetime(df["Start At Local Dttm"], errors="coerce").dt.normalize()
    df["Rescates"] = 1
    return df.groupby("fecha", as_index=False).agg({"Rescates": "sum"})


def process_whatsapp(df):
    df = clean_cols(df)
    if "Created At Local Dt" not in df.columns:
        return pd.DataFrame(columns=["fecha","Q_Tickets_WA"])
    df["fecha"] = pd.to_datetime(df["Created At Local Dt"], errors="coerce").dt.normalize()
    df["Q_Tickets_WA"] = 1
    return df.groupby("fecha", as_index=False).agg({"Q_Tickets_WA": "sum"})

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
# 🔵 PROCESAR GLOBAL
# ============================================================

def procesar_global(
    df_ventas, df_perf, df_aud, df_off, df_dur, df_dur30,
    df_insp, df_aband, df_resc, df_whatsapp,
    date_from, date_to
):
    v = process_ventas(df_ventas)
    p = process_performance(df_perf)
    a = process_auditorias(df_aud)
    o = process_offtime(df_off)
    d = process_duracion(df_dur)
    d30 = process_duracion30(df_dur30)
    insp = process_inspecciones(df_insp)
    ab = process_abandonados(df_aband)
    resc = process_rescates(df_resc)
    wa = process_whatsapp(df_whatsapp)

    # MERGE
    df = (
        v.merge(p, on="fecha", how="outer")
         .merge(a, on="fecha", how="outer")
         .merge(o, on="fecha", how="outer")
         .merge(d, on="fecha", how="outer")
         .merge(d30, on="fecha", how="outer")
         .merge(insp, on="fecha", how="outer")
         .merge(ab, on="fecha", how="outer")
         .merge(resc, on="fecha", how="outer")
         .merge(wa, on="fecha", how="outer")
    )

    # Filtrar rango
    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)]
    df = df.sort_values("fecha")

    sum_cols = [
        "Q_Encuestas", "Reopen", "Q_Ticket", "Q_Tickets_Resueltos",
        "Q_Tickets_WA",
        "Q_Auditorias", "OFF_TIME", "Duracion_90", "Duracion_30",
        "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas",
        "Inspecciones_Q", "Abandonados", "Rescates",
        "Cump_Exterior", "Incump_Exterior",
        "Cump_Interior", "Incump_Interior",
        "Cump_Conductor", "Incump_Conductor",
    ]

    mean_cols = [
        "CSAT", "NPS Score", "Firt (h)", "Furt (h)",
        "firt_pct", "furt_pct", "Nota_Auditorias",
    ]

    # Relleno sumas
    for c in sum_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    # Promedios: OJO → NO convertir nota de auditoría 0 en NaN
    for c in mean_cols:
        if c in df.columns and c != "Nota_Auditorias":
            df[c] = df[c].replace({0: np.nan})

    # ---------------------------------------------------------
    # SEMANAL
    df_sem = df.copy()
    df_sem["Semana"] = df_sem["fecha"].apply(semana_humana)
    agg = {c: "sum" for c in sum_cols}
    agg.update({c: "mean" for c in mean_cols})
    df_sem = df_sem.groupby("Semana", as_index=False).agg(agg)

    # ---------------------------------------------------------
    # PERIODO
    df_per = df.copy()
    df_per["Periodo"] = f"{date_from.date()} → {date_to.date()}"
    agg2 = {c: "sum" for c in sum_cols}
    agg2.update({c: "mean" for c in mean_cols})
    df_per = df_per.groupby("Periodo", as_index=False).agg(agg2)

    # ---------------------------------------------------------
    # Vista Traspuesta
    df_transp = build_transposed_view(df, sum_cols, mean_cols)

    return df, df_sem, df_per, df_transp


# ============================================================
# 📐 VISTA TRASPUESTA
# ============================================================

def build_transposed_view(df_diario, sum_cols, mean_cols, pct_cols=None):
    """
    Vista traspuesta con:
      - columnas por día (DD/MM/YYYY)
      - columna semanal al finalizar domingo
      - columna mensual al cambiar de mes en la serie (no requiere que esté el último día calendario)

    Reglas de agregación:
      - sum_cols: suma
      - mean_cols: promedio
      - ratios *_pct_pasajeros: se recalculan como 100 * sum(numerador) / sum(Q_pasajeros)
    """
    if df_diario is None or df_diario.empty:
        return pd.DataFrame()

    df = df_diario.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.normalize()
    df = df[df["fecha"].notna()].sort_values("fecha")

    kpis = [c for c in df.columns if c != "fecha"]

    # Ratios operativos por pasajeros
    operativos = ["OFF_TIME", "Duracion_90", "Duracion_30", "Abandonados", "Rescates"]
    if pct_cols is None:
        pct_cols = [f"{op}_pct_pasajeros" for op in operativos if f"{op}_pct_pasajeros" in df.columns]

    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }

    def week_label(start_date, end_date):
        return f"Semana {start_date.day:02d} al {end_date.day:02d} {meses[end_date.month]} {end_date.year}"

    def month_label(any_date):
        return f"Mes {meses[any_date.month]} {any_date.year}"

    def recompute_pct(subdf, op_name):
        denom = subdf.get("Q_pasajeros", pd.Series([0] * len(subdf), index=subdf.index)).sum()
        if denom == 0:
            return np.nan
        return (subdf[op_name].sum() / denom) * 100

    all_dates = sorted(df["fecha"].unique())
    all_dates = [pd.to_datetime(d).normalize() for d in all_dates]

    result = pd.DataFrame(index=kpis)

    for i, d in enumerate(all_dates):
        day_df = df[df["fecha"] == d]
        col_day = d.strftime("%d/%m/%Y")

        # Columna diaria
        row = day_df[kpis]
        result[col_day] = row.iloc[0] if len(row) > 0 else np.nan

        # Columna semanal: domingo
        if d.weekday() == 6:
            ws = d - pd.Timedelta(days=6)
            week_df = df[(df["fecha"] >= ws) & (df["fecha"] <= d)]

            label = week_label(ws, d)
            vals = []
            for k in kpis:
                if k in pct_cols:
                    op = k.replace("_pct_pasajeros", "")
                    vals.append(recompute_pct(week_df, op))
                elif k in sum_cols:
                    vals.append(week_df[k].sum())
                elif k in mean_cols:
                    vals.append(week_df[k].mean())
                else:
                    vals.append(np.nan)
            result[label] = vals

        # Columna mensual: cuando cambia el mes en la serie (o fin del rango)
        next_d = all_dates[i + 1] if i + 1 < len(all_dates) else None
        is_month_boundary = (next_d is None) or (next_d.month != d.month) or (next_d.year != d.year)

        if is_month_boundary:
            ms = d.replace(day=1)
            month_df = df[(df["fecha"] >= ms) & (df["fecha"] <= d)]

            label = month_label(d)
            vals = []
            for k in kpis:
                if k in pct_cols:
                    op = k.replace("_pct_pasajeros", "")
                    vals.append(recompute_pct(month_df, op))
                elif k in sum_cols:
                    vals.append(month_df[k].sum())
                elif k in mean_cols:
                    vals.append(month_df[k].mean())
                else:
                    vals.append(np.nan)
            result[label] = vals

    # Grupos de KPI (mantener orden)
    grupos = {
        "VENTAS (MONTO)": ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"],
        "VENTAS (VOLUMEN)": ["Q_journeys", "Q_pasajeros", "Q_pasajeros_exclusives", "Q_pasajeros_compartidas"],
        "PERFORMANCE": ["Q_Ticket", "Q_Tickets_WA", "Q_Tickets_Resueltos", "Reopen"],
        "CALIDAD (ENCUESTAS & SLA)": [
            "Q_Encuestas", "CSAT", "NPS Score",
            "Firt (h)", "firt_pct",
            "Furt (h)", "furt_pct",
            "Q_Auditorias", "Nota_Auditorias"
        ],
        "INSPECCIONES": [
            "Inspecciones_Q",
            "Cump_Exterior", "Incump_Exterior",
            "Cump_Interior", "Incump_Interior",
            "Cump_Conductor", "Incump_Conductor"
        ],
        "OTROS (OPERATIVOS)": [
            "OFF_TIME", "OFF_TIME_pct_pasajeros",
            "Duracion_90", "Duracion_90_pct_pasajeros",
            "Duracion_30", "Duracion_30_pct_pasajeros",
            "Abandonados", "Abandonados_pct_pasajeros",
            "Rescates", "Rescates_pct_pasajeros",
        ],
    }

    k_present = list(result.index)
    used = set()
    new_index = []

    for gr, lista in grupos.items():
        pres = [k for k in lista if k in k_present]
        if pres:
            new_index.append(f"=== {gr} ===")
            new_index.extend(pres)
            used.update(pres)

    restantes = [k for k in k_present if k not in used]
    if restantes:
        new_index.append("=== OTROS KPI ===")
        new_index.extend(restantes)

    result = result.reindex(new_index)
    result.insert(0, "KPI", result.index)

    return result.reset_index(drop=True)

