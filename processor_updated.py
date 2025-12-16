import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============================================================
# 🔧 LIMPIEZA DE COLUMNAS
# ============================================================

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    return df


def safe_pct(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Retorna 100 * numer/denom, con NaN cuando denom es 0 o NaN."""
    denom2 = denom.replace(0, np.nan)
    return (100.0 * numer / denom2)


# ============================================================
# 🟦 PROCESAR VENTAS
# ============================================================

def process_ventas(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPIs existentes:
      - Ventas_Totales (suma qt_price_local)
      - Ventas_Compartidas (ds_product_name == van_compartida)
      - Ventas_Exclusivas (ds_product_name == van_exclusive)

    Nuevos KPIs de volumen (solo FINISH_REASON_DROPOFF):
      - Q_journeys: count distinct journey_id (dropoff)
      - Q_pasajeros: count registros (dropoff)
      - Q_pasajeros_exclusives: count dropoff con van_exclusive
      - Q_pasajeros_compartidas: count dropoff con van_compartida
    """
    df = clean_cols(df)

    # Fecha base para agrupar por día
    if "tm_start_local_at" in df.columns:
        df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()
    elif "createdAt_local" in df.columns:
        df["fecha"] = pd.to_datetime(df["createdAt_local"], errors="coerce").dt.normalize()
    elif "date" in df.columns:
        # En algunos exports 'date' viene como DD-MM-YYYY o similar
        df["fecha"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.normalize()
    else:
        return pd.DataFrame(columns=[
            "fecha",
            "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas",
            "Q_journeys", "Q_pasajeros", "Q_pasajeros_exclusives", "Q_pasajeros_compartidas",
        ])

    # Monto / precio
    if "qt_price_local" in df.columns:
        df["qt_price_local"] = (
            df["qt_price_local"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace("$", "", regex=False)
        )
        df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")
    else:
        df["qt_price_local"] = np.nan

    # Ventas (monto)
    prod = df.get("ds_product_name", pd.Series([""] * len(df), index=df.index)).astype(str).str.lower().str.strip()

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = np.where(prod == "van_compartida", df["qt_price_local"], 0)
    df["Ventas_Exclusivas"] = np.where(prod == "van_exclusive", df["qt_price_local"], 0)

    # Dropoff filter (finishReason)
    fr_col = None
    for c in ["finishReason", "finisReason", "FinishReason", "finish_reason", "Finish Reason"]:
        if c in df.columns:
            fr_col = c
            break

    if fr_col is None:
        is_dropoff = pd.Series([False] * len(df), index=df.index)
    else:
        is_dropoff = df[fr_col].astype(str).str.strip().str.upper().eq("FINISH_REASON_DROPOFF")

    # Volumen de pasajeros
    df["Q_pasajeros"] = is_dropoff.astype(int)
    df["Q_pasajeros_exclusives"] = np.where(is_dropoff & (prod == "van_exclusive"), 1, 0)
    df["Q_pasajeros_compartidas"] = np.where(is_dropoff & (prod == "van_compartida"), 1, 0)

    # Journeys (unique journey_id para dropoff)
    if "journey_id" in df.columns:
        jid = df["journey_id"].astype(str).str.strip()
        df["_jid"] = jid
    else:
        df["_jid"] = ""

    # Agregación diaria base
    diario = df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
        "Q_pasajeros": "sum",
        "Q_pasajeros_exclusives": "sum",
        "Q_pasajeros_compartidas": "sum",
    })

    # Q_journeys (nunique)
    if "journey_id" in df.columns:
        qj = (
            df[is_dropoff & df["_jid"].ne("") & df["_jid"].notna()]
            .groupby("fecha")["_jid"]
            .nunique()
            .reset_index()
            .rename(columns={"_jid": "Q_journeys"})
        )
        diario = diario.merge(qj, on="fecha", how="left")
    else:
        diario["Q_journeys"] = 0

    diario["Q_journeys"] = diario["Q_journeys"].fillna(0)

    return diario


# ============================================================
# 🟩 PROCESAR PERFORMANCE
# ============================================================

def process_performance(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)

    df = df.rename(columns={"% Firt": "firt_pct", "% Furt": "furt_pct"})

    # Fecha de Referencia (MM/DD/YYYY)
    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.normalize()

    df["Q_Ticket"] = 1

    # Resueltos = todo menos pending (criterio global actual)
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
# 🟪 PROCESAR AUDITORÍAS
# ============================================================

def process_auditorias(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)

    candidates = ["Date Time Reference", "Date Time", "ï»¿Date Time"]
    col_fecha = next((c for c in candidates if c in df.columns), None)

    if col_fecha is None:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    def to_date_aud(x):
        if pd.isna(x):
            return None

        if isinstance(x, (int, float)):
            try:
                if x > 30000:
                    return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
            except:
                pass

        s = str(x).strip()

        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except:
                pass

        try:
            return pd.to_datetime(s, dayfirst=True).date()
        except:
            return None

    df["fecha"] = df[col_fecha].apply(to_date_aud)
    df = df[df["fecha"].notna()]
    df["fecha"] = pd.to_datetime(df["fecha"])

    if "Total Audit Score" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    score_raw = (
        df["Total Audit Score"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )

    df["Nota_Auditorias"] = pd.to_numeric(score_raw, errors="coerce").fillna(0)
    df["Q_Auditorias"] = 1

    diario = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })

    return diario


# ============================================================
# 🟧 OTROS PROCESADORES
# ============================================================

def process_offtime(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()
    df["OFF_TIME"] = np.where(
        df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)",
        1, 0
    )
    return df.groupby("fecha", as_index=False).agg({"OFF_TIME": "sum"})


def process_duracion(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce").dt.normalize()
    df["Duracion_90"] = np.where(pd.to_numeric(df["Duration (Minutes)"], errors="coerce") > 90, 1, 0)
    return df.groupby("fecha", as_index=False).agg({"Duracion_90": "sum"})


def process_duracion30(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Day of tm_start_local_at"], errors="coerce").dt.normalize()
    df["Duracion_30"] = 1
    return df.groupby("fecha", as_index=False).agg({"Duracion_30": "sum"})


def process_inspecciones(df: pd.DataFrame) -> pd.DataFrame:
    # (se mantiene tu versión actual)
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


def process_abandonados(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Marca temporal"], errors="coerce").dt.normalize()
    df["Abandonados"] = 1
    return df.groupby("fecha", as_index=False).agg({"Abandonados": "sum"})


def process_rescates(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    if "Start At Local Dttm" not in df.columns or "User Email" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Rescates"])

    df["User Email"] = df["User Email"].astype(str).str.lower().str.strip()
    df = df[df["User Email"] == "emergencias.excellence.cl@cabify.com"]

    df["fecha"] = pd.to_datetime(df["Start At Local Dttm"], errors="coerce").dt.normalize()
    df["Rescates"] = 1
    return df.groupby("fecha", as_index=False).agg({"Rescates": "sum"})


def process_whatsapp(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    if "Created At Local Dt" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Q_Tickets_WA"])
    df["fecha"] = pd.to_datetime(df["Created At Local Dt"], errors="coerce").dt.normalize()
    df["Q_Tickets_WA"] = 1
    return df.groupby("fecha", as_index=False).agg({"Q_Tickets_WA": "sum"})


# ============================================================
# 📅 SEMANA HUMANA
# ============================================================

def semana_humana(fecha: pd.Timestamp) -> str:
    lunes = fecha - pd.Timedelta(days=fecha.weekday())
    domingo = lunes + pd.Timedelta(days=6)
    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
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

    # --- columnas base
    sum_cols = [
        # performance / calidad
        "Q_Encuestas", "Reopen", "Q_Ticket", "Q_Tickets_Resueltos",
        "Q_Tickets_WA",
        "Q_Auditorias",
        # ventas $
        "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas",
        # ventas volumen
        "Q_journeys", "Q_pasajeros", "Q_pasajeros_exclusives", "Q_pasajeros_compartidas",
        # otros operativos
        "OFF_TIME", "Duracion_90", "Duracion_30", "Abandonados", "Rescates",
        # inspecciones
        "Inspecciones_Q",
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

    # Promedios: NO convertir Nota_Auditorias 0 en NaN
    for c in mean_cols:
        if c in df.columns and c != "Nota_Auditorias":
            df[c] = df[c].replace({0: np.nan})

    # ---------------------------------------------------------
    # % Operativos respecto a pasajeros (NUEVO)
    # ---------------------------------------------------------
    operativos = ["OFF_TIME", "Duracion_90", "Duracion_30", "Abandonados", "Rescates"]
    pct_cols = []
    for op in operativos:
        colp = f"{op}_pct_pasajeros"
        df[colp] = safe_pct(df[op], df["Q_pasajeros"]).round(4)
        pct_cols.append(colp)

    # ---------------------------------------------------------
    # SEMANAL
    # ---------------------------------------------------------
    df_sem = df.copy()
    df_sem["Semana"] = df_sem["fecha"].apply(semana_humana)

    agg = {c: "sum" for c in sum_cols}
    agg.update({c: "mean" for c in mean_cols})
    df_sem = df_sem.groupby("Semana", as_index=False).agg(agg)

    # recalcular % operativos en semanal como (sum op / sum pasajeros)
    for op in operativos:
        colp = f"{op}_pct_pasajeros"
        df_sem[colp] = safe_pct(df_sem[op], df_sem["Q_pasajeros"]).round(4)

    # ---------------------------------------------------------
    # PERIODO
    # ---------------------------------------------------------
    df_per = df.copy()
    df_per["Periodo"] = f"{date_from.date()} → {date_to.date()}"

    agg2 = {c: "sum" for c in sum_cols}
    agg2.update({c: "mean" for c in mean_cols})
    df_per = df_per.groupby("Periodo", as_index=False).agg(agg2)

    for op in operativos:
        colp = f"{op}_pct_pasajeros"
        df_per[colp] = safe_pct(df_per[op], df_per["Q_pasajeros"]).round(4)

    # ---------------------------------------------------------
    # Vista Traspuesta
    # ---------------------------------------------------------
    df_transp = build_transposed_view(df, sum_cols=sum_cols, mean_cols=mean_cols, pct_cols=pct_cols)

    return df, df_sem, df_per, df_transp


# ============================================================
# 📐 VISTA TRASPUESTA
# ============================================================

def build_transposed_view(df_diario: pd.DataFrame, sum_cols, mean_cols, pct_cols):
    """
    Matriz KPI x día, con columna de resumen semanal después de cada domingo.

    IMPORTANTE: para los ratios *_pct_pasajeros, el resumen semanal se calcula como:
      100 * sum(numerador) / sum(Q_pasajeros)
    y NO como promedio simple de los % diarios.
    """
    if df_diario is None or df_diario.empty:
        return pd.DataFrame()

    df = df_diario.copy()
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.sort_values("fecha")

    kpis = [c for c in df.columns if c != "fecha"]

    df["week_start"] = df["fecha"] - pd.to_timedelta(df["fecha"].dt.weekday, unit="D")
    weeks = sorted(df["week_start"].unique())

    result = pd.DataFrame(index=kpis)

    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }

    for ws in weeks:
        week_df = df[df["week_start"] == ws]
        fechas = sorted(week_df["fecha"].unique())

        # Días
        for f in fechas:
            col = f.strftime("%d/%m/%Y")
            row = week_df[week_df["fecha"] == f][kpis]
            result[col] = row.iloc[0] if len(row) > 0 else np.nan

        # Semana (resumen)
        start = pd.to_datetime(ws)
        end = max(fechas)
        label = f"Semana {start.day:02d} al {end.day:02d} {meses[end.month]} {end.year}"

        vals = []
        for k in kpis:
            if k in pct_cols and k.endswith("_pct_pasajeros"):
                numer = k.replace("_pct_pasajeros", "")
                if (numer in week_df.columns) and ("Q_pasajeros" in week_df.columns):
                    denom_sum = week_df["Q_pasajeros"].sum()
                    vals.append((100.0 * week_df[numer].sum() / denom_sum) if denom_sum > 0 else np.nan)
                else:
                    vals.append(np.nan)
            elif k in sum_cols:
                vals.append(week_df[k].sum())
            elif k in mean_cols:
                vals.append(week_df[k].mean())
            else:
                vals.append(np.nan)

        result[label] = vals

    # Grupos de KPI (incluye nuevos KPIs y ratios)
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
