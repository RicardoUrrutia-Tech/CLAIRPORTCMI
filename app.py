import streamlit as st
import pandas as pd
from processor import procesar_global
from io import BytesIO

st.set_page_config(page_title="CMI Diario Consolidado", layout="wide")
st.title("🟣 Consolidado Diario – Aeropuerto CL")

# ----------------------------------------------------
# ARCHIVOS
# ----------------------------------------------------
ventas_file = st.file_uploader("Ventas (.xlsx)", type=["xlsx"])
perf_file   = st.file_uploader("Performance (.csv)", type=["csv"])
aud_file    = st.file_uploader("Auditorías (.csv)", type=["csv"])
off_file    = st.file_uploader("OffTime (.csv)", type=["csv"])
dur_file    = st.file_uploader("Duración >90 (.csv)", type=["csv"])

if not all([ventas_file, perf_file, aud_file, off_file, dur_file]):
    st.warning("⚠️ Carga todos los archivos para continuar.")
    st.stop()

# ----------------------------------------------------
# LIMPIEZA GENERAL
# ----------------------------------------------------
def clean_columns(df):
    df.columns = (
        df.columns
        .str.replace("\ufeff", "", regex=False)   # BOM UTF-8
        .str.replace("\u200b", "", regex=False)  # Zero width
        .str.replace("\xa0", " ", regex=False)   # NBSP
        .str.strip()
    )
    return df

# ----------------------------------------------------
# CARGA ROBUSTA
# ----------------------------------------------------
try:
    df_ventas = clean_columns(pd.read_excel(ventas_file))

    df_performance = clean_columns(pd.read_csv(
        perf_file, sep=",", encoding="latin-1", engine="python"
    ))
    df_auditorias = clean_columns(pd.read_csv(
        aud_file, sep=None, encoding="latin-1", engine="python"
    ))
    df_offtime = clean_columns(pd.read_csv(
        off_file, sep=",", encoding="latin-1", engine="python"
    ))
    df_duracion = clean_columns(pd.read_csv(
        dur_file, sep=",", encoding="latin-1", engine="python"
    ))

except Exception as e:
    st.error(f"❌ Error leyendo archivos: {e}")
    st.stop()

# ----------------------------------------------------
# FIXES ESPECÍFICOS BASADOS EN TU DEBUG
# ----------------------------------------------------
# 1) PERFORMANCE – % Firt
if "ï»¿% Firt" in df_performance.columns:
    df_performance.rename(columns={"ï»¿% Firt": "% Firt"}, inplace=True)

# 2) AUDITORÍAS – Date Time
if "ï»¿Date Time" in df_auditorias.columns:
    df_auditorias.rename(columns={"ï»¿Date Time": "Date Time"}, inplace=True)

# 3) OFFTIME – Week of tm_start_local_at
if "ï»¿Week of tm_start_local_at" in df_offtime.columns:
    df_offtime.rename(columns={"ï»¿Week of tm_start_local_at": "Week of tm_start_local_at"}, inplace=True)

# 4) DURACIÓN >90 – Week of Start At Local Dt
if "ï»¿Week of Start At Local Dt" in df_duracion.columns:
    df_duracion.rename(columns={"ï»¿Week of Start At Local Dt": "Week of Start At Local Dt"}, inplace=True)

# ----------------------------------------------------
# RANGO FECHAS
# ----------------------------------------------------
st.subheader("📅 Rango de fechas")
from_date = st.date_input("Desde")
to_date   = st.date_input("Hasta")

if from_date > to_date:
    st.error("❌ La fecha inicial no puede ser mayor a la fecha final.")
    st.stop()

# ----------------------------------------------------
# PROCESAR
# ----------------------------------------------------
if st.button("▶️ Generar Consolidado"):
    try:
        df_diario, df_semanal, df_total = procesar_global(
            df_ventas,
            df_performance,
            df_auditorias,
            df_offtime,
            df_duracion,
            from_date,
            to_date
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    st.subheader("📘 Diario")
    st.dataframe(df_diario, use_container_width=True)

    st.subheader("📗 Semanal")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("📙 Total")
    st.dataframe(df_total, use_container_width=True)

    def make_excel(d1, d2, d3):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        d1.to_excel(writer, index=False, sheet_name="Diario")
        d2.to_excel(writer, index=False, sheet_name="Semanal")
        d3.to_excel(writer, index=False, sheet_name="Total")
        writer.close()
        return output.getvalue()

    st.download_button(
        "📥 Descargar Excel",
        data=make_excel(df_diario, df_semanal, df_total),
        file_name="Consolidado_CMI.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


