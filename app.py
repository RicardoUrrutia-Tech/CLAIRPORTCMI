import streamlit as st
import pandas as pd
from io import BytesIO
import os, sys

# Fix import path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path = [p for p in sys.path if p and p != "app.py"]
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from processor import procesar_global

# Streamlit UI
st.set_page_config(page_title="Reporte Diario Consolidado", layout="wide")
st.title("🟦 Reporte Diario Consolidado – Aeropuerto Cabify")

st.markdown("""
Carga los 5 reportes obligatorios y elige el rango de fechas.
""")

# Rango de fechas
st.header("📅 Período del análisis")
col1, col2 = st.columns(2)

date_from = col1.date_input("Desde", value=pd.to_datetime("2025-11-01"))
date_to   = col2.date_input("Hasta", value=pd.to_datetime("2025-11-30"))

if date_to < date_from:
    st.error("❌ La fecha final debe ser mayor o igual a la inicial.")
    st.stop()

# Archivos
st.header("📤 Cargar archivos")

ventas_file = st.file_uploader("Ventas (.xlsx)", type=["xlsx"])
performance_file = st.file_uploader("Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("OFF TIME (.csv)", type=["csv"])
duracion_file = st.file_uploader("Duración DO >90 (.csv)", type=["csv"])

# Procesar
if st.button("🔄 Procesar Reportes"):

    if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file]):
        st.error("❌ Debes cargar los 5 archivos")
        st.stop()

    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
        df_perf   = pd.read_csv(performance_file, sep=",", encoding="utf-8")
        df_aud    = pd.read_csv(auditorias_file, sep=";", encoding="utf-8-sig")
        df_off    = pd.read_csv(offtime_file, sep=",", encoding="utf-8-sig")
        df_dur    = pd.read_csv(duracion_file, sep=",", encoding="utf-8-sig")
    except Exception as e:
        st.error(f"Error al leer archivos: {e}")
        st.stop()

    df_final = procesar_global(
        df_ventas, df_perf, df_aud, df_off, df_dur,
        date_from, date_to
    )

    st.success("✔ Consolidado generado.")
    st.dataframe(df_final, use_container_width=True)

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Consolidado Diario")
        writer.close()
        return output.getvalue()

    st.download_button(
        "⬇ Descargar Excel",
        data=to_excel(df_final),
        file_name="Consolidado_Diario_Aeropuerto.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


