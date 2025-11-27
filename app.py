import streamlit as st
import pandas as pd
from io import BytesIO
import os, sys

# ==========================================================
# FIX IMPORT PATH
# ==========================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path = [p for p in sys.path if p and p != "app.py"]
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from processor import procesar_global

# ==========================================================
# CONFIG STREAMLIT
# ==========================================================
st.set_page_config(page_title="Reporte Diario Consolidado", layout="wide")
st.title("🟦 Reporte Diario Consolidado – Aeropuerto Cabify")

st.markdown("""
Esta aplicación consolida los reportes de **Ventas**, **Performance**, **Auditorías**
y **Reservas OFF TIME**, generando un informe diario del **periodo seleccionado**.
""")

# ==========================================================
# RANGO DE FECHAS
# ==========================================================
st.header("📅 Seleccione el período del análisis")

col1, col2 = st.columns(2)

with col1:
    date_from = st.date_input("Desde", value=pd.to_datetime("2025-11-01"))

with col2:
    date_to = st.date_input("Hasta", value=pd.to_datetime("2025-11-30"))

if date_to < date_from:
    st.error("❌ La fecha final debe ser mayor o igual a la fecha inicial.")
    st.stop()

# ==========================================================
# INPUT FILES
# ==========================================================
st.header("📤 Cargar Archivos")

ventas_file = st.file_uploader("Reporte de Ventas (.xlsx)", type=["xlsx"])
performance_file = st.file_uploader("Reporte de Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("Reporte de Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("Reporte OFF TIME (.csv)", type=["csv"])

# ==========================================================
# PROCESAR
# ==========================================================
if st.button("🔄 Procesar Reportes"):

    if not ventas_file or not performance_file or not auditorias_file or not offtime_file:
        st.error("❌ Debes cargar los 4 archivos para continuar.")
        st.stop()

    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
        df_performance = pd.read_csv(performance_file, sep=",", encoding="utf-8")
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(auditorias_file, sep=";", encoding="utf-8-sig")
        df_offtime = pd.read_csv(offtime_file, sep=",", encoding="utf-8-sig")
    except Exception as e:
        st.error(f"❌ Error de lectura: {e}")
        st.stop()

    # Procesamiento
    df_diario = procesar_global(
        df_ventas,
        df_performance,
        df_auditorias,
        df_offtime,
        date_from,
        date_to
    )

    st.success("✔ Consolidado generado correctamente.")
    st.subheader("📅 Resumen Diario Consolidado")
    st.dataframe(df_diario, use_container_width=True)

    # ==========================================================
    # DESCARGA
    # ==========================================================
    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Consolidado Diario")
        writer.close()
        return output.getvalue()

    st.download_button(
        label="⬇ Descargar Excel Consolidado",
        data=to_excel(df_diario),
        file_name="Consolidado_Diario_Aeropuerto.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube los 4 archivos y presiona **Procesar Reportes**.")

