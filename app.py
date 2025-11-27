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
y **Reservas OFF TIME**, generando un **informe diario**.
""")

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
        st.error("❌ Debes cargar los 4 archivos primero.")
        st.stop()

    # --- Ventas ---
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error en Ventas: {e}")
        st.stop()

    # --- Performance ---
    try:
        df_performance = pd.read_csv(performance_file, sep=",", encoding="utf-8")
    except:
        df_performance = pd.read_csv(performance_file, sep=",", encoding="latin-1")

    # --- Auditorías ---
    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(auditorias_file, sep=";", encoding="utf-8-sig")
    except Exception as e:
        st.error(f"❌ Error en Auditorías: {e}")
        st.stop()

    # --- OFF TIME ---
    try:
        df_offtime = pd.read_csv(offtime_file, sep=",", encoding="utf-8-sig")
    except Exception as e:
        st.error(f"❌ Error en OFF TIME: {e}")
        st.stop()

    # ==========================================================
    # DEBUG: VER LO QUE LEE STREAMLIT
    # ==========================================================
    st.subheader("🧪 DEBUG – Vista previa de datos cargados")

    st.write("VENTAS HEAD:", df_ventas.head())
    st.write("PERFORMANCE HEAD:", df_performance.head())
    st.write("AUDITORIAS HEAD:", df_auditorias.head())
    st.write("OFF TIME HEAD:", df_offtime.head())

    # ==========================================================
    # PROCESAR CONSOLIDADO
    # ==========================================================
    df_diario = procesar_global(df_ventas, df_performance, df_auditorias, df_offtime)

    st.success("✔ Consolidados generados correctamente.")
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
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

else:
    st.info("Sube los 4 archivos y presiona **Procesar Reportes**.")
