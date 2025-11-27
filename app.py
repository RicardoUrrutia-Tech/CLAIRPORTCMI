import streamlit as st
import pandas as pd
from io import BytesIO
import os
import sys

# -------------------------------------------------------------------
# Streamlit settings
# -------------------------------------------------------------------
st.set_page_config(page_title="CMI - Reporte Diario Consolidado", layout="wide")

st.title("🟦 Reporte Diario Consolidado – Aeropuerto Cabify")


# -------------------------------------------------------------------
# Fix import paths
# -------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path = [p for p in sys.path if p != "app.py"]
sys.path.insert(0, CURRENT_DIR)

from processor import procesar_global


# -------------------------------------------------------------------
# Rango de fechas
# -------------------------------------------------------------------
st.header("📅 Selección de período")

col1, col2 = st.columns(2)
date_from = col1.date_input("Desde")
date_to = col2.date_input("Hasta")

if date_to < date_from:
    st.error("❌ La fecha final debe ser mayor o igual que la inicial.")
    st.stop()


# -------------------------------------------------------------------
# Archivos obligatorios
# -------------------------------------------------------------------
st.header("📤 Cargar archivos obligatorios")

ventas_file = st.file_uploader("📁 Reporte de Ventas (.xlsx)", type=["xlsx"])
perf_file = st.file_uploader("📁 Reporte de Performance (.csv)", type=["csv"])
aud_file = st.file_uploader("📁 Reporte de Auditorías (.csv)", type=["csv"])
off_file = st.file_uploader("📁 Reporte de OFF TIME (.csv)", type=["csv"])
dur_file = st.file_uploader("📁 Reporte Duración >90 Minutos (.csv)", type=["csv"])

all_files = [ventas_file, perf_file, aud_file, off_file, dur_file]


# -------------------------------------------------------------------
# Procesar
# -------------------------------------------------------------------
if st.button("🚀 Procesar Reporte"):

    # Check files
    if not all(all_files):
        st.error("❌ Debes subir los 5 archivos obligatorios para continuar.")
        st.stop()

    # --------------------------------------------------------------
    # Load each dataset safely
    # --------------------------------------------------------------
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error al leer archivo de Ventas: {e}")
        st.stop()

    try:
        df_perf = pd.read_csv(perf_file, sep=",", encoding="latin-1", engine="python")
    except Exception as e:
        st.error(f"❌ Error al leer Performance: {e}")
        st.stop()

    try:
        df_aud = pd.read_csv(aud_file, sep=";", encoding="utf-8-sig", engine="python")
    except Exception as e:
        st.error(f"❌ Error al leer Auditorías: {e}")
        st.stop()

    try:
        df_off = pd.read_csv(off_file, sep=",", encoding="utf-8-sig", engine="python")
    except Exception as e:
        st.error(f"❌ Error al leer OFF TIME: {e}")
        st.stop()

    try:
        df_dur = pd.read_csv(dur_file, sep=",", encoding="utf-8-sig", engine="python")
    except Exception as e:
        st.error(f"❌ Error al leer Duración >90: {e}")
        st.stop()

    # --------------------------------------------------------------
    # Process with global processor
    # --------------------------------------------------------------
    try:
        df_final = procesar_global(
            df_ventas, df_perf, df_aud, df_off, df_dur,
            date_from, date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    # --------------------------------------------------------------
    # Display results
    # --------------------------------------------------------------
    st.success("✔ Reporte consolidado generado exitosamente.")
    st.dataframe(df_final, use_container_width=True)


    # ----------------------------------------------------------------
    # Download Excel
    # ----------------------------------------------------------------
    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Reporte Diario Consolidado")
        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_final)

    st.download_button(
        label="📥 Descargar Excel Consolidado",
        data=excel_bytes,
        file_name="Reporte_Diario_Consolidado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )



