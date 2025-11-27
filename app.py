import streamlit as st
import pandas as pd
from io import BytesIO
import os
import sys

# ==========================================================
#   FIX DEFINITIVO PARA ModuleNotFoundError
# ==========================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Limpiar sys.path de entradas inválidas
cleaned = []
for p in sys.path:
    if p and p != "app.py":   # descartar rutas vacías o inválidas
        cleaned.append(p)
sys.path = cleaned

# Asegurar que el directorio actual está en sys.path
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# Debug (puedes desactivarlo luego)
st.write("📂 Working Directory:", CURRENT_DIR)
st.write("🔍 Cleaned sys.path:", sys.path)

# Ahora sí importamos
from processor import procesar_global

# ==========================================================
#   CONFIG DE LA APP
# ==========================================================
st.set_page_config(page_title="Reporte Diario Consolidado", layout="wide")
st.title("🟦 Reporte Diario Consolidado – Aeropuerto Cabify")

st.markdown("""
Esta aplicación consolida los reportes de **Ventas**, **Performance** y **Auditorías**
para generar un **resumen diario general**, sin distinguir agentes.
""")

# ==========================================================
#   CARGA DE ARCHIVOS
# ==========================================================
st.header("📤 Cargar Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader(
        "Reporte de Ventas (.xlsx)",
        type=["xlsx"]
    )

with col2:
    performance_file = st.file_uploader(
        "Reporte de Performance (.csv)",
        type=["csv"]
    )

auditorias_file = st.file_uploader(
    "Reporte de Auditorías (.csv con separador ;) ",
    type=["csv"]
)

# ==========================================================
#   PROCESAR REPORTES
# ==========================================================
if st.button("🔄 Procesar Reportes"):

    if not ventas_file or not performance_file or not auditorias_file:
        st.error("❌ Debes cargar los 3 archivos para continuar.")
        st.stop()

    # --- Ventas ---
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error al cargar Ventas: {e}")
        st.stop()

    # --- Performance ---
    try:
        df_performance = pd.read_csv(
            performance_file,
            sep=",",
            encoding="utf-8",
            engine="python"
        )
    except Exception:
        try:
            df_performance = pd.read_csv(
                performance_file,
                sep=",",
                encoding="latin-1",
                engine="python"
            )
        except Exception as e:
            st.error(f"❌ Error al cargar Performance: {e}")
            st.stop()

    # --- Auditorías ---
    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(
            auditorias_file,
            sep=";",
            encoding="utf-8-sig",
            engine="python"
        )
    except Exception as e:
        st.error(f"❌ Error al cargar Auditorías: {e}")
        st.stop()

    # --------------------------------------------------------
    # PROCESAR
    # --------------------------------------------------------
    df_diario = procesar_global(df_ventas, df_performance, df_auditorias)

    st.success("✔ Reporte generado correctamente.")

    st.header("📅 Resumen Diario Consolidado")
    st.dataframe(df_diario, use_container_width=True)

    # --------------------------------------------------------
    # DESCARGA
    # --------------------------------------------------------
    st.header("📥 Descargar Excel Consolidado")

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Diario Consolidado")
        writer.close()
        return output.getvalue()

    st.download_button(
        label="⬇ Descargar Reporte Diario Consolidado",
        data=to_excel(df_diario),
        file_name="Reporte_Diario_Consolidado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube todos los archivos y presiona **Procesar Reportes** para continuar.")

