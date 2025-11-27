import streamlit as st
import pandas as pd
from processor import procesar_global
from io import BytesIO

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
st.set_page_config(page_title="CMI Diario Consolidado", layout="wide")
st.title("🟣 Consolidado Diario – Aeropuerto CL")

st.write("Carga los reportes y selecciona un rango de fechas para generar el informe consolidado.")

# ----------------------------------------------------
# LOAD FILES
# ----------------------------------------------------
ventas_file = st.file_uploader("📄 Cargar Reporte de Ventas (.xlsx)", type=["xlsx"])
perf_file = st.file_uploader("📄 Cargar Reporte de Performance (.csv)", type=["csv"])
aud_file = st.file_uploader("📄 Cargar Reporte de Auditorías (.csv)", type=["csv"])
off_file = st.file_uploader("📄 Cargar Reporte de OffTime (.csv)", type=["csv"])
dur_file = st.file_uploader("📄 Cargar Reporte de Duración >90 (.csv)", type=["csv"])

if not all([ventas_file, perf_file, aud_file, off_file, dur_file]):
    st.warning("⚠️ Debes cargar **todos los archivos** para continuar.")
    st.stop()

# ----------------------------------------------------
# SAFE COLUMN CLEANER
# ----------------------------------------------------
def clean_columns(df):
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    return df

# ----------------------------------------------------
# READ FILES (ROBUST)
# ----------------------------------------------------
try:
    # VENTAS
    df_ventas = pd.read_excel(ventas_file)

    # PERFORMANCE
    df_performance = pd.read_csv(
        perf_file,
        sep=",",
        encoding="latin-1",
        engine="python"
    )
    df_performance = clean_columns(df_performance)

    # AUDITORÍAS (autodetectar separador)
    df_auditorias = pd.read_csv(
        aud_file,
        sep=None,
        encoding="latin-1",
        engine="python"
    )
    df_auditorias = clean_columns(df_auditorias)

    # OFFTIME
    df_offtime = pd.read_csv(
        off_file,
        sep=",",
        encoding="latin-1",
        engine="python"
    )
    df_offtime = clean_columns(df_offtime)

    # DURACIÓN >90
    df_duracion = pd.read_csv(
        dur_file,
        sep=",",
        encoding="latin-1",
        engine="python"
    )
    df_duracion = clean_columns(df_duracion)

except Exception as e:
    st.error(f"❌ Error leyendo archivos: {e}")
    st.stop()

# ----------------------------------------------------
# DATE RANGE
# ----------------------------------------------------
st.subheader("📅 Seleccione rango de fechas")

c1, c2 = st.columns(2)
date_from = c1.date_input("Desde")
date_to = c2.date_input("Hasta")

if date_from > date_to:
    st.error("❌ La fecha inicial no puede ser mayor a la fecha final.")
    st.stop()

# ----------------------------------------------------
# PROCESS
# ----------------------------------------------------
if st.button("▶️ Generar Consolidado"):
    try:
        df_diario, df_semanal, df_total = procesar_global(
            df_ventas,
            df_performance,
            df_auditorias,
            df_offtime,
            df_duracion,
            date_from,
            date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    # -------------------------------
    # SHOW RESULTS
    # -------------------------------
    st.subheader("📘 Resultado Diario")
    st.dataframe(df_diario, use_container_width=True)

    st.subheader("📗 Resumen Semanal")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("📙 Resumen Total del Periodo")
    st.dataframe(df_total, use_container_width=True)

    # -------------------------------
    # EXPORT TO EXCEL
    # -------------------------------
    def to_excel(df1, df2, df3):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")

        df1.to_excel(writer, index=False, sheet_name="Diario")
        df2.to_excel(writer, index=False, sheet_name="Semanal")
        df3.to_excel(writer, index=False, sheet_name="Total")

        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_diario, df_semanal, df_total)

    st.download_button(
        label="📥 Descargar Consolidado (Excel)",
        data=excel_bytes,
        file_name="Consolidado_CMI.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

