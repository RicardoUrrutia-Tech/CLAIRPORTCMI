import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_global, resumen_periodo, resumen_semanal

st.set_page_config(page_title="CMI Global Airport", layout="wide")
st.title("🟦 Consolidado Diario / Semanal / Periodo – Aeropuerto")

st.markdown("Carga los 5 archivos requeridos para generar los reportes consolidados.")

# ===========================================================
# CARGA DE ARCHIVOS
# ===========================================================
ventas_file = st.file_uploader("📤 Reporte VENTAS (.xlsx)", type=["xlsx"])
perf_file = st.file_uploader("📤 Reporte PERFORMANCE (.csv)", type=["csv"])
aud_file = st.file_uploader("📤 Reporte AUDITORÍAS (.csv)", type=["csv"])
off_file = st.file_uploader("📤 Reporte OFF-TIME (.csv)", type=["csv"])
dur_file = st.file_uploader("📤 Reporte DURACIÓN+90 (.csv)", type=["csv"])

st.divider()

# ===========================================================
# FILTROS DE FECHAS
# ===========================================================
st.subheader("📅 Filtro de Fechas")
col1, col2 = st.columns(2)

date_from = col1.date_input("Fecha inicial", None)
date_to = col2.date_input("Fecha final", None)

st.divider()

# ===========================================================
# PROCESAR DATOS
# ===========================================================
if ventas_file and perf_file and aud_file and off_file and dur_file:

    try:
        df_ventas = pd.read_excel(ventas_file)
        df_perf = pd.read_csv(perf_file, sep=",", engine="python", encoding="latin-1")
        df_aud = pd.read_csv(aud_file, sep=",", engine="python", encoding="latin-1")
        df_off = pd.read_csv(off_file, sep=",", engine="python", encoding="latin-1")
        df_dur = pd.read_csv(dur_file, sep=",", engine="python", encoding="latin-1")

    except Exception as e:
        st.error(f"❌ Error leyendo archivos: {e}")
        st.stop()

    # =======================================================
    # PROCESAMIENTO GLOBAL
    # =======================================================
    try:
        df_final = procesar_global(
            df_ventas, df_perf, df_aud, df_off, df_dur,
            date_from, date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    st.success("✔ Datos procesados correctamente")
    st.divider()

    # =======================================================
    # DESCARGA A EXCEL
    # =======================================================
    def crear_excel(df):
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Reporte")
        buffer.seek(0)
        return buffer

    # ----------------------------------------
    st.header("📄 Reporte Diario Consolidado")
    st.dataframe(df_final, width="stretch")

    st.download_button(
        "📥 Descargar Excel Diario",
        crear_excel(df_final),
        "CMI_Diario.xlsx"
    )

    st.divider()

    # =======================================================
    # RESUMEN PERIODO
    # =======================================================
    st.header("📊 Resumen General del Periodo")

    df_resumen = resumen_periodo(df_final)
    st.dataframe(df_resumen, width="stretch")

    st.download_button(
        "📥 Descargar Resumen Periodo",
        crear_excel(df_resumen),
        "CMI_Resumen_Periodo.xlsx"
    )

    st.divider()

    # =======================================================
    # RESUMEN SEMANAL
    # =======================================================
    st.header("📆 Resumen Semanal (Formato Humano)")

    df_semanal = resumen_semanal(df_final)
    st.dataframe(df_semanal, width="stretch")

    st.download_button(
        "📥 Descargar Resumen Semanal",
        crear_excel(df_semanal),
        "CMI_Resumen_Semanal.xlsx"
    )


else:
    st.info("⚠️ Debes cargar los 5 archivos para continuar.")

