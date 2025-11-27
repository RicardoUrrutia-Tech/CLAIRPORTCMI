import streamlit as st
import pandas as pd
from processor import procesar_global
from io import BytesIO

st.set_page_config(page_title="CMI Diario Consolidado", layout="wide")
st.title("🟣 Consolidado Diario – Aeropuerto CL")

st.write("Carga los 5 reportes y selecciona un rango de fechas para consolidar resultados.")

# ======================================================
# CARGA DE ARCHIVOS
# ======================================================
ventas_file = st.file_uploader("📄 Cargar Reporte de Ventas (.xlsx)", type=["xlsx"])
perf_file = st.file_uploader("📄 Cargar Reporte de Performance (.csv)", type=["csv"])
aud_file = st.file_uploader("📄 Cargar Reporte de Auditorías (.csv)", type=["csv"])
off_file = st.file_uploader("📄 Cargar Reporte de OffTime (.csv)", type=["csv"])
dur_file = st.file_uploader("📄 Cargar Reporte de Duración >90 (.csv)", type=["csv"])

if not all([ventas_file, perf_file, aud_file, off_file, dur_file]):
    st.warning("⚠️ Debes cargar **todos los archivos** para continuar.")
    st.stop()

# ======================================================
# LECTURA DE ARCHIVOS (ROBUSTA)
# ======================================================
try:
    # VENTAS → SIEMPRE EXCEL
    df_ventas = pd.read_excel(ventas_file)

    # PERFORMANCE → CSV SEPARADO POR COMAS
    df_performance = pd.read_csv(
        perf_file,
        sep=",",
        engine="python",
        encoding="latin-1"
    )

    # AUDITORÍAS → CSV PERO SEPARADOR DESCONOCIDO → AUTO-DETECTAR
    df_auditorias = pd.read_csv(
        aud_file,
        sep=None,           # detecta automáticamente coma, punto y coma o tab
        engine="python",
        encoding="latin-1"
    )

    # OFFTIME → CSV CON COMAS
    df_offtime = pd.read_csv(
        off_file,
        sep=",",
        engine="python",
        encoding="latin-1"
    )

    # DURACIÓN >90 → CSV CON COMAS
    df_duracion = pd.read_csv(
        dur_file,
        sep=",",
        engine="python",
        encoding="latin-1"
    )

except Exception as e:
    st.error(f"❌ Error leyendo archivos: {e}")
    st.stop()

# ======================================================
# FILTRO DE FECHAS
# ======================================================
st.subheader("📅 Seleccione rango de fechas")

c1, c2 = st.columns(2)
date_from = c1.date_input("Desde")
date_to = c2.date_input("Hasta")

if date_from > date_to:
    st.error("❌ La fecha inicial no puede ser mayor a la fecha final.")
    st.stop()

# ======================================================
# PROCESAR
# ======================================================
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

    # ======================================================
    # MOSTRAR RESULTADOS
    # ======================================================
    st.subheader("📘 Resultado Diario")
    st.dataframe(df_diario, use_container_width=True)

    st.subheader("📗 Resumen Semanal")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("📙 Resumen Total del Periodo")
    st.dataframe(df_total, use_container_width=True)

    # ======================================================
    # DESCARGAR EXCEL COMPLETO
    # ======================================================
    def export_excel(df1, df2, df3):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")

        df1.to_excel(writer, index=False, sheet_name="Diario")
        df2.to_excel(writer, index=False, sheet_name="Semanal")
        df3.to_excel(writer, index=False, sheet_name="Total")

        writer.close()
        return output.getvalue()

    excel_bytes = export_excel(df_diario, df_semanal, df_total)

    st.download_button(
        label="📥 Descargar Consolidado en Excel",
        data=excel_bytes,
        file_name="Consolidado_CMI.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


