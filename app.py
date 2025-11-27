import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from processor import procesar_global

st.set_page_config(page_title="CLAIRPORT CMI – Consolidado Global", layout="wide")

st.title("📊 CLAIRPORT CMI – Consolidado Global")

st.write("Carga los 5 archivos requeridos para generar el reporte consolidado.")

# =====================================================
# FUNCIONES DE LECTURA
# =====================================================

def read_generic_csv(uploaded_file):
    """Lector seguro para CSV normales (coma)."""
    raw = uploaded_file.read()
    uploaded_file.seek(0)
    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")
    return pd.read_csv(StringIO(text), sep=",", engine="python")


def read_auditorias_csv(uploaded_file):
    """Lector especial para Auditorías (CSV con ';' y comillas)."""
    raw = uploaded_file.read()
    uploaded_file.seek(0)
    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")
    return pd.read_csv(StringIO(text), sep=";", quotechar='"', engine="python")


# =====================================================
# INPUT FILES
# =====================================================

ventas_file = st.file_uploader("📥 Cargar Ventas", type=["csv"])
perf_file = st.file_uploader("📥 Cargar Performance", type=["csv"])
aud_file = st.file_uploader("📥 Cargar Auditorías", type=["csv"])
off_file = st.file_uploader("📥 Cargar OffTime", type=["csv"])
dur_file = st.file_uploader("📥 Cargar Duración >90 min", type=["csv"])

st.markdown("---")

# Filtro de fechas
col1, col2 = st.columns(2)
with col1:
    date_from = st.date_input("📅 Fecha inicio")
with col2:
    date_to = st.date_input("📅 Fecha término")

st.markdown("---")

if st.button("🚀 Generar Reporte Consolidado"):
    if not all([ventas_file, perf_file, aud_file, off_file, dur_file]):
        st.error("⚠️ Debes cargar los 5 archivos.")
        st.stop()

    try:
        df_ventas = read_generic_csv(ventas_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Ventas: {e}")
        st.stop()

    try:
        df_perf = read_generic_csv(perf_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Performance: {e}")
        st.stop()

    try:
        df_aud = read_auditorias_csv(aud_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Auditorías: {e}")
        st.stop()

    try:
        df_off = read_generic_csv(off_file)
    except Exception as e:
        st.error(f"❌ Error leyendo OffTime: {e}")
        st.stop()

    try:
        df_dur = read_generic_csv(dur_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Duración >90 min: {e}")
        st.stop()

    # =====================================================
    # PROCESAR
    # =====================================================
    try:
        diario, semanal, total = procesar_global(
            df_ventas,
            df_perf,
            df_aud,
            df_off,
            df_dur,
            date_from,
            date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    st.success("✅ Reportes generados correctamente.")

    st.subheader("📅 Reporte Diario Consolidado")
    st.dataframe(diario, use_container_width=True)

    st.subheader("📆 Reporte Semanal (formato humano)")
    st.dataframe(semanal, use_container_width=True)

    st.subheader("📊 Resumen Total del Periodo")
    st.dataframe(total, use_container_width=True)

    # =====================================================
    # DESCARGA EN EXCEL
    # =====================================================
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        diario.to_excel(writer, index=False, sheet_name="Diario")
        semanal.to_excel(writer, index=False, sheet_name="Semanal")
        total.to_excel(writer, index=False, sheet_name="Total")

    st.download_button(
        label="📥 Descargar Excel Consolidado",
        data=output.getvalue(),
        file_name="CLAIRPORT_Consolidado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
