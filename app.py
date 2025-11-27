import streamlit as st
import pandas as pd
from io import StringIO
from processor import procesar_global

st.set_page_config(page_title="CLAIRPORT – Consolidado Global", layout="wide")

st.title("📊 Consolidado Global Aeroportuario – CLAIRPORT")


# =====================================================
# LECTORES DE ARCHIVOS
# =====================================================

def read_generic_csv(uploaded_file):
    """
    Lector universal para:
      - Ventas
      - Performance
      - OffTime
      - Duración >90
    Detecta automáticamente si el separador es ; o ,
    Maneja BOM y latin-1
    """
    raw = uploaded_file.read()
    uploaded_file.seek(0)

    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")

    sep = ";" if text.count(";") > text.count(",") else ","

    return pd.read_csv(StringIO(text), sep=sep, engine="python")


def read_auditorias_csv(uploaded_file):
    """
    Auditorías requiere un lector especial:
      - Separador fijo ;
      - quotechar y engine="python" por columnas con comillas
    """
    raw = uploaded_file.read()
    uploaded_file.seek(0)

    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")

    return pd.read_csv(StringIO(text), sep=";", quotechar='"', engine="python")


# =====================================================
# SUBIDA DE ARCHIVOS
# =====================================================

st.header("📥 Cargar Archivos")

ventas_file = st.file_uploader("🔵 Cargar reporte de VENTAS (.csv o .xlsx)", type=["csv", "xlsx"])
performance_file = st.file_uploader("🟢 Cargar reporte de PERFORMANCE (.csv)", type=["csv"])
auditorias_file = st.file_uploader("🟣 Cargar reporte de AUDITORÍAS (.csv)", type=["csv"])
offtime_file = st.file_uploader("🟠 Cargar reporte de OFF-TIME (.csv)", type=["csv"])
duracion_file = st.file_uploader("🔴 Cargar reporte DURACIÓN >90 MINUTOS (.csv)", type=["csv"])

st.divider()

# =====================================================
# PROCESAMIENTO
# =====================================================

if st.button("🚀 Procesar Consolidado", type="primary"):

    if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file]):
        st.error("⚠ Debes cargar TODOS los archivos para continuar.")
        st.stop()

    # -------------------------------------------------
    # LECTURA DE ARCHIVOS
    # -------------------------------------------------
    try:
        if ventas_file.name.endswith(".csv"):
            df_ventas = read_generic_csv(ventas_file)
        else:
            df_ventas = pd.read_excel(ventas_file)

    except Exception as e:
        st.error(f"❌ Error leyendo Ventas: {e}")
        st.stop()

    try:
        df_performance = read_generic_csv(performance_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Performance: {e}")
        st.stop()

    try:
        df_auditorias = read_auditorias_csv(auditorias_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Auditorías: {e}")
        st.stop()

    try:
        df_offtime = read_generic_csv(offtime_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Off-Time: {e}")
        st.stop()

    try:
        df_duracion = read_generic_csv(duracion_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Duración >90: {e}")
        st.stop()

    # -------------------------------------------------
    # PROCESAMIENTO FINAL
    # -------------------------------------------------
    try:
        df_final, df_semanal, df_mensual = procesar_global(
            df_ventas,
            df_performance,
            df_auditorias,
            df_offtime,
            df_duracion
        )

    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    # =================================================
    # MOSTRAR RESULTADOS
    # =================================================
    st.success("✅ Datos procesados correctamente")

    st.subheader("📅 Reporte Diario Consolidado")
    st.dataframe(df_final, use_container_width=True)

    st.subheader("📆 Reporte Semanal Consolidado")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("🗓 Reporte Total del Periodo")
    st.dataframe(df_mensual, use_container_width=True)

    # =================================================
    # DESCARGA EN EXCEL
    # =================================================
    st.subheader("⬇ Descargar Consolidado en Excel")

    import io
    import xlsxwriter

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_final.to_excel(writer, index=False, sheet_name="Diario")
        df_semanal.to_excel(writer, index=False, sheet_name="Semanal")
        df_mensual.to_excel(writer, index=False, sheet_name="Total Periodo")

    st.download_button(
        label="💾 Descargar Excel Consolidado",
        data=output.getvalue(),
        file_name="Consolidado_Global.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

