import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from processor import procesar_global

st.set_page_config(page_title="CLAIRPORT – Consolidado Global", layout="wide")

st.title("📊 Consolidado Global Aeroportuario – CLAIRPORT")

# =====================================================
# 📥 LECTORES DE ARCHIVOS (ROBUSTOS)
# =====================================================

def read_generic_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)

    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")
    sep = ";" if text.count(";") > text.count(",") else ","

    return pd.read_csv(StringIO(text), sep=sep, engine="python")


def read_auditorias_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)

    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")
    return pd.read_csv(StringIO(text), sep=";", quotechar='"', engine="python")


# =====================================================
# 📥 CARGA DE ARCHIVOS
# =====================================================

st.header("📥 Cargar Archivos")

ventas_file = st.file_uploader("🔵 Ventas (.csv/.xlsx)", type=["csv", "xlsx"])
performance_file = st.file_uploader("🟢 Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("🟣 Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("🟠 Off-Time (.csv)", type=["csv"])
duracion_file = st.file_uploader("🔴 Duración >90 minutos (.csv)", type=["csv"])
t30_file = st.file_uploader("🟡 Espera >30 minutos en losa (.csv)", type=["csv"])

st.divider()


# =====================================================
# 📅 SELECTOR DE FECHAS
# =====================================================

st.header("📅 Seleccionar Rango de Fechas")

col1, col2 = st.columns(2)
with col1:
    date_from = st.date_input("📆 Desde:", value=None, format="YYYY-MM-DD")
with col2:
    date_to = st.date_input("📆 Hasta:", value=None, format="YYYY-MM-DD")

if not date_from or not date_to:
    st.warning("Selecciona ambas fechas para poder procesar.")
    st.stop()

date_from = pd.to_datetime(date_from)
date_to = pd.to_datetime(date_to)

st.divider()


# =====================================================
# 🚀 PROCESAR CONSOLIDADO
# =====================================================

if st.button("🚀 Procesar Consolidado", type="primary"):

    # Validación inicial
    if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file, t30_file]):
        st.error("⚠ Debes cargar TODOS los archivos.")
        st.stop()

    # ---------------------------- VENTAS ----------------------------
    try:
        if ventas_file.name.endswith(".csv"):
            df_ventas = read_generic_csv(ventas_file)
        else:
            df_ventas = pd.read_excel(ventas_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Ventas: {e}")
        st.stop()

    # ---------------------------- PERFORMANCE ----------------------------
    try:
        df_performance = read_generic_csv(performance_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Performance: {e}")
        st.stop()

    # ---------------------------- AUDITORÍAS ----------------------------
    try:
        df_auditorias = read_auditorias_csv(auditorias_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Auditorías: {e}")
        st.stop()

    # ---------------------------- OFF TIME ----------------------------
    try:
        df_offtime = read_generic_csv(offtime_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Off-Time: {e}")
        st.stop()

    # ---------------------------- DURACIÓN >90 ----------------------------
    try:
        df_duracion = read_generic_csv(duracion_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Duración >90: {e}")
        st.stop()

    # ---------------------------- >30 MIN LOSA ----------------------------
    try:
        df_30m = read_generic_csv(t30_file)
    except Exception as e:
        st.error(f"❌ Error leyendo >30 minutos en losa: {e}")
        st.stop()

    # =====================================================
    # PROCESAR TODO
    # =====================================================

    try:
        df_final, df_semanal, df_periodo = procesar_global(
            df_ventas,
            df_performance,
            df_auditorias,
            df_offtime,
            df_duracion,
            df_30m,
            date_from,
            date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    # =====================================================
    # MOSTRAR RESULTADOS
    # =====================================================

    st.success("✅ Procesado con éxito")

    st.subheader("📅 Diario Consolidado")
    st.dataframe(df_final, use_container_width=True)

    st.subheader("📆 Semanal Consolidado")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("📊 Consolidado del Periodo")
    st.dataframe(df_periodo, use_container_width=True)

    # =====================================================
    # DESCARGA EN EXCEL
    # =====================================================

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_final.to_excel(writer, index=False, sheet_name="Diario")
        df_semanal.to_excel(writer, index=False, sheet_name="Semanal")
        df_periodo.to_excel(writer, index=False, sheet_name="Periodo")

    st.download_button(
        "💾 Descargar Excel",
        data=output.getvalue(),
        file_name="Consolidado_Global.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube los archivos y presiona **Procesar Consolidado** para continuar.")


