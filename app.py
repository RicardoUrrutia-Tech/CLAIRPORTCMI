import streamlit as st
import pandas as pd
from processor import procesar_global

st.set_page_config(page_title="CMI Aeropuerto – Consolidado Diario", layout="wide")
st.title("🟦 CMI Aeropuerto – Consolidado Diario Global")

st.markdown("Esta aplicación consolida Ventas, Performance, Auditorías, Reservas Off-Time y Viajes >90min.")

# =====================================================
# CARGA DE ARCHIVOS
# =====================================================
st.header("📂 Cargar Archivos")

ventas_file = st.file_uploader("1) Cargar Ventas (.xlsx)", type=["xlsx"])
performance_file = st.file_uploader("2) Cargar Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("3) Cargar Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("4) Cargar Reservas Off-Time (.csv)", type=["csv"])
duracion_file = st.file_uploader("5) Cargar Viajes >90 Minutos (.csv)", type=["csv"])

if not (ventas_file and performance_file and auditorias_file and offtime_file and duracion_file):
    st.info("👆 Sube los 5 archivos para continuar.")
    st.stop()

# =====================================================
# RANGO DE FECHAS
# =====================================================
st.header("📅 Seleccionar Rango de Fechas")

date_from = st.date_input("Fecha Desde")
date_to = st.date_input("Fecha Hasta")

if not (date_from and date_to):
    st.warning("Seleccione un rango de fechas válido.")
    st.stop()

# Convertir fechas
date_from = pd.to_datetime(date_from)
date_to = pd.to_datetime(date_to)

if date_from > date_to:
    st.error("La fecha inicial no puede ser mayor que la fecha final.")
    st.stop()

# =====================================================
# LECTURA DE ARCHIVOS
# =====================================================
st.header("⚙️ Procesamiento de Datos")

try:
    df_ventas = pd.read_excel(ventas_file)
except Exception as e:
    st.error(f"❌ Error al leer Ventas: {e}")
    st.stop()

try:
    df_performance = pd.read_csv(
        performance_file, sep=",", encoding="latin-1", engine="python"
    )
except Exception as e:
    st.error(f"❌ Error al leer Performance: {e}")
    st.stop()

try:
    df_auditorias = pd.read_csv(
        auditorias_file, sep=",", encoding="latin-1", engine="python"
    )
except Exception as e:
    st.error(f"❌ Error al leer Auditorías: {e}")
    st.stop()

try:
    df_offtime = pd.read_csv(
        offtime_file, sep=",", encoding="latin-1", engine="python"
    )
except Exception as e:
    st.error(f"❌ Error al leer Off-Time: {e}")
    st.stop()

try:
    df_duracion = pd.read_csv(
        duracion_file, sep=",", encoding="latin-1", engine="python"
    )
except Exception as e:
    st.error(f"❌ Error al leer Duración >90 min: {e}")
    st.stop()

# =====================================================
# PROCESAMIENTO GLOBAL
# =====================================================
st.header("📊 Resultado Consolidado Diario")

try:
    df_result = procesar_global(
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

st.success("✔️ Datos procesados correctamente.")

# Mostrar tabla
st.dataframe(df_result, use_container_width=True)

# =====================================================
# DESCARGA
# =====================================================
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode("utf-8")

csv = convert_df(df_result)

st.download_button(
    label="📥 Descargar Consolidado (CSV)",
    data=csv,
    file_name="CMI_Aeropuerto_Consolidado.csv",
    mime="text/csv"
)


