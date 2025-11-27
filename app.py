import streamlit as st
import pandas as pd
import csv
from processor import procesar_global

st.set_page_config(page_title="CMI Diario – Global", layout="wide")
st.title("🟦 Consolidado Diario – Global (Ventas / Performance / Auditorías / OffTime / >90 min)")

# =====================================================
# SUBIR ARCHIVOS
# =====================================================

st.header("📤 Carga de Archivos")

ventas_file = st.file_uploader("Cargar reporte de Ventas (.xlsx)", type=["xlsx"])
performance_file = st.file_uploader("Cargar reporte de Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("Cargar reporte de Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("Cargar reporte de OffTime (.csv)", type=["csv"])
duracion_file = st.file_uploader("Cargar reporte DO > 90 min (.csv)", type=["csv"])

# =====================================================
# FILTRO DE FECHAS
# =====================================================

st.header("📅 Filtro de Fechas")
col1, col2 = st.columns(2)
date_from = col1.date_input("Fecha desde")
date_to = col2.date_input("Fecha hasta")

if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file]):
    st.info("🔄 Sube todos los archivos para continuar...")
    st.stop()

# =====================================================
# LECTURA DE ARCHIVOS
# =====================================================

# VENTAS ------------------------------------------------
try:
    df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
except Exception as e:
    st.error(f"❌ Error en Ventas: {e}")
    st.stop()

# PERFORMANCE ------------------------------------------
try:
    df_performance = pd.read_csv(
        performance_file,
        sep=",",
        encoding="latin-1",
        engine="python"
    )
except Exception as e:
    st.error(f"❌ Error en Performance: {e}")
    st.stop()

# AUDITORÍAS (LECTOR INTELIGENTE) ----------------------
try:
    raw = auditorias_file.read().decode("latin-1")
    auditorias_file.seek(0)

    dialect = csv.Sniffer().sniff(raw, delimiters=";,|\t")
    detected_sep = dialect.delimiter

    df_auditorias = pd.read_csv(
        auditorias_file,
        sep=detected_sep,
        encoding="latin-1",
        engine="python"
    )

except Exception as e:
    st.error(f"❌ Error al leer Auditorías: {e}")
    st.stop()

# OFF TIME ---------------------------------------------
try:
    df_off = pd.read_csv(
        offtime_file,
        sep=",",
        encoding="latin-1",
        engine="python"
    )
except Exception as e:
    st.error(f"❌ Error en OffTime: {e}")
    st.stop()

# DURACIÓN > 90 ----------------------------------------
try:
    df_dur = pd.read_csv(
        duracion_file,
        sep=",",
        encoding="latin-1",
        engine="python"
    )
except Exception as e:
    st.error(f"❌ Error en Duración >90: {e}")
    st.stop()


# =====================================================
# PROCESAR TODO
# =====================================================
st.header("⚙ Procesando...")

try:
    df_final = procesar_global(
        df_ventas,
        df_performance,
        df_auditorias,
        df_off,
        df_dur,
        date_from,
        date_to
    )
except Exception as e:
    st.error(f"❌ Error al procesar datos: {e}")
    st.stop()


# =====================================================
# SALIDA FINAL
# =====================================================

st.success("✅ Procesamiento completado")

st.header("📄 Resultado Diario Consolidado")
st.dataframe(df_final, use_container_width=True)

# DESCARGA -------------------------------------------------

@st.cache_data
def descargar(df):
    return df.to_csv(index=False).encode("utf-8-sig")

st.download_button(
    "📥 Descargar CSV Consolidado",
    data=descargar(df_final),
    file_name="CMI_Global_Diario.csv",
    mime="text/csv"
)

