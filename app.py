import streamlit as st
import pandas as pd

st.set_page_config(page_title="DEBUG – CMI Aeropuerto", layout="wide")
st.title("🛠 DEBUG DE ARCHIVOS – CMI Aeropuerto")

st.write("Carga todos los archivos para inspeccionar sus columnas y contenido real.")

# ----------------------------------------------------
# File Uploads
# ----------------------------------------------------
ventas_file = st.file_uploader("Ventas (.xlsx)", type=["xlsx"])
perf_file = st.file_uploader("Performance (.csv)", type=["csv"])
aud_file = st.file_uploader("Auditorías (.csv)", type=["csv"])
off_file = st.file_uploader("Offtime (.csv)", type=["csv"])
dur_file = st.file_uploader("Duración >90 (.csv)", type=["csv"])

if not all([ventas_file, perf_file, aud_file, off_file, dur_file]):
    st.warning("⚠️ Carga todos los archivos para continuar.")
    st.stop()

# ----------------------------------------------------
# Helper función para normalizar BOM y espacios invisibles
# ----------------------------------------------------
def clean_columns(df):
    df.columns = (
        df.columns
        .str.replace("\ufeff", "", regex=False)  # BOM UTF-8
        .str.replace("\u200b", "", regex=False) # Zero width
        .str.replace("\xa0", " ", regex=False)  # NBSP
        .str.strip()
    )
    return df

# ----------------------------------------------------
# Cargar archivos con debug detallado
# ----------------------------------------------------
def debug_csv(file, name):
    st.subheader(f"🔧 {name}")

    try:
        df = pd.read_csv(file, sep=None, engine="python", encoding="latin-1")
    except Exception as e:
        st.error(f"❌ Error detectando separador automático: {e}")
        st.info("Intentando con separador coma ','")

        try:
            df = pd.read_csv(file, sep=",", engine="python", encoding="latin-1")
        except:
            st.info("Intentando con separador ';'")
            df = pd.read_csv(file, sep=";", engine="python", encoding="latin-1")

    st.write("📌 COLUMNAS ANTES DE LIMPIAR:")
    st.write(df.columns.tolist())

    df = clean_columns(df)

    st.write("📌 COLUMNAS DESPUÉS DE LIMPIAR:")
    st.write(df.columns.tolist())

    st.write("📄 Primeras 5 filas:")
    st.dataframe(df.head())

    st.write("📊 Info del dataframe:")
    st.write(df.dtypes)

    return df


# ----------------------------------------------------
# Ejecutar debug para cada archivo
# ----------------------------------------------------
df_ventas      = pd.read_excel(ventas_file)
st.subheader("🔧 VENTAS")
st.write("📌 COLUMNAS ANTES:", df_ventas.columns.tolist())
df_ventas = clean_columns(df_ventas)
st.write("📌 COLUMNAS DESPUÉS:", df_ventas.columns.tolist())
st.dataframe(df_ventas.head())


df_performance = debug_csv(perf_file, "PERFORMANCE")
df_auditorias  = debug_csv(aud_file, "AUDITORÍAS")
df_offtime     = debug_csv(off_file, "OFFTIME")
df_duracion    = debug_csv(dur_file, "DURACIÓN >90 MINUTOS")

st.success("🔍 DEBUG completado. Copia esta información y mándamela para ajustar el processor.")

