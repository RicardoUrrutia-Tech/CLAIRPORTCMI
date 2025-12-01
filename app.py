import streamlit as st
import pandas as pd
from io import StringIO
from processor import procesar_global

st.set_page_config(page_title="CLAIRPORT – Consolidado Global", layout="wide")

st.title("📊 Consolidado Global Aeroportuario – CLAIRPORT")


# =====================================================
# FORMATEO PARA MOSTRAR DINERO SOLO EN TABLA DIARIA
# =====================================================

def mostrar_dinero(df):
    df = df.copy()
    for c in ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda x: f"$ {int(round(x)):,}".replace(",", ".")
                if isinstance(x, (int, float)) else x
            )
    return df


# =====================================================
# LECTORES DE ARCHIVOS
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
# SUBIDA DE ARCHIVOS
# =====================================================

st.header("📥 Cargar Archivos")

ventas_file = st.file_uploader("🔵 Ventas (.csv/.xlsx)", type=["csv", "xlsx"])
performance_file = st.file_uploader("🟢 Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("🟣 Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("🟠 Off-Time (.csv)", type=["csv"])
duracion_file = st.file_uploader("🔴 Duración >90 minutos (.csv)", type=["csv"])

st.divider()

# =====================================================
# SELECTOR DE FECHAS
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
# PROCESAMIENTO
# =====================================================

if st.button("🚀 Procesar Consolidado", type="primary"):

    if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file]):
        st.error("⚠ Debes cargar TODOS los archivos.")
        st.stop()

    # Lectura
    try:
        df_ventas = read_generic_csv(ventas_file) if ventas_file.name.endswith(".csv") else pd.read_excel(ventas_file)
        df_perf = read_generic_csv(performance_file)
        df_aud = read_auditorias_csv(auditorias_file)
        df_off = read_generic_csv(offtime_file)
        df_dur = read_generic_csv(duracion_file)
    except Exception as e:
        st.error(f"❌ Error leyendo archivos: {e}")
        st.stop()

    # Procesar
    try:
        df_final, df_semanal, df_periodo = procesar_global(
            df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    st.success("✅ Procesado con éxito")

    # Mostrar tablas
    st.subheader("📅 Diario Consolidado")
    st.dataframe(mostrar_dinero(df_final), use_container_width=True)

    st.subheader("📆 Semanal Consolidado")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("📊 Consolidado del Periodo")
    st.dataframe(df_periodo, use_container_width=True)

    # Descargar Excel
    import io
    output = io.BytesIO()
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


