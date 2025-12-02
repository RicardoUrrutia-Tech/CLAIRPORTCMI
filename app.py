import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from processor import procesar_global

st.set_page_config(page_title="CLAIRPORT – Consolidado Global", layout="wide")
st.title("📊 Consolidado Global Aeroportuario – CLAIRPORT")

# =====================================================
# 📥 LECTORES ROBUSTOS PARA CSV/EXCEL
# =====================================================

def read_generic_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)
    text = raw.decode("latin-1").replace("ï»¿", "").replace("\ufeff", "")
    sep = ";" if text.count(";") > text.count(",") else ","
    return pd.read_csv(StringIO(text), sep=sep, engine="python")

def read_auditorias_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)
    text = raw.decode("latin-1").replace("ï»¿", "").replace("\ufeff", "")
    return pd.read_csv(StringIO(text), sep=";", engine="python")

# =====================================================
# 📥 CARGA DE ARCHIVOS
# =====================================================

st.header("📥 Cargar Archivos – Todos obligatorios")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader("🔵 Ventas (.csv o .xlsx)", type=["csv", "xlsx"])
    performance_file = st.file_uploader("🟢 Performance (.csv)", type=["csv"])
    auditorias_file = st.file_uploader("🟣 Auditorías (.csv)", type=["csv"])
    offtime_file = st.file_uploader("🟠 Off-Time (.csv)", type=["csv"])

with col2:
    duracion90_file = st.file_uploader("🔴 Duración >90 min (.csv)", type=["csv"])
    duracion30_file = st.file_uploader("🟤 Duración >30 min (.csv)", type=["csv"])
    inspecciones_file = st.file_uploader("🚗 Inspecciones Vehiculares (.xlsx)", type=["xlsx"])
    abandonados_file = st.file_uploader("🟣 Clientes Abandonados (.xlsx)", type=["xlsx"])

st.divider()

# =====================================================
# 📅 RANGO DE FECHAS
# =====================================================

st.header("📅 Seleccionar Rango de Fechas")

col_a, col_b = st.columns(2)
with col_a:
    date_from = st.date_input("📆 Desde:", value=None, format="YYYY-MM-DD")
with col_b:
    date_to = st.date_input("📆 Hasta:", value=None, format="YYYY-MM-DD")

if not date_from or not date_to:
    st.warning("⚠ Debes seleccionar ambas fechas para procesar.")
    st.stop()

date_from = pd.to_datetime(date_from)
date_to = pd.to_datetime(date_to)

st.divider()

# =====================================================
# 🚀 PROCESAR
# =====================================================

if st.button("🚀 Procesar Consolidado Global", type="primary"):

    required = [
        ventas_file, performance_file, auditorias_file, offtime_file,
        duracion90_file, duracion30_file, inspecciones_file, abandonados_file
    ]

    if not all(required):
        st.error("❌ Debes subir TODOS los archivos antes de continuar.")
        st.stop()

    # =====================================================
    # 📌 LECTURA DE ARCHIVOS
    # =====================================================

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
        df_dur90 = read_generic_csv(duracion90_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Duración >90 min: {e}")
        st.stop()

    try:
        df_dur30 = read_generic_csv(duracion30_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Duración >30 min: {e}")
        st.stop()

    try:
        df_ins = pd.read_excel(inspecciones_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Inspecciones: {e}")
        st.stop()

    try:
        df_aband = pd.read_excel(abandonados_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Clientes Abandonados (Excel): {e}")
        st.stop()

    # =====================================================
    # 🔵 PROCESAMIENTO GLOBAL
    # =====================================================

    try:
        df_diario, df_semanal, df_periodo = procesar_global(
            df_ventas, df_performance, df_auditorias,
            df_offtime, df_dur90, df_dur30,
            df_ins, df_aband,
            date_from, date_to
        )
    except Exception as e:
        st.error(f"❌ Error procesando datos: {e}")
        st.stop()

    st.success("✅ Consolidado generado con éxito")

    st.subheader("📅 Diario Consolidado")
    st.dataframe(df_diario, use_container_width=True)

    st.subheader("📆 Semanal Consolidado")
    st.dataframe(df_semanal, use_container_width=True)

    st.subheader("📊 Resumen del Periodo")
    st.dataframe(df_periodo, use_container_width=True)

    # =====================================================
    # 📥 DESCARGA
    # =====================================================

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_diario.to_excel(writer, index=False, sheet_name="Diario")
        df_semanal.to_excel(writer, index=False, sheet_name="Semanal")
        df_periodo.to_excel(writer, index=False, sheet_name="Periodo")

    st.download_button(
        "💾 Descargar Consolidado Global",
        data=output.getvalue(),
        file_name="Consolidado_Global.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Carga todos los archivos, selecciona fechas y presiona **Procesar Consolidado Global**.")



