import streamlit as st
import pandas as pd
from io import BytesIO
from processor_global import procesar_global

# ------------------------------------------------------------
# CONFIGURACIÓN DE LA APP
# ------------------------------------------------------------
st.set_page_config(page_title="Reporte Consolidado - Diario General", layout="wide")
st.title("🟦 Consolidador Diario General – Aeropuerto Cabify")

st.markdown("""
Esta aplicación consolida los reportes de **Ventas**, **Performance** y **Auditorías**  
para entregar un **resumen diario general**, sin considerar agentes de forma individual.
""")

# ------------------------------------------------------------
# SUBIDA DE ARCHIVOS
# ------------------------------------------------------------
st.header("📤 Cargar Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader(
        "Reporte de Ventas (Excel .xlsx)",
        type=["xlsx"],
        key="ventas"
    )

with col2:
    performance_file = st.file_uploader(
        "Reporte de Performance (CSV)",
        type=["csv"],
        key="performance"
    )

auditorias_file = st.file_uploader(
    "Reporte de Auditorías (CSV - separador ;) ",
    type=["csv"],
    key="auditorias"
)

# ------------------------------------------------------------
# BOTÓN PROCESAR
# ------------------------------------------------------------
if st.button("🔄 Procesar Reportes"):

    # Validación inicial
    if not ventas_file or not performance_file or not auditorias_file:
        st.error("❌ Debes cargar los 3 archivos para continuar.")
        st.stop()

    # -----------------------------
    # LECTURA VENTAS
    # -----------------------------
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error al leer Ventas: {e}")
        st.stop()

    # -----------------------------
    # LECTURA PERFORMANCE
    # -----------------------------
    try:
        df_performance = pd.read_csv(
            performance_file,
            sep=",",
            encoding="utf-8",
            engine="python",
        )
    except Exception:
        try:
            df_performance = pd.read_csv(
                performance_file,
                sep=",",
                encoding="latin-1",
                engine="python",
            )
        except Exception as e:
            st.error(f"❌ Error al leer Performance: {e}")
            st.stop()

    # -----------------------------
    # LECTURA AUDITORÍAS (FORMATO EXACTO)
    # -----------------------------
    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(
            auditorias_file,
            sep=";",
            encoding="utf-8-sig",
            engine="python",
        )
    except Exception as e:
        st.error(f"❌ Error al leer Auditorías: {e}")
        st.stop()

    if df_auditorias.shape[1] == 0:
        st.error("❌ El archivo de Auditorías no tiene columnas válidas.")
        st.stop()

    # ------------------------------------------------------------
    # PROCESAR DATOS
    # ------------------------------------------------------------
    df_diario = procesar_global(df_ventas, df_performance, df_auditorias)

    st.success("✔ Reporte generado correctamente.")

    # ------------------------------------------------------------
    # MOSTRAR RESULTADOS
    # ------------------------------------------------------------
    st.header("📅 Resumen Diario Consolidado")
    st.dataframe(df_diario, use_container_width=True)

    # ------------------------------------------------------------
    # DESCARGA EN EXCEL
    # ------------------------------------------------------------
    st.header("📥 Descargar Excel Consolidado")

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Diario Consolidado")
        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_diario)

    st.download_button(
        label="⬇ Descargar Reporte Diario Consolidado",
        data=excel_bytes,
        file_name="Reporte_Diario_Consolidado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube los archivos y presiona **Procesar Reportes** para continuar.")

