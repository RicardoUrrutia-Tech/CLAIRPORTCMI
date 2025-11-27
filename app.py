import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_global

st.set_page_config(page_title="CMI – Consolidado Diario", layout="wide")
st.title("📊 Consolidado Diario – Aeropuerto")

# ===============================
# CARGA DE ARCHIVOS
# ===============================
ventas_file = st.file_uploader("Ventas (.xlsx)", type=["xlsx"])
performance_file = st.file_uploader("Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("Off-Time (.csv)", type=["csv"])
duracion_file = st.file_uploader("Duración >90 min (.csv)", type=["csv"])

# ===============================
# FILTRO DE FECHAS
# ===============================
st.subheader("📅 Seleccione rango de fechas")
col1, col2 = st.columns(2)
date_from = col1.date_input("Desde fecha", None)
date_to = col2.date_input("Hasta fecha", None)

# ===============================
# PROCESAR
# ===============================
if st.button("Procesar Consolidado"):

    if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file]):
        st.error("Falta cargar archivos.")
        st.stop()

    try:
        df_ventas = pd.read_excel(ventas_file)
        df_perf = pd.read_csv(performance_file, sep=",", encoding="latin-1", engine="python")
        df_aud = pd.read_csv(auditorias_file, sep=",", encoding="latin-1", engine="python")
        df_off = pd.read_csv(offtime_file, sep=",", encoding="latin-1", engine="python")
        df_dur = pd.read_csv(duracion_file, sep=",", encoding="latin-1", engine="python")
    except Exception as e:
        st.error(f"❌ Error leyendo archivos: {e}")
        st.stop()

    try:
        df_final = procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, date_from, date_to)
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    st.success("Procesado correctamente ✔")
    st.dataframe(df_final, height=450)

    # ===============================
    # EXPORTAR EXCEL CON FORMATO MONEDA
    # ===============================
    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Consolidado")

        workbook = writer.book
        worksheet = writer.sheets["Consolidado"]

        # Formato CLP
        formato_clp = workbook.add_format({
            "num_format": "$ #,##0",
            "align": "right"
        })

        colnames = ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]

        for col in colnames:
            if col in df.columns:
                idx = df.columns.get_loc(col)
                worksheet.set_column(idx, idx, 18, formato_clp)

        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_final)

    st.download_button(
        "⬇ Descargar Excel Consolidado",
        data=excel_bytes,
        file_name="Consolidado_Diario.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


