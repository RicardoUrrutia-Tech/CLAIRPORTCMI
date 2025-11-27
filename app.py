import streamlit as st
import pandas as pd
import csv
from io import BytesIO, StringIO
from processor import procesar_global

st.set_page_config(page_title="CMI – Consolidado Diario", layout="wide")
st.title("📊 Consolidado Diario – Aeropuerto")

# ======================================================
# LECTOR UNIVERSAL CSV (BOM-PROOF + DETECTOR DE SEPARADOR)
# ======================================================
def read_any_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)

    # Decodificación con limpieza BOM real
    text = raw.decode("latin-1").replace("\ufeff", "").replace("ï»¿", "")

    # Detectar separador correctamente
    try:
        first_line = text.splitlines()[0]
        dialect = csv.Sniffer().sniff(first_line)
        sep = dialect.delimiter
    except:
        sep = ","  # fallback

    return pd.read_csv(StringIO(text), sep=sep, engine="python")


# ======================================================
# CARGA DE ARCHIVOS
# ======================================================
ventas_file = st.file_uploader("Ventas (.xlsx)", type=["xlsx"])
performance_file = st.file_uploader("Performance (.csv)", type=["csv"])
auditorias_file = st.file_uploader("Auditorías (.csv)", type=["csv"])
offtime_file = st.file_uploader("Off-Time (.csv)", type=["csv"])
duracion_file = st.file_uploader("Duración >90 min (.csv)", type=["csv"])

# ======================================================
# FILTRO DE FECHAS
# ======================================================
st.subheader("📅 Seleccione rango de fechas")
col1, col2 = st.columns(2)
date_from = col1.date_input("Desde fecha", None)
date_to = col2.date_input("Hasta fecha", None)


# ======================================================
# PROCESAR
# ======================================================
if st.button("Procesar Consolidado"):

    if not all([ventas_file, performance_file, auditorias_file, offtime_file, duracion_file]):
        st.error("❌ Falta cargar archivos.")
        st.stop()

    try:
        df_ventas = pd.read_excel(ventas_file)
        df_perf = read_any_csv(performance_file)
        df_aud = read_any_csv(auditorias_file)
        df_off = read_any_csv(offtime_file)
        df_dur = read_any_csv(duracion_file)

    except Exception as e:
        st.error(f"❌ Error leyendo archivos: {e}")
        st.stop()

    try:
        df_final = procesar_global(
            df_ventas, df_perf, df_aud, df_off, df_dur,
            date_from, date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    st.success("Procesado correctamente ✔")
    st.dataframe(df_final, height=450)

    # ======================================================
    # EXPORTAR A EXCEL (CON FORMATO CLP PARA VENTAS)
    # ======================================================
    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Consolidado")

        workbook = writer.book
        worksheet = writer.sheets["Consolidado"]

        formato_clp = workbook.add_format({
            "num_format": "$ #,##0",
            "align": "right"
        })

        cols_monetarias = ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]

        for col in cols_monetarias:
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
