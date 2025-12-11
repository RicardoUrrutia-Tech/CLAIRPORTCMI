import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from processor import procesar_global

st.set_page_config(page_title="CLAIRPORT – Consolidado Global", layout="wide")
st.title("📊 Consolidado Global Aeroportuario – CLAIRPORT")

# ... (todo igual que la última versión que te mandé)
# Lectura de archivos + date_from/date_to + botón Procesar
# ...

    try:
        df_diario, df_semanal, df_periodo, df_transpuesta = procesar_global(
            df_ventas, df_performance, df_auditorias,
            df_offtime, df_dur90, df_dur30,
            df_ins, df_aband, df_resc,
            df_whatsapp,
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

    st.subheader("📐 Vista Traspuesta (KPIs x Día / Semana)")
    st.dataframe(df_transpuesta, use_container_width=True)

    # =====================================================
    # 📥 DESCARGA (con estilo Cabify en semanas)
    # =====================================================

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_diario.to_excel(writer, index=False, sheet_name="Diario")
        df_semanal.to_excel(writer, index=False, sheet_name="Semanal")
        df_periodo.to_excel(writer, index=False, sheet_name="Periodo")
        df_transpuesta.to_excel(writer, index=False, sheet_name="Vista_Traspuesta")

        # 🎨 Estilo Cabify (moradul) para columnas de Semana en Vista_Traspuesta
        workbook = writer.book
        ws = writer.sheets["Vista_Traspuesta"]

        week_format = workbook.add_format({
            "bg_color": "#4A2B8D",   # Moradul Cabify
            "font_color": "#FFFFFF",
            "bold": True
        })

        for col_idx, col_name in enumerate(df_transpuesta.columns):
            if isinstance(col_name, str) and col_name.startswith("Semana "):
                ws.set_column(col_idx, col_idx, 20, week_format)

    st.download_button(
        "💾 Descargar Consolidado Global",
        data=output.getvalue(),
        file_name="Consolidado_Global.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )



