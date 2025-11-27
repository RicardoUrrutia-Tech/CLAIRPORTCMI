import streamlit as st
import pandas as pd
from io import BytesIO
import os, sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path = [p for p in sys.path if p != "app.py"]
sys.path.insert(0, CURRENT_DIR)

from processor import procesar_global

st.set_page_config(page_title="Reporte Diario Consolidado", layout="wide")
st.title("🟦 Reporte Diario Consolidado – Aeropuerto Cabify (DEBUG Mode)")

# Rango fechas
st.header("📅 Período")
col1, col2 = st.columns(2)
date_from = col1.date_input("Desde")
date_to   = col2.date_input("Hasta")

if date_to < date_from:
    st.error("❌ La fecha final debe ser mayor")
    st.stop()

# Archivos
st.header("📤 Subir los 5 archivos obligatorios")

ventas_file    = st.file_uploader("Ventas", type=["xlsx"])
perf_file      = st.file_uploader("Performance", type=["csv"])
aud_file       = st.file_uploader("Auditorías", type=["csv"])
off_file       = st.file_uploader("OFF TIME", type=["csv"])
duracion_file  = st.file_uploader("Duración >90", type=["csv"])

if st.button("Procesar"):

    if not all([ventas_file, perf_file, aud_file, off_file, duracion_file]):
        st.error("❌ Faltan archivos")
        st.stop()

    df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    df_perf   = pd.read_csv(perf_file, encoding="utf-8", sep=",")
    df_aud    = pd.read_csv(aud_file, encoding="utf-8-sig", sep=";")
    df_off    = pd.read_csv(off_file, encoding="utf-8-sig", sep=",")
    df_dur    = pd.read_csv(duracion_file, encoding="utf-8-sig", sep=",")

    st.subheader("🔍 DEBUG – Primeras filas de cada archivo")
    st.write("VENTAS:", df_ventas.head())
    st.write("PERFORMANCE:", df_perf.head())
    st.write("AUDITORÍAS:", df_aud.head())
    st.write("OFF TIME:", df_off.head())
    st.write("DURACIÓN:", df_dur.head())

    df_final = procesar_global(
        df_ventas, df_perf, df_aud, df_off, df_dur,
        date_from, date_to
    )

    st.success("✔ Procesado")
    st.dataframe(df_final)



