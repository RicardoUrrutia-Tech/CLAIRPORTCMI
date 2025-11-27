import pandas as pd

def procesar_global_debug(
    df_ventas,
    df_perf,
    df_aud,
    df_off,
    df_dur,
    date_from,
    date_to
):

    print("\n======================== VENTAS ========================")
    print(df_ventas.columns.tolist())
    print(df_ventas.head())

    print("\n===================== PERFORMANCE =======================")
    print(df_perf.columns.tolist())
    print(df_perf.head())

    print("\n==================== AUDITORÍAS =========================")
    print(df_aud.columns.tolist())
    print(df_aud.head())

    print("\n======================= OFFTIME =========================")
    print(df_off.columns.tolist())
    print(df_off.head())

    print("\n=================== DURACIÓN >90 =========================")
    print(df_dur.columns.tolist())
    print(df_dur.head())

    print("\n================== RANGO DE FECHAS ======================")
    print("FROM:", date_from)
    print("TO:", date_to)

    # Aquí NO hacemos procesamiento real
    print("\n⚠️ Modo DEBUG – Sin procesamiento real\n")
    return None

