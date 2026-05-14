# =============================================================================
# transform.py — Fase TRANSFORM: T01 a T10
# =============================================================================

import pandas as pd
import numpy as np
import hashlib
import unicodedata
import tracker


# -----------------------------------------------------------------------------
# T01 — Eliminación de duplicados
# -----------------------------------------------------------------------------
def t01_duplicados(df_raw, df_provincia, df_tipo):
    print("\n--- T01: Eliminación de duplicados ---")

    n_antes = len(df_raw)
    df_raw = df_raw.drop_duplicates()
    n_exactos = n_antes - len(df_raw)

    n_antes_pk = len(df_raw)
    df_raw = df_raw.drop_duplicates(subset=["ID_ACCIDENTE"])
    n_pk = n_antes_pk - len(df_raw)

    n_t01_raw = n_exactos + n_pk
    print(f"  df_raw       → duplicados exactos: {n_exactos} | por ID_ACCIDENTE: {n_pk}")
    tracker.log_fase("T01_Duplicados", "raw", n_antes, len(df_raw), n_t01_raw,
                     f"Exactos={n_exactos}, PK={n_pk}")

    n_antes = len(df_provincia)
    df_provincia = df_provincia.drop_duplicates(subset=["PROVINCIA", "AÑO"])
    n_dup = n_antes - len(df_provincia)
    print(f"  df_provincia → por (PROVINCIA, AÑO): {n_dup}")
    tracker.log_fase("T01_Duplicados", "provincia", n_antes, len(df_provincia), n_dup,
                     "Duplicados por (PROVINCIA,AÑO)")

    n_antes = len(df_tipo)
    df_tipo = df_tipo.drop_duplicates(subset=["TIPO_ACCIDENTE", "AÑO"])
    n_dup = n_antes - len(df_tipo)
    print(f"  df_tipo      → por (TIPO_ACCIDENTE, AÑO): {n_dup}")
    tracker.log_fase("T01_Duplicados", "tipo", n_antes, len(df_tipo), n_dup,
                     "Duplicados por (TIPO,AÑO)")

    return df_raw, df_provincia, df_tipo


# -----------------------------------------------------------------------------
# T03 — Eliminación de blancos (ejecutar antes de T02)
# -----------------------------------------------------------------------------
def t03_blancos(df_raw, df_provincia, df_tipo, df_resumen):
    print("\n--- T03: Eliminación de blancos (ejecutado antes de T02) ---")

    COLS_TEXTO_RAW = [
        "PROVINCIA", "DIA_SEMANA", "ZONA", "TIPO_VIA",
        "TIPO_ACCIDENTE", "CONDICION_METEO", "CONDICION_ILUMINACION",
    ]

    n_blancos_total = 0
    for col in COLS_TEXTO_RAW:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype(str).str.strip()
            n_blancos = (df_raw[col] == "").sum()
            if n_blancos > 0:
                df_raw.loc[df_raw[col] == "", col] = np.nan
                n_blancos_total += n_blancos
                print(f"  df_raw[{col}]: {n_blancos} blancos convertidos a NaN")

    for col in ["PROVINCIA"]:
        if col in df_provincia.columns:
            df_provincia[col] = df_provincia[col].astype(str).str.strip()

    for col in ["TIPO_ACCIDENTE"]:
        if col in df_tipo.columns:
            df_tipo[col] = df_tipo[col].astype(str).str.strip()

    for col in ["CONTEXTO"]:
        if col in df_resumen.columns:
            df_resumen[col] = df_resumen[col].astype(str).str.strip()

    if n_blancos_total == 0:
        print("  ✔ Sin blancos detectados en columnas de texto.")
    print(f"  Total blancos convertidos a NaN: {n_blancos_total}")
    tracker.log_fase("T03_Blancos", "raw+prov+tipo", len(df_raw), len(df_raw),
                     descartados=0, motivo=f"{n_blancos_total} blancos→NaN")

    return df_raw, df_provincia, df_tipo, df_resumen


# -----------------------------------------------------------------------------
# T02 — Tratamiento de nulos
# -----------------------------------------------------------------------------
def t02_nulos(df_raw):
    print("\n--- T02: Tratamiento de nulos ---")

    nulos_serie = df_raw.isnull().sum()
    nulos_con_valor = nulos_serie[nulos_serie > 0]
    if len(nulos_con_valor) == 0:
        print("  ✔ Sin nulos detectados en df_raw.")
    else:
        print(nulos_con_valor.to_string())

    n_imputados = 0

    COLS_VICTIMAS = ["TOTAL_VICTIMAS", "FALLECIDOS_24H", "HERIDOS_GRAVES", "HERIDOS_LEVES"]
    for col in COLS_VICTIMAS:
        n = df_raw[col].isnull().sum()
        if n > 0:
            df_raw[col] = df_raw[col].fillna(0)
            n_imputados += n
            print(f"  df_raw[{col}]: {n} nulos → imputados con 0")

    for col in ["HORA", "MES"]:
        n = df_raw[col].isnull().sum()
        if n > 0:
            mediana = df_raw[col].median()
            df_raw[col] = df_raw[col].fillna(mediana)
            n_imputados += n
            print(f"  df_raw[{col}]: {n} nulos → imputados con mediana ({mediana})")

    COLS_CAT_IMPUTE = [
        "ZONA", "TIPO_VIA", "TIPO_ACCIDENTE",
        "CONDICION_METEO", "CONDICION_ILUMINACION",
    ]
    for col in COLS_CAT_IMPUTE:
        n = df_raw[col].isnull().sum()
        if n > 0:
            moda = df_raw[col].mode()[0]
            df_raw[col] = df_raw[col].fillna(moda)
            n_imputados += n
            print(f"  df_raw[{col}]: {n} nulos → imputados con moda ('{moda}')")

    nulos_restantes = df_raw.isnull().sum().sum()
    print(f"  Total nulos imputados: {n_imputados} | Nulos restantes: {nulos_restantes}")
    tracker.log_fase("T02_Nulos", "raw", len(df_raw), len(df_raw),
                     descartados=0, motivo=f"{n_imputados} nulos imputados (0/mediana/moda)")

    return df_raw


# -----------------------------------------------------------------------------
# T04 — Corrección de tipos
# -----------------------------------------------------------------------------
def t04_tipos(df_raw, df_provincia, df_tipo):
    print("\n--- T04: Corrección de tipos ---")

    n_recasteadas = 0

    CASTINGS_RAW = {
        "ID_ACCIDENTE"  : "int64",
        "AÑO"           : "int16",
        "MES"           : "int8",
        "HORA"          : "int8",
        "TOTAL_VICTIMAS": "int16",
        "FALLECIDOS_24H": "int8",
        "HERIDOS_GRAVES": "int16",
        "HERIDOS_LEVES" : "int16",
    }
    for col, dtype in CASTINGS_RAW.items():
        if col in df_raw.columns:
            tipo_antes = str(df_raw[col].dtype)
            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").astype(dtype)
            if tipo_antes != dtype:
                n_recasteadas += 1
                print(f"  df_raw[{col}]: {tipo_antes} → {dtype}")

    COLS_CAT_RAW = [
        "DIA_SEMANA", "ZONA", "TIPO_VIA", "TIPO_ACCIDENTE",
        "CONDICION_METEO", "CONDICION_ILUMINACION", "PROVINCIA",
    ]
    for col in COLS_CAT_RAW:
        if col in df_raw.columns and str(df_raw[col].dtype) != "category":
            df_raw[col] = df_raw[col].astype("category")
            n_recasteadas += 1
            print(f"  df_raw[{col}]: object → category")

    for col in ["AÑO", "ACCIDENTES", "VICTIMAS", "FALLECIDOS", "HERIDOS_GRAVES", "HERIDOS_LEVES"]:
        if col in df_provincia.columns:
            df_provincia[col] = pd.to_numeric(df_provincia[col], errors="coerce").astype("int32")

    for col in ["AÑO", "ACCIDENTES", "VICTIMAS", "FALLECIDOS"]:
        if col in df_tipo.columns:
            df_tipo[col] = pd.to_numeric(df_tipo[col], errors="coerce").astype("int32")

    print(f"  Total columnas recasteadas: {n_recasteadas}")
    tracker.log_fase("T04_Tipos", "raw+prov+tipo", len(df_raw), len(df_raw),
                     descartados=0, motivo=f"{n_recasteadas} columnas casteadas")

    return df_raw, df_provincia, df_tipo, n_recasteadas


# -----------------------------------------------------------------------------
# T05 — Normalización de fechas
# -----------------------------------------------------------------------------
def t05_fechas(df_raw):
    print("\n--- T05: Normalización de fechas (ISO-8601) ---")

    df_raw["FECHA_ACCIDENTE"] = pd.to_datetime(
        df_raw["AÑO"].astype(str) + "-"
        + df_raw["MES"].astype(str).str.zfill(2) + "-01",
        format="%Y-%m-%d",
        errors="coerce",
    )

    n_fechas_ok  = df_raw["FECHA_ACCIDENTE"].notna().sum()
    n_fechas_err = df_raw["FECHA_ACCIDENTE"].isna().sum()

    print(f"  Columna FECHA_ACCIDENTE generada en formato ISO-8601 (YYYY-MM-01)")
    print(f"  Rango: {df_raw['FECHA_ACCIDENTE'].min().date()} "
          f"→ {df_raw['FECHA_ACCIDENTE'].max().date()}")
    print(f"  Fechas válidas: {n_fechas_ok} | Errores de parseo: {n_fechas_err}")
    tracker.log_fase("T05_Fechas", "raw", len(df_raw), len(df_raw),
                     descartados=0,
                     motivo=f"FECHA_ACCIDENTE ISO-8601 creada; {n_fechas_err} errores parseo")

    return df_raw


# -----------------------------------------------------------------------------
# T06 — Normalización de texto
# -----------------------------------------------------------------------------
def _normalizar_texto(valor):
    if pd.isna(valor):
        return valor
    s = str(valor).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


def t06_texto(df_raw, df_provincia, df_tipo):
    print("\n--- T06: Normalización de texto ---")

    COLS_NORM_RAW = [
        "TIPO_VIA", "TIPO_ACCIDENTE",
        "CONDICION_METEO", "CONDICION_ILUMINACION", "ZONA",
    ]
    n_campos_norm = 0
    for col in COLS_NORM_RAW:
        if col in df_raw.columns:
            df_raw[col + "_NORM"] = df_raw[col].astype(str).apply(_normalizar_texto)
            n_campos_norm += 1
            print(f"  df_raw: {col} → {col}_NORM  (ej: '{df_raw[col + '_NORM'].iloc[0]}')")

    df_provincia["PROVINCIA_NORM"] = df_provincia["PROVINCIA"].apply(_normalizar_texto)
    df_tipo["TIPO_ACCIDENTE_NORM"] = df_tipo["TIPO_ACCIDENTE"].apply(_normalizar_texto)

    total_norm = n_campos_norm + 2
    print(f"  Total campos normalizados: {total_norm} (columnas _NORM, originales conservadas)")
    tracker.log_fase("T06_Texto", "raw+prov+tipo", len(df_raw), len(df_raw),
                     descartados=0, motivo=f"{total_norm} campos normalizados en columnas _NORM")

    return df_raw, df_provincia, df_tipo


# -----------------------------------------------------------------------------
# T07 — Corrección de rangos
# -----------------------------------------------------------------------------
def t07_rangos(df_raw):
    print("\n--- T07: Corrección de rangos ---")

    n_antes_t07 = len(df_raw)
    n_out_rango = 0

    mask = ~df_raw["HORA"].between(0, 23)
    n = int(mask.sum())
    if n > 0:
        df_raw = df_raw[~mask].copy()
        print(f"  HORA fuera de [0–23]: {n} registros eliminados")
    else:
        print(f"  HORA [0–23]: OK — 0 registros fuera de rango")
    n_out_rango += n

    mask = ~df_raw["MES"].between(1, 12)
    n = int(mask.sum())
    if n > 0:
        df_raw = df_raw[~mask].copy()
        print(f"  MES fuera de [1–12]: {n} registros eliminados")
    else:
        print(f"  MES [1–12]: OK — 0 registros fuera de rango")
    n_out_rango += n

    mask = ~df_raw["AÑO"].between(2019, 2022)
    n = int(mask.sum())
    if n > 0:
        df_raw = df_raw[~mask].copy()
        print(f"  AÑO fuera de [2019–2022]: {n} registros eliminados")
    else:
        print(f"  AÑO [2019–2022]: OK — 0 registros fuera de rango")
    n_out_rango += n

    for col in ["TOTAL_VICTIMAS", "FALLECIDOS_24H", "HERIDOS_GRAVES", "HERIDOS_LEVES"]:
        mask = df_raw[col] < 0
        n = int(mask.sum())
        if n > 0:
            df_raw = df_raw[~mask].copy()
            print(f"  {col} < 0: {n} registros eliminados")
        else:
            print(f"  {col} >= 0: OK")
        n_out_rango += n

    print(f"  Total registros eliminados por rango: {n_out_rango}")
    tracker.log_fase("T07_Rangos", "raw", n_antes_t07, len(df_raw), n_out_rango,
                     "Fuera de rango negocio (HORA/MES/AÑO/victimas)")

    return df_raw


# -----------------------------------------------------------------------------
# T08 — Detección y gestión de outliers
# -----------------------------------------------------------------------------
def t08_outliers(df_raw):
    print("\n--- T08: Detección y gestión de outliers (IQR, winsorizing) ---")

    COLS_OUTLIER = ["TOTAL_VICTIMAS", "HERIDOS_GRAVES", "HERIDOS_LEVES"]
    n_acotados_total = 0

    for col in COLS_OUTLIER:
        Q1  = df_raw[col].quantile(0.25)
        Q3  = df_raw[col].quantile(0.75)
        IQR = Q3 - Q1

        if IQR == 0:
            media  = df_raw[col].mean()
            std    = df_raw[col].std()
            lower  = media - 3 * std
            upper  = media + 3 * std
            metodo = "z-score (IQR=0)"
        else:
            lower  = Q1 - 1.5 * IQR
            upper  = Q3 + 1.5 * IQR
            metodo = "IQR"

        mask_out  = (df_raw[col] < lower) | (df_raw[col] > upper)
        n_out     = int(mask_out.sum())

        lower_val = max(round(lower), 0)
        upper_val = round(upper)
        df_raw.loc[df_raw[col] < lower, col] = lower_val
        df_raw.loc[df_raw[col] > upper, col] = upper_val

        print(f"  {col:<20} método={metodo:<18} límites=[{lower_val}, {upper_val}] "
              f"| outliers acotados: {n_out}")
        n_acotados_total += n_out

    print(f"  Estrategia: winsorizing (acotar en los límites, no eliminar registros)")
    print(f"  Total outliers acotados: {n_acotados_total} (registros conservados)")
    tracker.log_fase("T08_Outliers", "raw", len(df_raw), len(df_raw),
                     descartados=0,
                     motivo=f"{n_acotados_total} outliers acotados (IQR/z-score, winsorizing)")

    return df_raw


# -----------------------------------------------------------------------------
# T09 — Seudonimización de PII
# -----------------------------------------------------------------------------
def _sha256_hash(valor):
    return hashlib.sha256(str(valor).encode("utf-8")).hexdigest()


def t09_pii(df_raw):
    print("\n--- T09: Seudonimización de PII (SHA-256) ---")

    df_raw["ID_ACCIDENTE_HASH"] = df_raw["ID_ACCIDENTE"].apply(_sha256_hash)
    df_raw = df_raw.drop(columns=["ID_ACCIDENTE"])

    print(f"  ID_ACCIDENTE → ID_ACCIDENTE_HASH (SHA-256, 64 caracteres hexadecimales)")
    print(f"  Ejemplo hash: {df_raw['ID_ACCIDENTE_HASH'].iloc[0][:32]}...")
    print(f"  Columna original ID_ACCIDENTE eliminada del dataset procesado")
    tracker.log_fase("T09_PII", "raw", len(df_raw), len(df_raw),
                     descartados=0, motivo="ID_ACCIDENTE→ID_ACCIDENTE_HASH (SHA-256)")

    return df_raw


# Añadir import que faltaba arriba
import hashlib


# -----------------------------------------------------------------------------
# T10 — Validación referencial
# -----------------------------------------------------------------------------
def t10_referencial(df_raw, df_provincia, df_tipo):
    print("\n--- T10: Validación referencial ---")

    provincias_raw = set(df_raw["PROVINCIA"].astype(str).unique())
    provincias_ref = set(df_provincia["PROVINCIA"].unique())
    huerfanos_prov = provincias_raw - provincias_ref
    n_huerf_prov   = int(df_raw["PROVINCIA"].astype(str).isin(huerfanos_prov).sum())

    print(f"  [Check 1] df_raw[PROVINCIA] vs df_provincia[PROVINCIA]")
    print(f"    Provincias únicas en raw   : {len(provincias_raw)}")
    print(f"    Provincias únicas en ref   : {len(provincias_ref)}")
    print(f"    Provincias sin referencia  : {len(huerfanos_prov)}")
    if huerfanos_prov:
        print(f"    → {huerfanos_prov}")
    print(f"    Registros huérfanos        : {n_huerf_prov} (documentados, NO eliminados)")

    tipos_raw      = set(df_raw["TIPO_ACCIDENTE"].astype(str).unique())
    tipos_ref      = set(df_tipo["TIPO_ACCIDENTE"].unique())
    huerfanos_tipo = tipos_raw - tipos_ref
    n_huerf_tipo   = int(df_raw["TIPO_ACCIDENTE"].astype(str).isin(huerfanos_tipo).sum())

    print(f"\n  [Check 2] df_raw[TIPO_ACCIDENTE] vs df_tipo[TIPO_ACCIDENTE]")
    print(f"    Tipos únicos en raw        : {len(tipos_raw)}")
    print(f"    Tipos únicos en ref        : {len(tipos_ref)}")
    print(f"    Tipos sin referencia       : {len(huerfanos_tipo)}")
    if huerfanos_tipo:
        print(f"    → {huerfanos_tipo}")
    print(f"    Registros huérfanos        : {n_huerf_tipo} (documentados, NO eliminados)")

    tracker.log_fase("T10_Referencial", "raw↔prov", len(df_raw), len(df_raw),
                     descartados=0,
                     motivo=f"{n_huerf_prov} huerf.PROV + {n_huerf_tipo} huerf.TIPO (doc.)")


# -----------------------------------------------------------------------------
# run() — Ejecuta todas las transformaciones en orden
# -----------------------------------------------------------------------------
def run(df_raw, df_provincia, df_tipo, df_resumen):
    df_raw, df_provincia, df_tipo              = t01_duplicados(df_raw, df_provincia, df_tipo)
    df_raw, df_provincia, df_tipo, df_resumen  = t03_blancos(df_raw, df_provincia, df_tipo, df_resumen)
    df_raw                                     = t02_nulos(df_raw)
    df_raw, df_provincia, df_tipo, n_recast    = t04_tipos(df_raw, df_provincia, df_tipo)
    df_raw                                     = t05_fechas(df_raw)
    df_raw, df_provincia, df_tipo              = t06_texto(df_raw, df_provincia, df_tipo)
    df_raw                                     = t07_rangos(df_raw)
    df_raw                                     = t08_outliers(df_raw)
    df_raw                                     = t09_pii(df_raw)
    t10_referencial(df_raw, df_provincia, df_tipo)

    return df_raw, df_provincia, df_tipo, df_resumen, n_recast
