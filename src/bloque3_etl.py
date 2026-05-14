# =============================================================================
# PIPELINE ETL — ACCIDENTES DE TRÁFICO EN ESPAÑA (2019–2022)
# BLOQUE 3: Extracción, Transformación y Limpieza (ETL)
# =============================================================================
# Fuente: datos.gob.es (DGT — Dirección General de Tráfico)
# =============================================================================

import pandas as pd
import numpy as np
import hashlib
import json
import os
import unicodedata
from datetime import datetime

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Directorio base = carpeta donde está este script (funciona desde cualquier sitio)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FICHEROS = {
    "raw"      : os.path.join(BASE_DIR, "accidentes_raw.csv"),
    "provincia": os.path.join(BASE_DIR, "accidentes_por_provincia.csv"),
    "tipo"     : os.path.join(BASE_DIR, "accidentes_por_tipo.csv"),
    "resumen"  : os.path.join(BASE_DIR, "accidentes_resumen.csv"),
}

TRACKING_FILE = os.path.join(BASE_DIR, "logs", "pipeline_tracking.json")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
RAW_DIR       = os.path.join(BASE_DIR, "data", "raw")
ENCODING      = "utf-8-sig"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# =============================================================================
# MÓDULO TRACKER
# Registra la entrada/salida de registros en cada fase del pipeline
# =============================================================================

tracking_log = []

def log_fase(fase, fuente, registros_entrada, registros_salida, descartados=None, motivo="—"):
    if descartados is None:
        descartados = registros_entrada - registros_salida
    entrada  = int(registros_entrada)
    salida   = int(registros_salida)
    descart  = int(descartados)
    entry = {
        "fase"              : fase,
        "fuente"            : fuente,
        "registros_entrada" : entrada,
        "registros_salida"  : salida,
        "descartados"       : descart,
        "motivo_principal"  : motivo,
        "timestamp"         : datetime.utcnow().isoformat() + "Z",
    }
    tracking_log.append(entry)
    print(f"  [TRACK] {fase:<22} | {fuente:<12} | "
          f"entrada={entrada:>6} | salida={salida:>6} | "
          f"desc={descart:>5} | {motivo}")


def guardar_tracking():
    with open(TRACKING_FILE, "w", encoding="utf-8") as f:
        json.dump(tracking_log, f, ensure_ascii=False, indent=2)
    print(f"\n  ✔ Tracking guardado en '{TRACKING_FILE}'")


# =============================================================================
# FASE EXTRACT — Carga de fuentes originales
# =============================================================================

print("=" * 72)
print("BLOQUE 3 — PIPELINE ETL: EXTRACCIÓN, TRANSFORMACIÓN Y LIMPIEZA")
print("=" * 72)

print("\n[EXTRACT] Cargando fuentes de datos...")

dataframes = {}
for nombre, fichero in FICHEROS.items():
    try:
        df = pd.read_csv(fichero, encoding=ENCODING)
        dataframes[nombre] = df
        log_fase("EXTRACT", nombre, df.shape[0], df.shape[0],
                 descartados=0, motivo="Carga OK")
        print(f"  ✔  {fichero:<40} → {df.shape[0]:>5} filas, {df.shape[1]} columnas")
    except FileNotFoundError:
        print(f"  ✗  ERROR: No se encontró '{fichero}'")
        raise
    except Exception as e:
        print(f"  ✗  ERROR al cargar '{fichero}': {e}")
        raise

df_raw       = dataframes["raw"].copy()
df_provincia = dataframes["provincia"].copy()
df_tipo      = dataframes["tipo"].copy()
df_resumen   = dataframes["resumen"].copy()

# Guardamos copia cruda en data/raw/ para trazabilidad (fuente sin modificar)
import shutil
os.makedirs(RAW_DIR, exist_ok=True)
for nombre, ruta in FICHEROS.items():
    shutil.copy2(ruta, os.path.join(RAW_DIR, os.path.basename(ruta)))
print("  ✔  Copias crudas guardadas en data/raw/")


# =============================================================================
# T01 — ELIMINACIÓN DE DUPLICADOS
# drop_duplicates() por clave primaria + duplicados exactos
# =============================================================================

print("\n--- T01: Eliminación de duplicados ---")

# df_raw: duplicados exactos primero, luego por clave primaria
n_antes = len(df_raw)
df_raw = df_raw.drop_duplicates()
n_exactos = n_antes - len(df_raw)

n_antes_pk = len(df_raw)
df_raw = df_raw.drop_duplicates(subset=["ID_ACCIDENTE"])
n_pk = n_antes_pk - len(df_raw)

n_t01_raw = n_exactos + n_pk
print(f"  df_raw       → duplicados exactos: {n_exactos} | por ID_ACCIDENTE: {n_pk}")
log_fase("T01_Duplicados", "raw",       n_antes,       len(df_raw),       n_t01_raw,
         f"Exactos={n_exactos}, PK={n_pk}")

n_antes = len(df_provincia)
df_provincia = df_provincia.drop_duplicates(subset=["PROVINCIA", "AÑO"])
n_dup = n_antes - len(df_provincia)
print(f"  df_provincia → por (PROVINCIA, AÑO): {n_dup}")
log_fase("T01_Duplicados", "provincia", n_antes, len(df_provincia), n_dup,
         "Duplicados por (PROVINCIA,AÑO)")

n_antes = len(df_tipo)
df_tipo = df_tipo.drop_duplicates(subset=["TIPO_ACCIDENTE", "AÑO"])
n_dup = n_antes - len(df_tipo)
print(f"  df_tipo      → por (TIPO_ACCIDENTE, AÑO): {n_dup}")
log_fase("T01_Duplicados", "tipo",      n_antes, len(df_tipo),      n_dup,
         "Duplicados por (TIPO,AÑO)")


# =============================================================================
# T03 — ELIMINACIÓN DE BLANCOS
# strip() y reemplazar "" por NaN ANTES de T02 (según rúbrica)
# =============================================================================

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
log_fase("T03_Blancos", "raw+prov+tipo", len(df_raw), len(df_raw),
         descartados=0, motivo=f"{n_blancos_total} blancos→NaN")


# =============================================================================
# T02 — TRATAMIENTO DE NULOS
# Estrategia por columna: imputar media/moda o descartar
# =============================================================================

print("\n--- T02: Tratamiento de nulos ---")

print("  Nulos por columna en df_raw ANTES de imputación:")
nulos_serie = df_raw.isnull().sum()
nulos_con_valor = nulos_serie[nulos_serie > 0]
if len(nulos_con_valor) == 0:
    print("  ✔ Sin nulos detectados en df_raw.")
else:
    print(nulos_con_valor.to_string())

n_imputados = 0

# Numéricas de víctimas: imputar 0 (sin víctima registrada = 0)
COLS_VICTIMAS = ["TOTAL_VICTIMAS", "FALLECIDOS_24H", "HERIDOS_GRAVES", "HERIDOS_LEVES"]
for col in COLS_VICTIMAS:
    n = df_raw[col].isnull().sum()
    if n > 0:
        df_raw[col] = df_raw[col].fillna(0)
        n_imputados += n
        print(f"  df_raw[{col}]: {n} nulos → imputados con 0")

# Numéricas de tiempo: imputar mediana
for col in ["HORA", "MES"]:
    n = df_raw[col].isnull().sum()
    if n > 0:
        mediana = df_raw[col].median()
        df_raw[col] = df_raw[col].fillna(mediana)
        n_imputados += n
        print(f"  df_raw[{col}]: {n} nulos → imputados con mediana ({mediana})")

# Categóricas: imputar moda
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
log_fase("T02_Nulos", "raw", len(df_raw), len(df_raw),
         descartados=0, motivo=f"{n_imputados} nulos imputados (0/mediana/moda)")


# =============================================================================
# T04 — CORRECCIÓN DE TIPOS
# Convertir a int, float, datetime, category según semántica de negocio
# =============================================================================

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
log_fase("T04_Tipos", "raw+prov+tipo", len(df_raw), len(df_raw),
         descartados=0, motivo=f"{n_recasteadas} columnas casteadas")


# =============================================================================
# T05 — NORMALIZACIÓN DE FECHAS
# Formato ISO-8601: YYYY-MM-DD, timezone UTC
# =============================================================================

print("\n--- T05: Normalización de fechas (ISO-8601) ---")

# El dataset no tiene columna de fecha explícita.
# Se construye FECHA_ACCIDENTE a partir de AÑO + MES (día = 01 del mes).
# Esto es la mejor aproximación posible con los datos disponibles.
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
log_fase("T05_Fechas", "raw", len(df_raw), len(df_raw),
         descartados=0,
         motivo=f"FECHA_ACCIDENTE ISO-8601 creada; {n_fechas_err} errores parseo")


# =============================================================================
# T06 — NORMALIZACIÓN DE TEXTO
# Minúsculas + eliminación de acentos/caracteres especiales
# =============================================================================

print("\n--- T06: Normalización de texto ---")


def normalizar_texto(valor):
    """Convierte a minúsculas y elimina acentos/diacríticos."""
    if pd.isna(valor):
        return valor
    s = str(valor).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


# Creamos columnas _NORM para conservar los valores originales
COLS_NORM_RAW = [
    "TIPO_VIA", "TIPO_ACCIDENTE",
    "CONDICION_METEO", "CONDICION_ILUMINACION", "ZONA",
]
n_campos_norm = 0
for col in COLS_NORM_RAW:
    if col in df_raw.columns:
        df_raw[col + "_NORM"] = (
            df_raw[col].astype(str).apply(normalizar_texto)
        )
        n_campos_norm += 1
        print(f"  df_raw: {col} → {col}_NORM  (ej: '{df_raw[col + '_NORM'].iloc[0]}')")

df_provincia["PROVINCIA_NORM"] = (
    df_provincia["PROVINCIA"].apply(normalizar_texto)
)
df_tipo["TIPO_ACCIDENTE_NORM"] = (
    df_tipo["TIPO_ACCIDENTE"].apply(normalizar_texto)
)

total_norm = n_campos_norm + 2  # +2 por provincia y tipo
print(f"  Total campos normalizados: {total_norm} (columnas _NORM, originales conservadas)")
log_fase("T06_Texto", "raw+prov+tipo", len(df_raw), len(df_raw),
         descartados=0, motivo=f"{total_norm} campos normalizados en columnas _NORM")


# =============================================================================
# T07 — CORRECCIÓN DE RANGOS
# Validar rangos de negocio y eliminar registros fuera de rango
# =============================================================================

print("\n--- T07: Corrección de rangos ---")

n_antes_t07   = len(df_raw)
n_out_rango   = 0

# HORA: rango válido 0–23
mask = ~df_raw["HORA"].between(0, 23)
n = int(mask.sum())
if n > 0:
    df_raw = df_raw[~mask].copy()
    print(f"  HORA fuera de [0–23]: {n} registros eliminados")
else:
    print(f"  HORA [0–23]: OK — 0 registros fuera de rango")
n_out_rango += n

# MES: rango válido 1–12
mask = ~df_raw["MES"].between(1, 12)
n = int(mask.sum())
if n > 0:
    df_raw = df_raw[~mask].copy()
    print(f"  MES fuera de [1–12]: {n} registros eliminados")
else:
    print(f"  MES [1–12]: OK — 0 registros fuera de rango")
n_out_rango += n

# AÑO: rango válido 2019–2022
mask = ~df_raw["AÑO"].between(2019, 2022)
n = int(mask.sum())
if n > 0:
    df_raw = df_raw[~mask].copy()
    print(f"  AÑO fuera de [2019–2022]: {n} registros eliminados")
else:
    print(f"  AÑO [2019–2022]: OK — 0 registros fuera de rango")
n_out_rango += n

# Víctimas: todas las columnas de víctimas deben ser >= 0
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
log_fase("T07_Rangos", "raw", n_antes_t07, len(df_raw), n_out_rango,
         "Fuera de rango negocio (HORA/MES/AÑO/victimas)")


# =============================================================================
# T08 — DETECCIÓN Y GESTIÓN DE OUTLIERS
# Método IQR (1.5×IQR) — estrategia: winsorizing (acotar, no eliminar)
# =============================================================================

print("\n--- T08: Detección y gestión de outliers (IQR, winsorizing) ---")

COLS_OUTLIER = ["TOTAL_VICTIMAS", "HERIDOS_GRAVES", "HERIDOS_LEVES"]
n_acotados_total = 0

for col in COLS_OUTLIER:
    Q1  = df_raw[col].quantile(0.25)
    Q3  = df_raw[col].quantile(0.75)
    IQR = Q3 - Q1

    if IQR == 0:
        # Cuando IQR=0 la distribución está muy concentrada (ej. la mayoría = 1).
        # Fallback: z-score (media ± 3 desviaciones típicas)
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

    # Winsorizing: reemplazar por el límite, no eliminar el registro
    lower_val = max(round(lower), 0)
    upper_val = round(upper)
    df_raw.loc[df_raw[col] < lower, col] = lower_val
    df_raw.loc[df_raw[col] > upper, col] = upper_val

    print(f"  {col:<20} método={metodo:<18} límites=[{lower_val}, {upper_val}] "
          f"| outliers acotados: {n_out}")
    n_acotados_total += n_out

print(f"  Estrategia: winsorizing (acotar en los límites, no eliminar registros)")
print(f"  Total outliers acotados: {n_acotados_total} (registros conservados)")
log_fase("T08_Outliers", "raw", len(df_raw), len(df_raw),
         descartados=0,
         motivo=f"{n_acotados_total} outliers acotados (IQR/z-score, winsorizing)")


# =============================================================================
# T09 — SEUDONIMIZACIÓN DE DATOS PERSONALES (PII)
# Hash SHA-256 sobre campos identificables
# =============================================================================

print("\n--- T09: Seudonimización de PII (SHA-256) ---")

# ID_ACCIDENTE es el identificador del expediente DGT y podría rastrearse
# hasta la persona implicada. Se sustituye por su hash SHA-256.


def sha256_hash(valor):
    """Devuelve el hash SHA-256 hexadecimal del valor como string."""
    return hashlib.sha256(str(valor).encode("utf-8")).hexdigest()


df_raw["ID_ACCIDENTE_HASH"] = df_raw["ID_ACCIDENTE"].apply(sha256_hash)
df_raw = df_raw.drop(columns=["ID_ACCIDENTE"])

print(f"  ID_ACCIDENTE → ID_ACCIDENTE_HASH (SHA-256, 64 caracteres hexadecimales)")
print(f"  Ejemplo hash: {df_raw['ID_ACCIDENTE_HASH'].iloc[0][:32]}...")
print(f"  Columna original ID_ACCIDENTE eliminada del dataset procesado")
print(f"  Campos seudonimizados: 1")
log_fase("T09_PII", "raw", len(df_raw), len(df_raw),
         descartados=0, motivo="ID_ACCIDENTE→ID_ACCIDENTE_HASH (SHA-256)")


# =============================================================================
# T10 — VALIDACIÓN REFERENCIAL
# Registros huérfanos: documentar, NO eliminar automáticamente
# =============================================================================

print("\n--- T10: Validación referencial ---")

# Comprobación 1: PROVINCIA en df_raw ↔ df_provincia
provincias_raw  = set(df_raw["PROVINCIA"].astype(str).unique())
provincias_ref  = set(df_provincia["PROVINCIA"].unique())
huerfanos_prov  = provincias_raw - provincias_ref
n_huerf_prov    = int(df_raw["PROVINCIA"].astype(str).isin(huerfanos_prov).sum())

print(f"  [Check 1] df_raw[PROVINCIA] vs df_provincia[PROVINCIA]")
print(f"    Provincias únicas en raw   : {len(provincias_raw)}")
print(f"    Provincias únicas en ref   : {len(provincias_ref)}")
print(f"    Provincias sin referencia  : {len(huerfanos_prov)}")
if huerfanos_prov:
    print(f"    → {huerfanos_prov}")
print(f"    Registros huérfanos        : {n_huerf_prov} (documentados, NO eliminados)")

# Comprobación 2: TIPO_ACCIDENTE en df_raw ↔ df_tipo
tipos_raw    = set(df_raw["TIPO_ACCIDENTE"].astype(str).unique())
tipos_ref    = set(df_tipo["TIPO_ACCIDENTE"].unique())
huerfanos_tipo = tipos_raw - tipos_ref
n_huerf_tipo   = int(df_raw["TIPO_ACCIDENTE"].astype(str).isin(huerfanos_tipo).sum())

print(f"\n  [Check 2] df_raw[TIPO_ACCIDENTE] vs df_tipo[TIPO_ACCIDENTE]")
print(f"    Tipos únicos en raw        : {len(tipos_raw)}")
print(f"    Tipos únicos en ref        : {len(tipos_ref)}")
print(f"    Tipos sin referencia       : {len(huerfanos_tipo)}")
if huerfanos_tipo:
    print(f"    → {huerfanos_tipo}")
print(f"    Registros huérfanos        : {n_huerf_tipo} (documentados, NO eliminados)")

log_fase("T10_Referencial", "raw↔prov", len(df_raw), len(df_raw),
         descartados=0,
         motivo=f"{n_huerf_prov} huerf.PROV + {n_huerf_tipo} huerf.TIPO (doc.)")


# =============================================================================
# MERGE FINAL
# Añadir columnas de trazabilidad requeridas por el modelo dimensional
# =============================================================================

print("\n--- MERGE FINAL ---")

n_antes_merge = len(df_raw)
df_raw["source_id"]       = "accidentes_raw_dgt"
df_raw["load_timestamp"]  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

print(f"  Dataset limpio final: {len(df_raw)} registros × {df_raw.shape[1]} columnas")
print(f"  Columnas de trazabilidad añadidas: source_id, load_timestamp")
print(f"  Columnas finales del dataset:\n  {list(df_raw.columns)}")
log_fase("MERGE_FINAL", "Todas", n_antes_merge, len(df_raw),
         descartados=0, motivo="source_id + load_timestamp añadidos")


# =============================================================================
# GUARDAR DATASETS PROCESADOS
# =============================================================================

print("\n--- Guardando datasets limpios en processed/ ---")

df_raw.to_csv(
    os.path.join(PROCESSED_DIR, "accidentes_raw_clean.csv"), index=False, encoding="utf-8-sig"
)
df_provincia.to_csv(
    os.path.join(PROCESSED_DIR, "accidentes_por_provincia_clean.csv"), index=False, encoding="utf-8-sig"
)
df_tipo.to_csv(
    os.path.join(PROCESSED_DIR, "accidentes_por_tipo_clean.csv"), index=False, encoding="utf-8-sig"
)
df_resumen.to_csv(
    os.path.join(PROCESSED_DIR, "accidentes_resumen_clean.csv"), index=False, encoding="utf-8-sig"
)

print(f"  ✔  accidentes_raw_clean.csv              ({len(df_raw)} filas, "
      f"{df_raw.shape[1]} columnas)")
print(f"  ✔  accidentes_por_provincia_clean.csv    ({len(df_provincia)} filas)")
print(f"  ✔  accidentes_por_tipo_clean.csv         ({len(df_tipo)} filas)")
print(f"  ✔  accidentes_resumen_clean.csv          ({len(df_resumen)} filas)")


# =============================================================================
# TABLA RESUMEN DE TRACKING
# =============================================================================

print("\n" + "=" * 72)
print("TABLA RESUMEN DE TRACKING DEL PIPELINE (Bloque 3)")
print("=" * 72)
print(f"  {'FASE':<22} {'FUENTE':<15} {'ENTRADA':>8} {'SALIDA':>8} "
      f"{'DESCART':>8}  MOTIVO")
print("  " + "-" * 68)
for entry in tracking_log:
    print(f"  {entry['fase']:<22} {entry['fuente']:<15} "
          f"{entry['registros_entrada']:>8} {entry['registros_salida']:>8} "
          f"{entry['descartados']:>8}  {entry['motivo_principal']}")

guardar_tracking()

# =============================================================================
# CALIDAD MÍNIMA EXIGIDA — Verificación final
# =============================================================================

print("\n--- Verificación de calidad mínima exigida ---")

nulos_criticos = df_raw[["FECHA_ACCIDENTE", "PROVINCIA", "TIPO_ACCIDENTE",
                          "TOTAL_VICTIMAS"]].isnull().mean() * 100
dup_pk = df_raw["ID_ACCIDENTE_HASH"].duplicated().sum()

print(f"  Tasa de nulos en campos críticos (< 2% exigido):")
for col, pct in nulos_criticos.items():
    estado = "✔" if pct < 2 else "✗"
    print(f"    {estado} {col}: {pct:.2f}%")
print(f"  Duplicados en clave primaria (hash): {dup_pk} {'✔' if dup_pk == 0 else '✗'}")
print(f"  Tipos casteados: {'✔' if n_recasteadas > 0 else 'Revisar'}")
print(f"  Rango de fechas 2019–2022: "
      f"{'✔' if df_raw['AÑO'].between(2019, 2022).all() else '✗'}")

print("\n✔ Bloque 3 completado — ETL finalizado. Datasets limpios en processed/")
print("=" * 72)
