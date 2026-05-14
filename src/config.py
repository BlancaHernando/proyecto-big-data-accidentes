# =============================================================================
# config.py — Configuración compartida del pipeline ETL
# =============================================================================

import os

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
RAW_DIR       = os.path.join(BASE_DIR, "data", "raw")
TRACKING_FILE = os.path.join(BASE_DIR, "logs", "pipeline_tracking.json")
ENCODING      = "utf-8-sig"

FICHEROS = {
    "raw"      : os.path.join(BASE_DIR, "accidentes_raw.csv"),
    "provincia": os.path.join(BASE_DIR, "accidentes_por_provincia.csv"),
    "tipo"     : os.path.join(BASE_DIR, "accidentes_por_tipo.csv"),
    "resumen"  : os.path.join(BASE_DIR, "accidentes_resumen.csv"),
}

# Crear carpetas necesarias al importar este módulo
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
