# =============================================================================
# extract.py — Fase EXTRACT: carga de fuentes originales
# =============================================================================

import pandas as pd
import shutil
import os
from config import FICHEROS, ENCODING, RAW_DIR
import tracker


def run():
    print("\n[EXTRACT] Cargando fuentes de datos...")

    dataframes = {}
    for nombre, fichero in FICHEROS.items():
        try:
            df = pd.read_csv(fichero, encoding=ENCODING)
            dataframes[nombre] = df
            tracker.log_fase("EXTRACT", nombre, df.shape[0], df.shape[0],
                             descartados=0, motivo="Carga OK")
            print(f"  ✔  {fichero:<55} → {df.shape[0]:>5} filas, {df.shape[1]} columnas")
        except FileNotFoundError:
            print(f"  ✗  ERROR: No se encontró '{fichero}'")
            raise
        except Exception as e:
            print(f"  ✗  ERROR al cargar '{fichero}': {e}")
            raise

    # Guardar copia cruda en data/raw/ (fuentes sin modificar)
    for nombre, ruta in FICHEROS.items():
        shutil.copy2(ruta, os.path.join(RAW_DIR, os.path.basename(ruta)))
    print("  ✔  Copias crudas guardadas en data/raw/")

    df_raw       = dataframes["raw"].copy()
    df_provincia = dataframes["provincia"].copy()
    df_tipo      = dataframes["tipo"].copy()
    df_resumen   = dataframes["resumen"].copy()

    return df_raw, df_provincia, df_tipo, df_resumen
