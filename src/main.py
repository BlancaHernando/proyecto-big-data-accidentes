# =============================================================================
# main.py — Orquestador del pipeline ETL
# Ejecuta en orden: EXTRACT → TRANSFORM → LOAD
# =============================================================================

import extract
import transform
import load
import tracker

print("=" * 72)
print("BLOQUE 3 — PIPELINE ETL: EXTRACCIÓN, TRANSFORMACIÓN Y LIMPIEZA")
print("=" * 72)

# FASE 1: EXTRACT
df_raw, df_provincia, df_tipo, df_resumen = extract.run()

# FASE 2: TRANSFORM (T01–T10)
df_raw, df_provincia, df_tipo, df_resumen, n_recast = transform.run(
    df_raw, df_provincia, df_tipo, df_resumen
)

# FASE 3: LOAD
load.run(df_raw, df_provincia, df_tipo, df_resumen, n_recast)

# TABLA RESUMEN + GUARDAR TRACKING
tracker.imprimir_tabla()
tracker.guardar_tracking()

print("\n✔ Bloque 3 completado — ETL finalizado. Datasets limpios en processed/")
print("=" * 72)
