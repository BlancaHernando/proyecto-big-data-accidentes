# =============================================================================
# load.py — Fase LOAD: guardar datasets limpios + verificación de calidad
# =============================================================================

import os
from datetime import datetime
from config import PROCESSED_DIR
import tracker


def run(df_raw, df_provincia, df_tipo, df_resumen, n_recasteadas):
    print("\n--- Guardando datasets limpios en processed/ ---")

    n_antes_merge = len(df_raw)
    df_raw["source_id"]      = "accidentes_raw_dgt"
    df_raw["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print(f"  Dataset limpio final: {len(df_raw)} registros × {df_raw.shape[1]} columnas")
    print(f"  Columnas de trazabilidad añadidas: source_id, load_timestamp")
    tracker.log_fase("MERGE_FINAL", "Todas", n_antes_merge, len(df_raw),
                     descartados=0, motivo="source_id + load_timestamp añadidos")

    df_raw.to_csv(
        os.path.join(PROCESSED_DIR, "accidentes_raw_clean.csv"),
        index=False, encoding="utf-8-sig"
    )
    df_provincia.to_csv(
        os.path.join(PROCESSED_DIR, "accidentes_por_provincia_clean.csv"),
        index=False, encoding="utf-8-sig"
    )
    df_tipo.to_csv(
        os.path.join(PROCESSED_DIR, "accidentes_por_tipo_clean.csv"),
        index=False, encoding="utf-8-sig"
    )
    df_resumen.to_csv(
        os.path.join(PROCESSED_DIR, "accidentes_resumen_clean.csv"),
        index=False, encoding="utf-8-sig"
    )

    print(f"  ✔  accidentes_raw_clean.csv              ({len(df_raw)} filas, {df_raw.shape[1]} columnas)")
    print(f"  ✔  accidentes_por_provincia_clean.csv    ({len(df_provincia)} filas)")
    print(f"  ✔  accidentes_por_tipo_clean.csv         ({len(df_tipo)} filas)")
    print(f"  ✔  accidentes_resumen_clean.csv          ({len(df_resumen)} filas)")

    # Verificación de calidad mínima exigida
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
