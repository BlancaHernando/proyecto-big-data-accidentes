# =============================================================================
# tracker.py — Módulo de tracking del pipeline ETL
# =============================================================================

import json
from datetime import datetime
from config import TRACKING_FILE

tracking_log = []


def log_fase(fase, fuente, registros_entrada, registros_salida, descartados=None, motivo="—"):
    if descartados is None:
        descartados = registros_entrada - registros_salida
    entrada = int(registros_entrada)
    salida  = int(registros_salida)
    descart = int(descartados)
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


def imprimir_tabla():
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
