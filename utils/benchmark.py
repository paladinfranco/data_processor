"""
utils/benchmark.py
Módulo de benchmark comparativo de rendimiento.
Ejecuta el mismo filtro con distintas configuraciones de workers,
calcula métricas de speedup y eficiencia, y muestra una tabla comparativa.
"""

import os
import pandas as pd
from processor.orchestrator import procesar
from utils.logger import logger


# ── Constantes ────────────────────────────────────────────────────────────────
CONFIGURACIONES_DEFAULT = [1, 2, 4, 8, 12, 16]
UMBRAL_EFICIENCIA       = 70.0   # Por debajo de este % ya no es eficiente agregar workers


# ── Función pública ───────────────────────────────────────────────────────────

def ejecutar_benchmark(
    df: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
    modo: str = "hilos",
    archivo: str = "",
) -> None:
    """
    Ejecuta el benchmark comparativo con distintas configuraciones de workers.
    """
    logger.info("🔬 Iniciando benchmark de rendimiento ...")
    logger.info("─" * 54)

    cpus          = os.cpu_count() or 1
    multiplicador = 4 if modo == "hilos" else 2
    maximo        = cpus * multiplicador

    configuraciones = [n for n in CONFIGURACIONES_DEFAULT if n <= maximo]
    if maximo not in configuraciones:
        configuraciones.append(maximo)

    # ── Ejecución única por configuración ─────────────────────────────────────
    resultados = []
    for n_workers in configuraciones:
        logger.info(f"⏳ Probando con {n_workers} worker(s) ...")
        df_resultado, tiempo, workers_reales = procesar(
            df,
            campo    = campo,
            operador = operador,
            valor    = valor,
            n_hilos  = n_workers,
            modo     = modo,
        )
        resultados.append({
            "workers":   workers_reales,
            "tiempo":    tiempo,
            "registros": len(df_resultado),
        })

    _mostrar_tabla(resultados, df, campo, operador, valor, modo, archivo)


# ── Ejecución con captura de resultados ──────────────────────────────────────

def _recalcular_con_resultados(
    df: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
    modo: str,
    configuraciones: list[int],
) -> list[dict]:
    """
    Ejecuta cada configuración y captura tiempo y cantidad de resultados.
    """
    resultados = []

    for n_workers in configuraciones:
        df_resultado, tiempo, workers_reales = procesar(
            df,
            campo    = campo,
            operador = operador,
            valor    = valor,
            n_hilos  = n_workers,
            modo     = modo,
        )

        resultados.append({
            "workers":   workers_reales,
            "tiempo":    tiempo,
            "registros": len(df_resultado),
        })

    return resultados


# ── Tabla de resultados ───────────────────────────────────────────────────────

def _mostrar_tabla(
    resultados: list[dict],
    df: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
    modo: str,
    archivo: str,
) -> None:
    """
    Calcula métricas y muestra la tabla comparativa de rendimiento.
    """
    tiempo_base = resultados[0]["tiempo"] if resultados else 1.0
    if tiempo_base == 0:
        tiempo_base = 0.001

    nombre_archivo = os.path.basename(archivo) if archivo else "archivo"

    # ── Encabezado ────────────────────────────────────────────────────────────
    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║                   BENCHMARK DE RENDIMIENTO                      ║")
    print(f"║  Archivo : {nombre_archivo:<54}║")
    print(f"║  Registros: {len(df):,}{' ' * (53 - len(f'{len(df):,}'))}║")
    print(f"║  Filtro  : {campo} {operador} {valor:<51}║")
    print(f"║  Modo    : {modo:<54}║")
    print("╠═══════════╦═══════════╦══════════════╦══════════════╦═══════════╣")
    print("║  Workers  ║   Tiempo  ║  Registros   ║   Speedup    ║ Eficiencia║")
    print("╠═══════════╬═══════════╬══════════════╬══════════════╬═══════════╣")

    mejor_balance    = None
    mejor_eficiencia = None
    max_speedup      = 0.0
    max_speedup_w    = 1

    for r in resultados:
        n        = r["workers"]
        tiempo   = r["tiempo"]
        regs     = r["registros"]
        speedup  = tiempo_base / tiempo if tiempo > 0 else 1.0
        efic     = (speedup / n) * 100

        # Marca el mejor balance velocidad/eficiencia
        if efic >= UMBRAL_EFICIENCIA and (
            mejor_balance is None or speedup > resultados[mejor_balance]["tiempo"]
        ):
            mejor_balance = resultados.index(r)

        if speedup > max_speedup:
            max_speedup   = speedup
            max_speedup_w = n

        # Indicador visual de eficiencia
        if efic >= 85:
            indicador = "🟢"
        elif efic >= UMBRAL_EFICIENCIA:
            indicador = "🟡"
        else:
            indicador = "🔴"

        # Indicador visual de speedup
        if speedup >= 1.0:
            speedup_str = f"{speedup:>8.2f}x    "
        else:
            speedup_str = f"{speedup:>8.2f}x ⚠️ "

        print(
            f"║  {n:>7}  ║"
            f"  {tiempo:>6.3f}s  ║"
            f"  {regs:>10,}  ║"
            f"  {speedup_str}║"
            f" {indicador} {efic:>5.1f}%  ║"
        )

    print("╚═══════════╩═══════════╩══════════════╩══════════════╩═══════════╝")

    # ── Leyenda ───────────────────────────────────────────────────────────────
    print()
    print("  🟢 Eficiencia ≥ 85%  │  🟡 Eficiencia ≥ 70%  │  🔴 Eficiencia < 70%")
    print()

    # ── Conclusiones ──────────────────────────────────────────────────────────
    print(f"  🏆 Speedup máximo      : {max_speedup:.2f}x con {max_speedup_w} workers")

    if mejor_balance is not None:
        w_optimo = resultados[mejor_balance]["workers"]
        print(f"  ⚡ Configuración óptima: {w_optimo} workers "
              f"(mejor balance velocidad/eficiencia ≥ {UMBRAL_EFICIENCIA}%)")

    # Verificación de integridad
    registros_set = set(r["registros"] for r in resultados)
    if len(registros_set) == 1:
        print(f"  ✅ Integridad verificada: "
              f"todos los workers retornaron {list(registros_set)[0]:,} registros")
    else:
        print(f"  ⚠️  Inconsistencia detectada: "
              f"distintas cantidades de registros entre configuraciones")
    print()

     # ── Nota sobre overhead ───────────────────────────────────────────────────
    tiene_overhead = any(
        (tiempo_base / r["tiempo"] if r["tiempo"] > 0 else 1.0) < 1.0
        for r in resultados[1:]
    )
    if tiene_overhead:
        print("  ⚠️  Speedup < 1.0x indica overhead de coordinación mayor al beneficio.")
        print("     Esto es normal en datasets pequeños o tareas de filtrado simple.")
        print("     El beneficio real del paralelismo se ve con datasets > 500K registros")
        print("     y transformaciones CPU-intensivas.")
        print()
