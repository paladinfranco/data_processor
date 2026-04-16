"""
processor/orchestrator.py
Orquestador del procesamiento concurrente.
Responsable de dividir la carga, gestionar los hilos y consolidar resultados.
"""

import os
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from processor.worker import ejecutar_worker
from utils.logger import logger
from utils.timer import Timer


# ── Constantes ────────────────────────────────────────────────────────────────
MULTIPLICADOR_HILOS = 4
MIN_HILOS           = 1


# ── Función pública ───────────────────────────────────────────────────────────

def procesar(
    df: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
    n_hilos: int = 4,
) -> tuple[pd.DataFrame, float]:
    """
    Coordina el procesamiento concurrente del DataFrame completo.

    Args:
        df:       DataFrame completo a procesar.
        campo:    Campo por el cual filtrar.
        operador: Operador de comparación.
        valor:    Valor de referencia para el filtro.
        n_hilos:  Número de hilos solicitado por el usuario.

    Returns:
        Tupla (DataFrame con resultados consolidados, tiempo de ejecución).
    """
    n_hilos = _validar_hilos(n_hilos)
    chunks  = _dividir_carga(df, n_hilos)

    logger.info(f"🚀 Iniciando procesamiento con {n_hilos} hilo(s) ...")
    logger.info(f"🔍 Filtro: {campo} {operador} {valor}")

    with Timer("Procesamiento concurrente") as timer:
        resultados = _ejecutar_hilos(chunks, campo, operador, valor, n_hilos)

    df_resultado = _consolidar(resultados)

    logger.info(
        f"📊 Resultado: {len(df_resultado):,} registros encontrados "
        f"de {len(df):,} procesados"
    )

    return df_resultado, timer.transcurrido


# ── Funciones privadas ────────────────────────────────────────────────────────

def _validar_hilos(n_hilos: int) -> int:
    """
    Valida y ajusta el número de hilos al rango permitido.
    Máximo: cpu_count * MULTIPLICADOR_HILOS
    Mínimo: 1

    Args:
        n_hilos: Número de hilos solicitado.

    Returns:
        Número de hilos validado y ajustado.
    """
    cpus       = os.cpu_count() or 1
    maximo     = cpus * MULTIPLICADOR_HILOS

    if n_hilos < MIN_HILOS:
        logger.warning(
            f"⚠️  Número de hilos inválido ({n_hilos}). "
            f"Se usará el mínimo: {MIN_HILOS}"
        )
        return MIN_HILOS

    if n_hilos > maximo:
        logger.warning(
            f"⚠️  Solicitaste {n_hilos} hilos. "
            f"Tu sistema tiene {cpus} CPU(s) lógica(s). "
            f"Límite máximo permitido: {maximo} hilos. "
            f"Se ajustará automáticamente a {maximo}."
        )
        return maximo

    return n_hilos


def _dividir_carga(df: pd.DataFrame, n_hilos: int) -> list[pd.DataFrame]:
    """
    Divide el DataFrame en N chunks lo más equitativos posible.
    Usa numpy.array_split para manejar divisiones no exactas sin pérdida
    de registros.

    Args:
        df:      DataFrame completo.
        n_hilos: Número de chunks a generar.

    Returns:
        Lista de DataFrames (chunks).
    """
    chunks = np.array_split(df, n_hilos)

    logger.info(f"📦 Distribución de carga ({len(df):,} registros en {n_hilos} hilo(s)):")
    for i, chunk in enumerate(chunks, start=1):
        logger.info(f"   Hilo {i} → {len(chunk):,} registros")

    total_verificado = sum(len(c) for c in chunks)
    assert total_verificado == len(df), (
        f"Error de integridad: se perdieron registros en la división. "
        f"Original: {len(df)}, Suma chunks: {total_verificado}"
    )

    return chunks


def _ejecutar_hilos(
    chunks: list[pd.DataFrame],
    campo: str,
    operador: str,
    valor: str,
    n_hilos: int,
) -> list[pd.DataFrame]:
    """
    Lanza los hilos concurrentes usando ThreadPoolExecutor y recolecta
    los resultados en orden de finalización.

    Args:
        chunks:   Lista de chunks a procesar.
        campo:    Campo de filtro.
        operador: Operador de comparación.
        valor:    Valor del filtro.
        n_hilos:  Número máximo de hilos concurrentes.

    Returns:
        Lista de DataFrames resultado de cada hilo.
    """
    resultados: list[pd.DataFrame] = []
    futuros     = {}

    with ThreadPoolExecutor(max_workers=n_hilos) as executor:
        for i, chunk in enumerate(chunks, start=1):
            futuro = executor.submit(
                ejecutar_worker, i, chunk, campo, operador, valor
            )
            futuros[futuro] = i

        for futuro in as_completed(futuros):
            id_hilo = futuros[futuro]
            try:
                resultado = futuro.result()
                resultados.append(resultado)
                logger.info(
                    f"✔  Hilo {id_hilo} completado → "
                    f"{len(resultado):,} registros encontrados"
                )
            except Exception as e:
                logger.error(f"✘  Hilo {id_hilo} falló: {e}")

    return resultados


def _consolidar(resultados: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Consolida los resultados de todos los hilos en un único DataFrame.

    Args:
        resultados: Lista de DataFrames de cada hilo.

    Returns:
        DataFrame unificado con todos los registros encontrados.
    """
    if not resultados:
        logger.warning("⚠️  Ningún hilo retornó resultados.")
        return pd.DataFrame()

    df_final = pd.concat(resultados, ignore_index=True)
    return df_final
