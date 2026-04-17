"""
processor/orchestrator.py
Orquestador del procesamiento concurrente.
Responsable de dividir la carga, gestionar los hilos/procesos y consolidar resultados.
Soporta dos modos de ejecución:
  - 'hilos'    : ThreadPoolExecutor  (mejor para I/O, menor overhead)
  - 'procesos' : ProcessPoolExecutor (mejor para CPU-intensivo, paralelismo real)
"""

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from tqdm import tqdm

from processor.worker import ejecutar_worker
from utils.logger import logger
from utils.timer import Timer


# ── Constantes ────────────────────────────────────────────────────────────────
MULTIPLICADOR_HILOS    = 4   # CPUs × 4  → liviano, comparte memoria
MULTIPLICADOR_PROCESOS = 2   # CPUs × 2  → pesado, memoria propia por proceso
MIN_WORKERS            = 1
MODOS_VALIDOS          = {"hilos", "procesos"}


# ── Función pública ───────────────────────────────────────────────────────────

def procesar(
    df: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
    n_hilos: int = 4,
    modo: str = "hilos",
) -> tuple[pd.DataFrame, float, int]:
    """
    Coordina el procesamiento concurrente del DataFrame completo.

    Args:
        df:       DataFrame completo a procesar.
        campo:    Campo por el cual filtrar.
        operador: Operador de comparación.
        valor:    Valor de referencia para el filtro.
        n_hilos:  Número de workers solicitado por el usuario.
        modo:     Modo de ejecución: 'hilos' o 'procesos'.

    Returns:
        Tupla (DataFrame con resultados consolidados, tiempo de ejecución,
        número real de workers utilizados).
    """
    modo    = _validar_modo(modo)
    n_hilos = _validar_hilos(n_hilos, modo)
    chunks  = _dividir_carga(df, n_hilos)

    logger.info(f"🚀 Iniciando procesamiento con {n_hilos} {modo} ...")
    logger.info(f"🔍 Filtro: {campo} {operador} {valor}")

    with Timer("Procesamiento concurrente") as timer:
        resultados = _ejecutar_workers(chunks, campo, operador, valor, n_hilos, modo)

    df_resultado = _consolidar(resultados)

    logger.info(
        f"📊 Resultado: {len(df_resultado):,} registros encontrados "
        f"de {len(df):,} procesados"
    )

    return df_resultado, timer.transcurrido, n_hilos


# ── Validaciones ──────────────────────────────────────────────────────────────

def _validar_modo(modo: str) -> str:
    """Valida que el modo de ejecución sea válido."""
    modo = modo.lower().strip()
    if modo not in MODOS_VALIDOS:
        logger.warning(
            f"⚠️  Modo inválido: '{modo}'. "
            f"Se usará el modo por defecto: 'hilos'. "
            f"Modos válidos: {', '.join(sorted(MODOS_VALIDOS))}"
        )
        return "hilos"
    return modo


def _validar_hilos(n_hilos: int, modo: str) -> int:
    """
    Valida y ajusta el número de workers al rango permitido según el modo.

    Límites:
        hilos    → CPUs × 4  (livianos, comparten memoria)
        procesos → CPUs × 2  (pesados, memoria propia por proceso)

    Args:
        n_hilos: Número de workers solicitado.
        modo:    Modo de ejecución ('hilos' o 'procesos').

    Returns:
        Número de workers validado y ajustado.
    """
    cpus          = os.cpu_count() or 1
    multiplicador = (
        MULTIPLICADOR_HILOS if modo == "hilos" else MULTIPLICADOR_PROCESOS
    )
    maximo = cpus * multiplicador
    tipo   = "hilos" if modo == "hilos" else "procesos"

    if n_hilos < MIN_WORKERS:
        logger.warning(
            f"⚠️  Número inválido ({n_hilos}). "
            f"Se usará el mínimo: {MIN_WORKERS}"
        )
        return MIN_WORKERS

    if n_hilos > maximo:
        logger.warning(
            f"⚠️  Solicitaste {n_hilos} {tipo}. "
            f"Tu sistema tiene {cpus} CPU(s) lógica(s). "
            f"Límite máximo para {tipo}: {maximo} ({cpus} CPUs × {multiplicador}). "
            f"Se ajustará automáticamente a {maximo}."
        )
        return maximo

    return n_hilos


# ── División de carga ─────────────────────────────────────────────────────────

def _dividir_carga(df: pd.DataFrame, n_hilos: int) -> list[pd.DataFrame]:
    """
    Divide el DataFrame en N chunks equitativos usando indexación nativa
    de pandas (iloc). El último chunk absorbe el residuo de divisiones
    no exactas, garantizando cero pérdida de registros.

    A diferencia de numpy.array_split, este enfoque opera directamente
    sobre pandas sin dependencias externas ni FutureWarnings.

    Args:
        df:      DataFrame completo.
        n_hilos: Número de chunks a generar.

    Returns:
        Lista de DataFrames (chunks).
    """
    tamanio_chunk = len(df) // n_hilos

    chunks = [
        df.iloc[i * tamanio_chunk : (i + 1) * tamanio_chunk]
        if i < n_hilos - 1
        else df.iloc[i * tamanio_chunk :]
        for i in range(n_hilos)
    ]

    logger.info(f"📦 Distribución de carga ({len(df):,} registros en {n_hilos} worker(s)):")
    for i, chunk in enumerate(chunks, start=1):
        logger.info(f"   Worker {i} → {len(chunk):,} registros")

    total_verificado = sum(len(c) for c in chunks)
    assert total_verificado == len(df), (
        f"Error de integridad: se perdieron registros en la división. "
        f"Original: {len(df)}, Suma chunks: {total_verificado}"
    )

    return chunks


# ── Ejecución concurrente ─────────────────────────────────────────────────────

def _ejecutar_workers(
    chunks: list[pd.DataFrame],
    campo: str,
    operador: str,
    valor: str,
    n_hilos: int,
    modo: str,
) -> list[pd.DataFrame]:
    """
    Lanza los workers concurrentes usando ThreadPoolExecutor o ProcessPoolExecutor
    según el modo seleccionado. Muestra barra de progreso en tiempo real.

    Args:
        chunks:   Lista de chunks a procesar.
        campo:    Campo de filtro.
        operador: Operador de comparación.
        valor:    Valor del filtro.
        n_hilos:  Número máximo de workers concurrentes.
        modo:     'hilos' para ThreadPoolExecutor, 'procesos' para ProcessPoolExecutor.

    Returns:
        Lista de DataFrames resultado de cada worker.
    """
    ExecutorClass = ThreadPoolExecutor if modo == "hilos" else ProcessPoolExecutor
    etiqueta      = "hilos" if modo == "hilos" else "procesos"
    resultados: list[pd.DataFrame] = []
    futuros: dict = {}

    with ExecutorClass(max_workers=n_hilos) as executor:
        for i, chunk in enumerate(chunks, start=1):
            futuro = executor.submit(
                ejecutar_worker, i, chunk, campo, operador, valor
            )
            futuros[futuro] = i

        barra = tqdm(
            as_completed(futuros),
            total=len(futuros),
            desc=f"  ⚙️  Procesando {etiqueta}",
            unit="worker",
            ncols=70,
            colour="green",
        )

        for futuro in barra:
            id_worker = futuros[futuro]
            try:
                resultado = futuro.result()
                resultados.append(resultado)
                logger.info(
                    f"✔  Worker {id_worker} completado → "
                    f"{len(resultado):,} registros encontrados"
                )
            except Exception as e:
                logger.error(f"✘  Worker {id_worker} falló: {e}")

    return resultados


# ── Consolidación ─────────────────────────────────────────────────────────────

def _consolidar(resultados: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Consolida los resultados de todos los workers en un único DataFrame.
    """
    if not resultados:
        logger.warning("⚠️  Ningún worker retornó resultados.")
        return pd.DataFrame()

    return pd.concat(resultados, ignore_index=True)