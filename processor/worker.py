"""
processor/worker.py
Define la unidad de trabajo que ejecuta cada hilo.
Cada worker recibe su chunk, aplica el filtro y retorna los resultados.
"""

import pandas as pd
from processor.filters import aplicar_filtro
from utils.logger import logger


def ejecutar_worker(
    id_hilo: int,
    chunk: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
) -> pd.DataFrame:
    """
    Función ejecutada por cada hilo del ThreadPoolExecutor.
    Procesa un subconjunto (chunk) del DataFrame total.

    Args:
        id_hilo:  Identificador numérico del hilo (para trazabilidad).
        chunk:    Subconjunto de registros asignado a este hilo.
        campo:    Campo por el cual filtrar.
        operador: Operador de comparación.
        valor:    Valor de referencia para el filtro.

    Returns:
        pd.DataFrame con los registros del chunk que cumplen el filtro.
    """
    logger.debug(
        f"[Hilo-{id_hilo}] Iniciando procesamiento de {len(chunk):,} registros ..."
    )

    try:
        resultado = aplicar_filtro(chunk, campo, operador, valor)

        logger.debug(
            f"[Hilo-{id_hilo}] Finalizado → "
            f"{len(resultado):,} registros cumplen el filtro "
            f"de {len(chunk):,} procesados"
        )

        return resultado

    except Exception as e:
        logger.error(f"[Hilo-{id_hilo}] Error durante el procesamiento: {e}")
        return pd.DataFrame()
